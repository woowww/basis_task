# Payment Processing Service

Микросервис асинхронной обработки платежей. Принимает запрос на оплату, сразу отвечает
`202 Accepted`, обрабатывает платёж через эмулятор внешнего шлюза и уведомляет клиента о
результате через webhook. Гарантии: Outbox pattern, идемпотентность по `Idempotency-Key`,
повторы с экспоненциальной задержкой и Dead Letter Queue.

**Стек:** Python 3.12, FastAPI + Pydantic v2, SQLAlchemy 2.0 (async, asyncpg), PostgreSQL 16,
RabbitMQ (FastStream), Alembic, dishka (DI), Docker Compose, uv.

## Быстрый старт

```bash
docker compose up --build -d
```

Поднимаются `postgres`, `rabbitmq`, одноразовый `migrate` (`alembic upgrade head`), затем `api`,
`outbox-relay` и `consumer`. Файл `.env` не обязателен: по умолчанию `API_KEY=change-me`.
Чтобы переопределить параметры — `cp .env.example .env` (адреса БД и брокера compose задаёт сам).

| Что | Где |
|---|---|
| API | http://localhost:8000 |
| Swagger UI | http://localhost:8000/docs (кнопка **Authorize** → `X-API-Key`) |
| RabbitMQ Management | http://localhost:15672 (`guest` / `guest`) |

```bash
export API_KEY=change-me

# создать платёж
curl -s -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: $API_KEY" -H "Idempotency-Key: order-1001" -H "Content-Type: application/json" \
  -d '{"amount": "1500.00", "currency": "RUB", "description": "Заказ #1001",
       "metadata": {"order_id": 1001}, "webhook_url": "https://webhook.site/<your-id>"}'

# через 2–5 секунд — итоговый статус
curl -s http://localhost:8000/api/v1/payments/<payment_id> -H "X-API-Key: $API_KEY"
```

Для приёма webhook удобно взять одноразовый URL на https://webhook.site.

## Архитектура

```
              ┌────────────── одна транзакция ──────────────┐
POST ──► api ─┤ INSERT payments  +  INSERT outbox            ├──► PostgreSQL
              └──────────────────────────────────────────────┘        │
                                                                       ▼
                                   outbox-relay: SELECT … FOR UPDATE SKIP LOCKED,
                                   publish с publisher confirms → published_at
                                                                       │
                                                                       ▼
                                       exchange payments ──► queue payments.new
                                                                       │
                                                                       ▼
                         consumer: SELECT … FOR UPDATE → шлюз (2–5 с, 90/10) → статус → webhook
                                     │ технический сбой (webhook, БД, …)
                                     ▼
              exchange payments.retry ──► payments.new.retry.N (TTL) ──(истёк)──► payments
                                     │ 3-я неудачная попытка / неизвестный платёж / битое сообщение
                                     ▼
                         exchange payments.dlx ──► payments.new.dlq
```

| Сервис | Роль |
|---|---|
| `postgres` | Платежи и outbox |
| `rabbitmq` | Брокер сообщений (с Management UI) |
| `migrate` | Одноразовый `alembic upgrade head` перед стартом приложений |
| `api` | HTTP API |
| `outbox-relay` | Публикует события из таблицы `outbox` в RabbitMQ |
| `consumer` | Один обработчик: шлюз → статус в БД → webhook → retry/DLQ |

Все три приложения собраны из одного образа и отличаются командой запуска.

### Слои кода

```
src/payment_service/
├── domain/        сущность Payment (переходы статусов), OutboxEvent, Currency, PaymentStatus
├── application/   юзкейсы и порты (Protocol)
│   ├── create_payment.py   CreatePayment     — POST: идемпотентность + outbox в одном коммите
│   ├── get_payment.py      GetPayment        — GET
│   ├── publish_outbox.py   PublishOutboxBatch — relay: пачка outbox → брокер
│   ├── process_payment.py  ProcessPayment    — consumer: шлюз → статус → webhook
│   └── ports.py            PaymentRepository, OutboxRepository, UnitOfWork,
│                           PaymentGateway, WebhookSender, MessagePublisher
├── infra/         реализации портов: SQLAlchemy (db/), RabbitMQ (broker/),
│                  эмулятор шлюза (gateway/), httpx-клиент webhook (webhook/)
├── api/           адаптер HTTP: роуты, схемы, X-API-Key, единый формат ошибок
├── consumer/      адаптер FastStream: разбор сообщения, вызов юзкейса, политика retry/DLQ
├── relay/         цикл outbox-relay
├── ioc.py         граф зависимостей dishka
└── config.py      настройки (pydantic-settings)
```

Адаптеры тонкие: разобрать вход → вызвать юзкейс → отдать результат. Юзкейсы зависят только от
портов; транзакционные границы задают сами юзкейсы через `UnitOfWork`.

### Топология RabbitMQ

| Объект | Тип | Аргументы / привязка |
|---|---|---|
| `payments` | direct exchange | — |
| `payments.new` | очередь | ключ `payments.new`; DLX → `payments.dlx` (страховка на reject) |
| `payments.retry` | direct exchange | — |
| `payments.new.retry.1` | очередь | TTL 1 с; DLX → `payments` / `payments.new` |
| `payments.new.retry.2` | очередь | TTL 2 с; DLX → `payments` / `payments.new` |
| `payments.dlx` | direct exchange | — |
| `payments.new.dlq` | очередь | ключ `payments.new` |

Всё durable, сообщения persistent. Топологию идемпотентно объявляют и consumer, и relay — порядок
их старта не важен. Число retry-очередей = `MAX_ATTEMPTS − 1`, TTL очереди N =
`RETRY_BASE_DELAY_SECONDS × 2^(N−1)`. Номер попытки — в заголовке `x-attempt`, причина попадания в
DLQ — в `x-death-reason`.

## Гарантии и как они обеспечены

**Outbox pattern.** Платёж и событие `payments.new` пишутся в одной транзакции. Relay забирает
неопубликованные записи пачкой (`FOR UPDATE SKIP LOCKED` — несколько relay не публикуют одно
событие дважды), публикует каждую с publisher confirms и только после подтверждения ставит
`published_at`. Канал открыт с `on_return_raises`: если сообщение некуда доставить, публикация
падает, а событие остаётся неопубликованным. Сбой между публикацией и коммитом даёт повторную
публикацию — доставка at-least-once, consumer к этому готов.

**Идемпотентность API.** `Idempotency-Key` обязателен и уникален в БД. Повтор с тем же ключом и
тем же телом возвращает тот же платёж (`202`); с другим телом — `409`. Тела сравниваются по
sha256 канонического JSON (`100.5` = `100.50`, порядок ключей `metadata` не важен). Гонка
параллельных запросов с одним ключом разрешается уникальным индексом: проигравший откатывается и
возвращает платёж победителя.

**Идемпотентность consumer.** Платёж читается с `SELECT … FOR UPDATE`: параллельный дубль
сообщения ждёт, пока первый обработчик не закоммитит статус, и шлюз повторно не вызывает (нет
двойного списания). Дальше:

| Состояние платежа | Действие |
|---|---|
| `pending` | шлюз → статус + `processed_at` → webhook → `webhook_delivered_at` |
| финальный, webhook не доставлен | только webhook |
| финальный, webhook доставлен | ничего (ack) |

**Retry и DLQ.**

| Ситуация | Что происходит |
|---|---|
| Отказ шлюза (10%) | бизнес-исход: статус `failed`, webhook отправляется, повтора нет |
| Технический сбой: webhook не 2xx / таймаут, ошибка БД, исключение шлюза | повтор через `payments.new.retry.N` (1 с, затем 2 с) |
| 3-я неудачная попытка | `payments.new.dlq` с `x-attempt=3` и `x-death-reason` |
| Неизвестный `payment_id`, битое сообщение | сразу в DLQ (повтор не поможет), тело сохраняется как было |
| Не удалось опубликовать повтор / в DLQ | reject → DLX основной очереди → DLQ (сообщение не теряется) |

«3 попытки» — это 3 попытки обработки всего (исходная + 2 повтора). Недоставленный webhook
статус платежа не меняет: `GET` показывает реальный итог оплаты.

**Параллелизм.** Consumer обрабатывает до `CONSUMER_PREFETCH` сообщений одновременно; пул
соединений БД подстраивается под это значение.

## API

Все эндпоинты `/api/v1/*` требуют заголовок `X-API-Key` (сравнение за постоянное время).
`/docs` и `/openapi.json` открыты, чтобы работал Swagger UI.

### `POST /api/v1/payments`

Заголовки: `X-API-Key`, `Idempotency-Key` (1–255 символов).

| Поле | Тип | Правила |
|---|---|---|
| `amount` | строка-decimal (число тоже принимается) | `> 0`, не более 2 знаков после запятой, до 18 цифр |
| `currency` | строка | `RUB`, `USD`, `EUR` |
| `description` | строка | 1–1000 символов |
| `metadata` | объект | произвольный JSON-объект, по умолчанию `{}` |
| `webhook_url` | строка | http(s) URL |

Лишние поля отклоняются (`422`).

```http
HTTP/1.1 202 Accepted

{"payment_id": "6f1c2b0e-3d6a-4b8e-9a51-2f0c7d3e9b11", "status": "pending", "created_at": "2026-11-19T12:00:00Z"}
```

Повтор с тем же ключом возвращает тот же платёж с его **текущим** статусом.

### `GET /api/v1/payments/{payment_id}`

```json
{
  "payment_id": "6f1c2b0e-3d6a-4b8e-9a51-2f0c7d3e9b11",
  "amount": "1500.00",
  "currency": "RUB",
  "description": "Заказ #1001",
  "metadata": {"order_id": 1001},
  "status": "succeeded",
  "idempotency_key": "order-1001",
  "webhook_url": "https://webhook.site/<your-id>",
  "created_at": "2026-11-19T12:00:00Z",
  "processed_at": "2026-11-19T12:00:03Z"
}
```

`processed_at` — `null`, пока платёж в `pending`. Суммы всегда строки.

### Ошибки

Единый формат для всех ошибок:

```json
{"error": {"code": "idempotency_conflict", "message": "Idempotency-Key 'order-1001' was already used with a different request", "details": null}}
```

| Статус | `code` | Когда |
|---|---|---|
| 401 | `unauthorized` | нет или неверный `X-API-Key` |
| 404 | `payment_not_found` | платёж не найден |
| 404 | `not_found` | неизвестный маршрут |
| 409 | `idempotency_conflict` | ключ уже использован с другим телом |
| 422 | `validation_error` | невалидное тело/заголовки/путь; в `details` — список `{loc, msg, type}` |
| 500 | `internal_error` | непредвиденная ошибка; детали только в логах |

### Webhook

```http
POST <webhook_url>
Content-Type: application/json
User-Agent: payment-service-webhook/0.1

{"payment_id": "6f1c2b0e-…", "status": "succeeded", "amount": "1500.00", "currency": "RUB", "processed_at": "2026-11-19T12:00:03Z"}
```

Доставка успешна при ответе `2xx` (редиректы не выполняются). Иначе, а также при таймауте
(`WEBHOOK_TIMEOUT_SECONDS`) — повтор по схеме retry. Возможна повторная доставка одного
уведомления (at-least-once), поэтому обработчик на стороне клиента должен быть идемпотентен по
`payment_id`.

## Проверка retry и DLQ вручную

1. Создайте платёж с недоступным webhook:

   ```bash
   curl -s -X POST http://localhost:8000/api/v1/payments \
     -H "X-API-Key: $API_KEY" -H "Idempotency-Key: dlq-demo-1" -H "Content-Type: application/json" \
     -d '{"amount": "10.00", "currency": "USD", "description": "dlq demo", "webhook_url": "http://localhost:1/hook"}'
   ```

2. Логи consumer покажут всю цепочку:

   ```bash
   docker compose logs -f consumer
   # payment processed payment_id=… status=succeeded
   # processing failed, retry scheduled payment_id=… attempt=1/3 error=WebhookDeliveryError: …
   # processing failed, retry scheduled payment_id=… attempt=2/3 error=WebhookDeliveryError: …
   # message dead-lettered payment_id=… attempt=3 reason=WebhookDeliveryError: …
   ```

3. RabbitMQ Management → **Queues and Streams** → `payments.new.dlq` → **Get messages**:
   у сообщения заголовки `x-attempt: 3` и `x-death-reason`.
4. `GET` платежа показывает итоговый статус (`succeeded`/`failed`) — недоставленный webhook его
   не меняет.

## Конфигурация

Все параметры — переменные окружения (см. `.env.example`).

| Переменная | Назначение | По умолчанию |
|---|---|---|
| `API_KEY` | Ключ для заголовка `X-API-Key` | обязательна (в compose — `change-me`) |
| `DATABASE_URL` | DSN PostgreSQL (`postgresql+asyncpg://…`) | обязательна (задаёт compose) |
| `RABBITMQ_URL` | URL RabbitMQ (`amqp://…`) | обязательна (задаёт compose) |
| `RETRY_BASE_DELAY_SECONDS` | База экспоненциальной задержки | `1` |
| `MAX_ATTEMPTS` | Всего попыток обработки до DLQ | `3` |
| `CONSUMER_PREFETCH` | Сколько сообщений consumer обрабатывает параллельно | `10` |
| `GATEWAY_SUCCESS_RATE` | Доля успешных платежей в эмуляторе | `0.9` |
| `GATEWAY_MIN_DELAY_SECONDS` / `GATEWAY_MAX_DELAY_SECONDS` | Задержка эмулятора | `2` / `5` |
| `WEBHOOK_TIMEOUT_SECONDS` | Таймаут отправки webhook | `5` |
| `OUTBOX_POLL_INTERVAL_SECONDS` | Интервал опроса outbox | `1` |
| `OUTBOX_BATCH_SIZE` | Размер пачки relay | `100` |
| `LOG_LEVEL` | Уровень логирования | `INFO` |

Хост-порты compose: `API_PORT` (8000), `POSTGRES_PORT` (5432), `RABBITMQ_PORT` (5672),
`RABBITMQ_MANAGEMENT_PORT` (15672).

Аргументы очередей RabbitMQ неизменяемы: после смены `RETRY_BASE_DELAY_SECONDS` или
`MAX_ATTEMPTS` удалите очереди `payments.new.retry.*` (или volume: `docker compose down -v`).

## Разработка и тесты

```bash
uv sync
docker compose up -d postgres rabbitmq
uv run ruff check . && uv run ruff format --check .
uv run mypy                # strict
uv run pytest -n auto      # ~110 тестов, несколько секунд
```

Тесты работают с настоящими PostgreSQL и RabbitMQ из compose и изолированы на уровне прогона и
xdist-воркера: каждый создаёт свежую базу `test_<run>_<worker>_<n>` и отдельный vhost
`test_<run>_<worker>` и удаляет их по завершении. Другие серверы — через `TEST_DATABASE_URL`,
`TEST_RABBITMQ_URL`, `TEST_RABBITMQ_MANAGEMENT_URL`.

Тестируется внешнее поведение через швы:

| Шов | Как | Что покрыто |
|---|---|---|
| HTTP API | `httpx` + ASGI-приложение, реальная БД | валидация, auth, идемпотентность (в т.ч. гонка), формат ошибок, GET |
| Relay | юзкейс из DI-контейнера, реальные БД и RabbitMQ | порядок, лимит, SKIP LOCKED, частичный сбой пачки, цикл и остановка |
| Consumer | FastStream `TestRabbitBroker`, фейковый шлюз, webhook через `respx` | статусы, webhook, повторная доставка, блокировка от двойного списания, retry/DLQ |
| Брокер | реальный RabbitMQ | аргументы очередей, возврат из delay-очереди по TTL (`x-death`), reject → DLQ, unroutable → ошибка; сквозной сценарий 3 попыток → DLQ |
| Миграции | Alembic на свежей базе | upgrade/downgrade/upgrade, отсутствие расхождений моделей и миграций |

## Допущения и ограничения

- Если технический сбой происходит до записи статуса (например, БД недоступна все 3 попытки),
  платёж остаётся в `pending`, а сообщение — в DLQ. Модель статусов из ТЗ (pending/succeeded/failed)
  намеренно не расширялась.
- Сообщения из DLQ автоматически не переобрабатываются; разбор — вручную через Management UI.
- Опубликованные записи outbox не удаляются (нет фоновой очистки).
- На время вызова шлюза consumer держит соединение с БД (блокировка строки платежа) — это цена
  защиты от двойного списания при параллельных дублях.
- Webhook не подписывается (HMAC), `webhook_url` не проверяется на SSRF.
- Ключ идемпотентности не истекает.
