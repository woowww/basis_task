# Payment Processing Service

Микросервис асинхронной обработки платежей. Принимает запрос на оплату, сразу отвечает `202 Accepted`, обрабатывает платёж через эмулятор внешнего шлюза и уведомляет клиента о результате через webhook.

**Стек:** FastAPI + Pydantic v2, SQLAlchemy 2.0 (async), PostgreSQL, RabbitMQ (FastStream), Alembic, Docker Compose.

## Архитектура

```
              ┌──────────── одна транзакция ────────────┐
POST ──► api ─┤ INSERT payments  +  INSERT outbox        ├──► PostgreSQL
              └──────────────────────────────────────────┘        │
                                                                   ▼
                                                     outbox-relay (poll, SKIP LOCKED,
                                                     publisher confirms)
                                                                   │
                                                                   ▼
                                        RabbitMQ: exchange payments ─► queue payments.new
                                                                   │
                                                                   ▼
                                     consumer: шлюз (2–5 с, 90/10) ─► статус в БД ─► webhook
                                                 │ техническая ошибка
                                                 ▼
                         payments.new.retry.{1,2} (TTL) ──► обратно в payments.new
                                                 │ 3-я неудача
                                                 ▼
                                         payments.new.dlq
```

| Сервис | Роль |
|---|---|
| `postgres` | Хранилище платежей и outbox |
| `rabbitmq` | Брокер сообщений (с Management UI) |
| `migrate` | Одноразовый запуск `alembic upgrade head` перед стартом приложений |
| `api` | HTTP API |
| `outbox-relay` | Публикует события из таблицы `outbox` в RabbitMQ |
| `consumer` | Обрабатывает платёж, обновляет статус, отправляет webhook |

### Гарантии

- **Outbox pattern.** Платёж и событие пишутся в одной транзакции; relay помечает событие опубликованным только после подтверждения брокера. Доставка at-least-once.
- **Идемпотентность API.** Обязательный заголовок `Idempotency-Key`. Повтор с тем же ключом и тем же телом возвращает тот же платёж; с другим телом — `409 Conflict`.
- **Идемпотентность consumer.** Повторная доставка не запускает шлюз повторно для уже обработанного платежа; если статус записан, а webhook не доставлен — досылается только webhook.
- **Retry.** Технические сбои (webhook ответил не 2xx / таймаут, сбой БД) повторяются через delay-очереди RabbitMQ с экспоненциальной задержкой. Всего 3 попытки.
- **DLQ.** После 3 неудачных попыток сообщение уходит в `payments.new.dlq`.
- Отказ шлюза (10%) — это бизнес-результат: статус `failed`, webhook отправляется, повтора нет.

## Быстрый старт

```bash
cp .env.example .env
docker compose up --build
```

- API: http://localhost:8000 (Swagger UI: http://localhost:8000/docs)
- RabbitMQ Management: http://localhost:15672 (guest / guest)

## Конфигурация

Все параметры задаются через переменные окружения (см. `.env.example`).

| Переменная | Назначение | По умолчанию |
|---|---|---|
| `API_KEY` | Статический ключ для заголовка `X-API-Key` | — (обязательна) |
| `DATABASE_URL` | DSN PostgreSQL (`postgresql+asyncpg://…`) | — |
| `RABBITMQ_URL` | URL RabbitMQ (`amqp://…`) | — |
| `RETRY_BASE_DELAY_SECONDS` | База экспоненциальной задержки (`база × 2^(n−1)`) | `1` |
| `MAX_ATTEMPTS` | Всего попыток обработки до DLQ | `3` |
| `CONSUMER_PREFETCH` | Prefetch consumer | `10` |
| `GATEWAY_SUCCESS_RATE` | Доля успешных платежей в эмуляторе | `0.9` |
| `GATEWAY_MIN_DELAY_SECONDS` / `GATEWAY_MAX_DELAY_SECONDS` | Диапазон задержки эмулятора | `2` / `5` |
| `WEBHOOK_TIMEOUT_SECONDS` | Таймаут отправки webhook | `5` |
| `OUTBOX_POLL_INTERVAL_SECONDS` | Интервал опроса outbox | `1` |
| `OUTBOX_BATCH_SIZE` | Размер пачки relay | `100` |

## API

Все запросы требуют заголовок `X-API-Key`. Без него или с неверным ключом — `401`.

### Создание платежа

`POST /api/v1/payments`

```bash
curl -i -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: $API_KEY" \
  -H "Idempotency-Key: order-1001" \
  -H "Content-Type: application/json" \
  -d '{
        "amount": "1500.00",
        "currency": "RUB",
        "description": "Заказ #1001",
        "metadata": {"order_id": 1001},
        "webhook_url": "https://webhook.site/<your-id>"
      }'
```

```http
HTTP/1.1 202 Accepted

{
  "payment_id": "6f1c2b0e-3d6a-4b8e-9a51-2f0c7d3e9b11",
  "status": "pending",
  "created_at": "2026-11-19T12:00:00Z"
}
```

Правила валидации:
- `amount` — строка-десятичное число, `> 0`, не более 2 знаков после запятой;
- `currency` — `RUB`, `USD` или `EUR`;
- `webhook_url` — валидный URL;
- `metadata` — произвольный JSON-объект (по умолчанию `{}`);
- `Idempotency-Key` — обязателен.

Ошибки валидации — `422`.

**Повтор с тем же ключом и телом** — тот же ответ `202` с тем же `payment_id`, новый платёж не создаётся.

**Тот же ключ, другое тело** — `409 Conflict`:

```bash
curl -i -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: $API_KEY" \
  -H "Idempotency-Key: order-1001" \
  -H "Content-Type: application/json" \
  -d '{"amount": "99.00", "currency": "RUB", "description": "другое", "webhook_url": "https://webhook.site/<your-id>"}'
```

### Получение платежа

`GET /api/v1/payments/{payment_id}`

```bash
curl -s http://localhost:8000/api/v1/payments/6f1c2b0e-3d6a-4b8e-9a51-2f0c7d3e9b11 \
  -H "X-API-Key: $API_KEY"
```

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

`processed_at` равен `null`, пока платёж в `pending`. Несуществующий платёж — `404`.

### Webhook

После обработки сервис отправляет на `webhook_url`:

```http
POST <webhook_url>
Content-Type: application/json

{
  "payment_id": "6f1c2b0e-3d6a-4b8e-9a51-2f0c7d3e9b11",
  "status": "succeeded",
  "amount": "1500.00",
  "currency": "RUB",
  "processed_at": "2026-11-19T12:00:03Z"
}
```

Успешной доставкой считается любой ответ `2xx`. Не-2xx или таймаут — повтор по схеме retry. Возможна повторная доставка одного и того же уведомления, поэтому обработчик на стороне клиента должен быть идемпотентен по `payment_id`.

## Проверка retry и DLQ

1. Создайте платёж с недоступным `webhook_url`, например `"webhook_url": "http://localhost:1/hook"`.
2. Откройте RabbitMQ Management → **Queues**: сообщение пройдёт через `payments.new.retry.1`, `payments.new.retry.2` и окажется в `payments.new.dlq`.
3. `GET` платежа покажет итоговый статус (`succeeded`/`failed`) — недоставленный webhook статус не меняет.

## Разработка

```bash
uv sync
docker compose up -d postgres rabbitmq   # PostgreSQL :5432, RabbitMQ :5672 / :15672
uv run ruff check .
uv run mypy
uv run pytest            # или параллельно: uv run pytest -n auto
```

Тесты изолированы на уровне прогона и xdist-воркера: каждый создаёт свежую базу
`test_<run>_<worker>_<n>` в PostgreSQL и отдельный vhost `test_<run>_<worker>` в RabbitMQ
и удаляет их по завершении. Другие серверы — через `TEST_DATABASE_URL`, `TEST_RABBITMQ_URL`,
`TEST_RABBITMQ_MANAGEMENT_URL`; другие хост-порты compose — через `POSTGRES_PORT`,
`RABBITMQ_PORT`, `RABBITMQ_MANAGEMENT_PORT`.

Аргументы очередей RabbitMQ неизменяемы: после смены `RETRY_BASE_DELAY_SECONDS` или
`MAX_ATTEMPTS` удалите очереди `payments.new.retry.*` (или volume `rabbitmq-data`).

## Известные ограничения

- Если технический сбой происходит до записи статуса (например, БД недоступна все 3 попытки), платёж остаётся в `pending`, а сообщение — в DLQ.
- Сообщения из DLQ не переобрабатываются автоматически.
- Опубликованные записи outbox не удаляются.
- Webhook не подписывается; `webhook_url` не проверяется на SSRF.
