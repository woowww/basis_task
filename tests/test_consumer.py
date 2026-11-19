"""Consumer через FastStream TestRabbitBroker: реальная БД, фейковый шлюз, webhook через respx."""

import asyncio
import json
import uuid
from collections import defaultdict
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import httpx
import pytest
import respx
from faststream.rabbit import RabbitBroker, TestRabbitBroker
from faststream.rabbit.annotations import RabbitMessage
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from payment_service.application.create_payment import CreatePayment, CreatePaymentCommand
from payment_service.consumer.main import Consumer, create_consumer
from payment_service.domain.payment import Currency, PaymentStatus
from payment_service.infra.broker.topology import ROUTING_KEY
from payment_service.infra.db.models import PaymentModel
from tests.fakes import FakeGateway, GatewayOverride
from tests.helpers import make_settings

WEBHOOK_URL = "https://client.example.com/hooks/payments"


@pytest.fixture
def gateway() -> FakeGateway:
    return FakeGateway()


@dataclass(frozen=True)
class Captured:
    body: bytes
    headers: dict[str, Any]
    message_id: str
    content_type: str | None


def capture_into(store: list[Captured]) -> Callable[[RabbitMessage], Awaitable[None]]:
    async def capture(message: RabbitMessage) -> None:
        store.append(
            Captured(
                body=message.body,
                headers=dict(message.headers),
                message_id=message.message_id,
                content_type=message.content_type,
            )
        )

    return capture


@pytest.fixture
def captured() -> dict[str, list[Captured]]:
    """Сообщения, переложенные consumer'ом: имя очереди → список."""
    return defaultdict(list)


@pytest.fixture
async def consumer(
    migrated_db_url: str, gateway: FakeGateway, captured: dict[str, list[Captured]]
) -> AsyncIterator[Consumer]:
    consumer = create_consumer(make_settings(migrated_db_url), GatewayOverride(gateway))
    topology = consumer.topology
    targets = [(q, topology.retry_exchange) for q in topology.retry_queues]
    targets.append((topology.dead_letter_queue, topology.dead_letter_exchange))
    for queue, exchange in targets:
        consumer.broker.subscriber(queue, exchange)(capture_into(captured[queue.name]))
    try:
        yield consumer
    finally:
        await consumer.container.close()


@pytest.fixture
async def broker(consumer: Consumer) -> AsyncIterator[RabbitBroker]:
    """Брокер consumer, подменённый in-memory транспортом FastStream."""
    async with TestRabbitBroker(consumer.broker) as broker:
        yield broker


@pytest.fixture
def webhook(respx_mock: respx.MockRouter) -> respx.Route:
    return respx_mock.post(WEBHOOK_URL).respond(200)


async def create_payment(consumer: Consumer) -> uuid.UUID:
    async with consumer.container() as request:
        create = await request.get(CreatePayment)
        payment = await create(
            CreatePaymentCommand(
                idempotency_key=f"consumer-{uuid.uuid4()}",
                amount=Decimal("250.00"),
                currency=Currency.USD,
                description="consumer test",
                metadata={},
                webhook_url=WEBHOOK_URL,
            )
        )
    return payment.id


async def deliver(
    broker: RabbitBroker, consumer: Consumer, payment_id: uuid.UUID, attempt: int | None = None
) -> None:
    await broker.publish(
        {"payment_id": str(payment_id)},
        exchange=consumer.topology.exchange,
        routing_key=ROUTING_KEY,
        message_id=str(payment_id),
        headers={"x-attempt": attempt} if attempt is not None else None,
    )


async def redeliver(broker: RabbitBroker, consumer: Consumer, message: Captured) -> None:
    """То, что сделал бы RabbitMQ по истечении TTL delay-очереди."""
    await broker.publish(
        message.body,
        exchange=consumer.topology.exchange,
        routing_key=ROUTING_KEY,
        message_id=message.message_id,
        headers=message.headers,
        content_type=message.content_type,
    )


async def load(db: AsyncEngine, payment_id: uuid.UUID) -> PaymentModel:
    async with AsyncSession(db) as session:
        row = await session.get(PaymentModel, payment_id)
        assert row is not None
        return row


def webhook_bodies(route: respx.Route) -> list[dict[str, object]]:
    return [json.loads(call.request.content) for call in route.calls]


@pytest.mark.parametrize("outcome", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED])
async def test_processes_pending_payment_and_notifies(
    consumer: Consumer,
    broker: RabbitBroker,
    gateway: FakeGateway,
    webhook: respx.Route,
    db: AsyncEngine,
    outcome: PaymentStatus,
) -> None:
    gateway.outcome = outcome
    payment_id = await create_payment(consumer)

    await deliver(broker, consumer, payment_id)

    row = await load(db, payment_id)
    assert row.status is outcome
    assert row.processed_at is not None
    assert row.webhook_delivered_at is not None
    assert gateway.calls == [payment_id]
    assert webhook_bodies(webhook) == [
        {
            "payment_id": str(payment_id),
            "status": outcome.value,
            "amount": "250.00",
            "currency": "USD",
            "processed_at": row.processed_at.isoformat(),
        }
    ]


async def test_duplicate_of_processed_payment_is_noop(
    consumer: Consumer,
    broker: RabbitBroker,
    gateway: FakeGateway,
    webhook: respx.Route,
    db: AsyncEngine,
) -> None:
    payment_id = await create_payment(consumer)
    await deliver(broker, consumer, payment_id)
    before = await load(db, payment_id)

    await deliver(broker, consumer, payment_id)

    after = await load(db, payment_id)
    assert gateway.calls == [payment_id]
    assert webhook.call_count == 1
    assert (after.processed_at, after.webhook_delivered_at) == (
        before.processed_at,
        before.webhook_delivered_at,
    )


async def test_final_status_without_webhook_only_sends_webhook(
    consumer: Consumer,
    broker: RabbitBroker,
    gateway: FakeGateway,
    webhook: respx.Route,
    db: AsyncEngine,
) -> None:
    payment_id = await create_payment(consumer)
    processed_at = datetime(2026, 11, 19, 12, 0, tzinfo=UTC)
    async with db.begin() as conn:
        await conn.execute(
            update(PaymentModel)
            .where(PaymentModel.id == payment_id)
            .values(status=PaymentStatus.FAILED, processed_at=processed_at)
        )

    await deliver(broker, consumer, payment_id)

    row = await load(db, payment_id)
    assert gateway.calls == []
    assert row.status is PaymentStatus.FAILED
    assert row.processed_at == processed_at
    assert row.webhook_delivered_at is not None
    [body] = webhook_bodies(webhook)
    assert body["status"] == "failed"
    assert body["processed_at"] == processed_at.isoformat()


async def test_webhook_failure_keeps_status_and_retry_only_resends_webhook(
    consumer: Consumer,
    broker: RabbitBroker,
    gateway: FakeGateway,
    respx_mock: respx.MockRouter,
    db: AsyncEngine,
    captured: dict[str, list[Captured]],
) -> None:
    route = respx_mock.post(WEBHOOK_URL).mock(
        side_effect=[httpx.Response(503), httpx.Response(200)]
    )
    payment_id = await create_payment(consumer)

    await deliver(broker, consumer, payment_id)

    row = await load(db, payment_id)
    assert row.status is PaymentStatus.SUCCEEDED  # итог оплаты зафиксирован
    assert row.webhook_delivered_at is None
    [retry] = captured["payments.new.retry.1"]

    await redeliver(broker, consumer, retry)

    row = await load(db, payment_id)
    assert row.webhook_delivered_at is not None
    assert gateway.calls == [payment_id]  # шлюз повторно не вызывался
    assert route.call_count == 2
    assert captured["payments.new.dlq"] == []


async def test_concurrent_duplicates_call_gateway_once(
    consumer: Consumer,
    broker: RabbitBroker,
    gateway: FakeGateway,
    webhook: respx.Route,
    db: AsyncEngine,
) -> None:
    gateway.delay = 0.2  # первый обработчик держит блокировку, второй ждёт
    payment_id = await create_payment(consumer)

    await asyncio.gather(*(deliver(broker, consumer, payment_id) for _ in range(3)))

    assert gateway.calls == [payment_id]
    assert (await load(db, payment_id)).status is PaymentStatus.SUCCEEDED
    assert webhook.call_count >= 1  # дубль webhook возможен (at-least-once), см. README


# --- retry / DLQ -------------------------------------------------------------------------


@pytest.mark.parametrize(("attempt", "retry_queue"), [(None, 1), (1, 1), (2, 2)])
async def test_technical_failure_is_scheduled_for_retry(
    consumer: Consumer,
    broker: RabbitBroker,
    respx_mock: respx.MockRouter,
    captured: dict[str, list[Captured]],
    attempt: int | None,
    retry_queue: int,
) -> None:
    respx_mock.post(WEBHOOK_URL).respond(503)
    payment_id = await create_payment(consumer)

    await deliver(broker, consumer, payment_id, attempt=attempt)

    [retry] = captured[f"payments.new.retry.{retry_queue}"]
    assert json.loads(retry.body) == {"payment_id": str(payment_id)}
    assert retry.message_id == str(payment_id)
    assert retry.headers["x-attempt"] == (attempt or 1) + 1
    assert captured["payments.new.dlq"] == []


async def test_last_attempt_failure_goes_to_dlq_with_reason(
    consumer: Consumer,
    broker: RabbitBroker,
    respx_mock: respx.MockRouter,
    db: AsyncEngine,
    captured: dict[str, list[Captured]],
) -> None:
    respx_mock.post(WEBHOOK_URL).respond(503)
    payment_id = await create_payment(consumer)

    await deliver(broker, consumer, payment_id, attempt=3)

    [dead] = captured["payments.new.dlq"]
    assert json.loads(dead.body) == {"payment_id": str(payment_id)}
    assert dead.message_id == str(payment_id)
    assert dead.headers["x-attempt"] == 3
    assert dead.headers["x-death-reason"] == "WebhookDeliveryError: webhook responded 503"
    assert not any(captured[q.name] for q in consumer.topology.retry_queues)
    row = await load(db, payment_id)
    assert row.status is PaymentStatus.SUCCEEDED  # статус не меняется из-за webhook
    assert row.webhook_delivered_at is None


async def test_retries_follow_exponential_chain_then_dlq(
    consumer: Consumer,
    broker: RabbitBroker,
    respx_mock: respx.MockRouter,
    captured: dict[str, list[Captured]],
) -> None:
    route = respx_mock.post(WEBHOOK_URL).respond(503)
    payment_id = await create_payment(consumer)

    await deliver(broker, consumer, payment_id)
    await redeliver(broker, consumer, captured["payments.new.retry.1"][0])
    await redeliver(broker, consumer, captured["payments.new.retry.2"][0])

    assert route.call_count == 3
    assert len(captured["payments.new.retry.1"]) == len(captured["payments.new.retry.2"]) == 1
    [dead] = captured["payments.new.dlq"]
    assert dead.headers["x-attempt"] == 3


async def test_recovers_on_second_attempt(
    consumer: Consumer,
    broker: RabbitBroker,
    respx_mock: respx.MockRouter,
    db: AsyncEngine,
    captured: dict[str, list[Captured]],
) -> None:
    respx_mock.post(WEBHOOK_URL).mock(side_effect=[httpx.Response(500), httpx.Response(204)])
    payment_id = await create_payment(consumer)

    await deliver(broker, consumer, payment_id)
    await redeliver(broker, consumer, captured["payments.new.retry.1"][0])

    assert (await load(db, payment_id)).webhook_delivered_at is not None
    assert captured["payments.new.retry.2"] == []
    assert captured["payments.new.dlq"] == []


async def test_gateway_exception_is_retried_and_payment_stays_pending(
    consumer: Consumer,
    broker: RabbitBroker,
    gateway: FakeGateway,
    db: AsyncEngine,
    captured: dict[str, list[Captured]],
) -> None:
    gateway.error = ConnectionError("gateway unreachable")
    payment_id = await create_payment(consumer)

    await deliver(broker, consumer, payment_id)

    assert (await load(db, payment_id)).status is PaymentStatus.PENDING
    [retry] = captured["payments.new.retry.1"]
    assert retry.headers["x-attempt"] == 2


async def test_unknown_payment_goes_straight_to_dlq(
    consumer: Consumer, broker: RabbitBroker, captured: dict[str, list[Captured]]
) -> None:
    payment_id = uuid.uuid4()

    await deliver(broker, consumer, payment_id)

    [dead] = captured["payments.new.dlq"]
    assert dead.headers["x-attempt"] == 1
    assert dead.headers["x-death-reason"] == (
        f"PaymentNotFoundError: Payment {payment_id} not found"
    )
    assert not any(captured[q.name] for q in consumer.topology.retry_queues)


@pytest.mark.parametrize(
    ("body", "content_type"),
    [
        pytest.param(b"not json at all", "text/plain", id="not-json"),
        pytest.param(b'{"order": 1}', "application/json", id="no-payment-id"),
        pytest.param(b'{"payment_id": "nope"}', "application/json", id="bad-uuid"),
        pytest.param(b"[1, 2]", "application/json", id="not-object"),
    ],
)
async def test_malformed_message_goes_to_dlq_unchanged(
    consumer: Consumer,
    broker: RabbitBroker,
    captured: dict[str, list[Captured]],
    body: bytes,
    content_type: str,
) -> None:
    await broker.publish(
        body,
        exchange=consumer.topology.exchange,
        routing_key=ROUTING_KEY,
        message_id="broken-1",
        content_type=content_type,
    )

    [dead] = captured["payments.new.dlq"]
    assert dead.body == body
    assert dead.message_id == "broken-1"
    assert dead.headers["x-death-reason"].startswith("invalid message")
    assert not any(captured[q.name] for q in consumer.topology.retry_queues)
