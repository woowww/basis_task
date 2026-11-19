"""Consumer через FastStream TestRabbitBroker: реальная БД, фейковый шлюз, webhook через respx."""

import asyncio
import json
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest
import respx
from faststream.rabbit import RabbitBroker, TestRabbitBroker
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from payment_service.application.create_payment import CreatePayment, CreatePaymentCommand
from payment_service.application.errors import WebhookDeliveryError
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


@pytest.fixture
async def consumer(migrated_db_url: str, gateway: FakeGateway) -> AsyncIterator[Consumer]:
    consumer = create_consumer(make_settings(migrated_db_url), GatewayOverride(gateway))
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


async def deliver(broker: RabbitBroker, consumer: Consumer, payment_id: uuid.UUID) -> None:
    await broker.publish(
        {"payment_id": str(payment_id)},
        exchange=consumer.topology.exchange,
        routing_key=ROUTING_KEY,
        message_id=str(payment_id),
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


async def test_webhook_failure_keeps_status_and_redelivery_only_resends_webhook(
    consumer: Consumer,
    broker: RabbitBroker,
    gateway: FakeGateway,
    respx_mock: respx.MockRouter,
    db: AsyncEngine,
) -> None:
    route = respx_mock.post(WEBHOOK_URL).mock(
        side_effect=[httpx.Response(503), httpx.Response(200)]
    )
    payment_id = await create_payment(consumer)

    with pytest.raises(WebhookDeliveryError, match="503"):
        await deliver(broker, consumer, payment_id)

    row = await load(db, payment_id)
    assert row.status is PaymentStatus.SUCCEEDED  # итог оплаты зафиксирован
    assert row.webhook_delivered_at is None

    await deliver(broker, consumer, payment_id)  # повторная доставка сообщения

    row = await load(db, payment_id)
    assert row.webhook_delivered_at is not None
    assert gateway.calls == [payment_id]  # шлюз повторно не вызывался
    assert route.call_count == 2


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
