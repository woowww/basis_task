"""Consumer на настоящем RabbitMQ: реальные TTL-очереди, DLX и DLQ."""

import asyncio
import uuid
from collections.abc import AsyncIterator
from decimal import Decimal

import pytest
import respx
from faststream.rabbit import RabbitBroker
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from payment_service.application.create_payment import CreatePayment, CreatePaymentCommand
from payment_service.consumer.main import Consumer, create_consumer
from payment_service.domain.payment import Currency, OutboxEvent, Payment, PaymentStatus
from payment_service.infra.broker.publisher import RabbitMessagePublisher
from payment_service.infra.broker.topology import Topology
from payment_service.infra.db.models import PaymentModel
from tests.conftest import TEST_RETRY_BASE_DELAY
from tests.fakes import FakeGateway, GatewayOverride
from tests.helpers import make_settings, receive

WEBHOOK_URL = "https://client.example.com/hooks/rabbit"


@pytest.fixture
async def consumer(
    migrated_db_url: str, rabbitmq_url: str, broker: RabbitBroker
) -> AsyncIterator[Consumer]:
    # `broker` (conftest) объявил топологию и очистил очереди на тестовом vhost.
    settings = make_settings(migrated_db_url).model_copy(
        update={"rabbitmq_url": rabbitmq_url, "retry_base_delay_seconds": TEST_RETRY_BASE_DELAY}
    )
    consumer = create_consumer(settings, GatewayOverride(FakeGateway()))
    await consumer.broker.start()
    try:
        yield consumer
    finally:
        await consumer.broker.stop()
        await consumer.container.close()


async def create_and_publish(
    consumer: Consumer, broker: RabbitBroker, topology: Topology
) -> Payment:
    async with consumer.container() as request:
        create = await request.get(CreatePayment)
        payment = await create(
            CreatePaymentCommand(
                idempotency_key=f"rabbit-{uuid.uuid4()}",
                amount=Decimal("42.00"),
                currency=Currency.EUR,
                description="rabbit e2e",
                metadata={},
                webhook_url=WEBHOOK_URL,
            )
        )
    await RabbitMessagePublisher(broker, topology).publish_payment_event(
        OutboxEvent.payment_created(payment)
    )
    return payment


async def test_persistent_webhook_failure_ends_in_dlq_after_three_attempts(
    consumer: Consumer, broker: RabbitBroker, topology: Topology
) -> None:
    with respx.mock(assert_all_called=False) as mock:
        route = mock.post(WEBHOOK_URL).respond(503)
        started = asyncio.get_running_loop().time()

        payment = await create_and_publish(consumer, broker, topology)
        dead = await receive(broker, topology.dead_letter_queue, within=5)

        elapsed = asyncio.get_running_loop().time() - started

    assert dead.message_id == str(payment.id)
    assert dead.headers["x-attempt"] == 3
    assert dead.headers["x-death-reason"] == "WebhookDeliveryError: webhook responded 503"
    assert route.call_count == 3
    # Прошли через обе delay-очереди: 100 мс + 200 мс.
    assert elapsed >= 3 * TEST_RETRY_BASE_DELAY


async def test_happy_path_on_real_broker(
    consumer: Consumer, broker: RabbitBroker, topology: Topology, db: AsyncEngine
) -> None:
    with respx.mock(assert_all_called=False) as mock:
        route = mock.post(WEBHOOK_URL).respond(200)
        payment = await create_and_publish(consumer, broker, topology)

        row = None
        for _ in range(150):
            async with AsyncSession(db) as session:
                row = await session.get(PaymentModel, payment.id)
            if row is not None and row.webhook_delivered_at is not None:
                break
            await asyncio.sleep(0.02)

    assert row is not None
    assert row.status is PaymentStatus.SUCCEEDED
    assert row.webhook_delivered_at is not None
    assert route.call_count == 1
