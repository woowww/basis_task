"""Relay: юзкейс PublishOutboxBatch и цикл — на реальных PostgreSQL и RabbitMQ."""

import asyncio
import uuid
from collections.abc import AsyncIterator
from decimal import Decimal

import pytest
from dishka import AsyncContainer, Provider, Scope, provide
from faststream.rabbit import RabbitBroker
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from payment_service.application.create_payment import CreatePayment, CreatePaymentCommand
from payment_service.application.ports import MessagePublisher
from payment_service.application.publish_outbox import PublishOutboxBatch
from payment_service.config import Settings
from payment_service.domain.payment import Currency, OutboxEvent
from payment_service.infra.broker.publisher import RabbitMessagePublisher
from payment_service.infra.broker.topology import Topology
from payment_service.infra.db.models import OutboxModel
from payment_service.ioc import create_container
from payment_service.relay.runner import run_relay
from tests.conftest import CreateDatabase, run_alembic
from tests.helpers import drain, make_settings, receive


@pytest.fixture
async def settings(create_database: CreateDatabase, rabbitmq_url: str) -> Settings:
    # Своя база: на выборку relay не должны влиять события из других тестов.
    url = await create_database()
    await run_alembic(url, "upgrade", "head")
    return make_settings(url).model_copy(
        update={
            "rabbitmq_url": rabbitmq_url,
            "outbox_batch_size": 100,
            "outbox_poll_interval_seconds": 0.05,
        }
    )


@pytest.fixture
async def container(settings: Settings, broker: RabbitBroker) -> AsyncIterator[AsyncContainer]:
    container = create_container(settings, broker=broker)
    try:
        yield container
    finally:
        await container.close()


async def create_payments(container: AsyncContainer, n: int) -> list[str]:
    ids = []
    for i in range(n):
        async with container() as request:
            create = await request.get(CreatePayment)
            payment = await create(
                CreatePaymentCommand(
                    idempotency_key=f"relay-{uuid.uuid4()}",
                    amount=Decimal("10.00") + i,
                    currency=Currency.RUB,
                    description=f"payment {i}",
                    metadata={},
                    webhook_url="https://example.com/hook",
                )
            )
        ids.append(str(payment.id))
    return ids


async def publish_batch(container: AsyncContainer, limit: int = 100) -> int:
    async with container() as request:
        publish = await request.get(PublishOutboxBatch)
        return await publish(limit)


async def published_state(settings: Settings) -> dict[str, bool]:
    """payment_id → опубликовано ли событие."""
    engine = create_async_engine(settings.database_url)
    try:
        async with AsyncSession(engine) as session:
            rows = await session.scalars(select(OutboxModel))
            return {str(r.aggregate_id): r.published_at is not None for r in rows}
    finally:
        await engine.dispose()


async def test_publishes_pending_events_in_order_and_marks_them(
    container: AsyncContainer, settings: Settings, broker: RabbitBroker, topology: Topology
) -> None:
    ids = await create_payments(container, 3)

    assert await publish_batch(container) == 3

    messages = await drain(broker, topology.queue)
    assert [m.message_id for m in messages] == ids
    assert await published_state(settings) == dict.fromkeys(ids, True)


async def test_published_events_are_not_published_again(
    container: AsyncContainer, broker: RabbitBroker, topology: Topology
) -> None:
    await create_payments(container, 2)
    await publish_batch(container)
    await drain(broker, topology.queue)

    assert await publish_batch(container) == 0
    assert await drain(broker, topology.queue) == []


async def test_respects_batch_limit(
    container: AsyncContainer, settings: Settings, broker: RabbitBroker, topology: Topology
) -> None:
    ids = await create_payments(container, 5)

    assert await publish_batch(container, limit=2) == 2
    assert [m.message_id for m in await drain(broker, topology.queue)] == ids[:2]

    assert await publish_batch(container, limit=2) == 2
    assert [m.message_id for m in await drain(broker, topology.queue)] == ids[2:4]

    state = await published_state(settings)
    assert [state[i] for i in ids] == [True, True, True, True, False]


class FlakyPublisher:
    """Делегирует настоящему издателю, но падает на N-й публикации события."""

    def __init__(self, inner: RabbitMessagePublisher, fail_on_call: int) -> None:
        self._inner = inner
        self._fail_on_call = fail_on_call
        self._calls = 0

    async def publish_payment_event(self, event: OutboxEvent) -> None:
        self._calls += 1
        if self._calls == self._fail_on_call:
            raise ConnectionError("broker unavailable")
        await self._inner.publish_payment_event(event)

    async def publish_retry(self, *args: object, **kwargs: object) -> None:
        raise NotImplementedError

    async def publish_dead_letter(self, *args: object, **kwargs: object) -> None:
        raise NotImplementedError


class PublisherOverride(Provider):
    def __init__(self, publisher: object) -> None:
        super().__init__()
        self._publisher = publisher

    @provide(scope=Scope.APP)
    def publisher(self) -> MessagePublisher:
        return self._publisher  # type: ignore[return-value]


async def test_failure_mid_batch_keeps_published_part_and_retries_rest(
    container: AsyncContainer,
    settings: Settings,
    broker: RabbitBroker,
    topology: Topology,
) -> None:
    ids = await create_payments(container, 3)
    flaky = FlakyPublisher(RabbitMessagePublisher(broker, topology), fail_on_call=2)
    flaky_container = create_container(settings, PublisherOverride(flaky), broker=broker)
    try:
        with pytest.raises(ConnectionError):
            await publish_batch(flaky_container)
    finally:
        await flaky_container.close()

    state = await published_state(settings)
    assert [state[i] for i in ids] == [True, False, False]
    assert [m.message_id for m in await drain(broker, topology.queue)] == ids[:1]

    # Следующий проход штатного relay дожимает остаток.
    assert await publish_batch(container) == 2
    assert [m.message_id for m in await drain(broker, topology.queue)] == ids[1:]


async def test_concurrent_relays_do_not_publish_event_twice(
    container: AsyncContainer, broker: RabbitBroker, topology: Topology
) -> None:
    ids = await create_payments(container, 20)

    counts = await asyncio.gather(*(publish_batch(container) for _ in range(3)))

    assert sum(counts) == 20
    messages = await drain(broker, topology.queue)
    assert sorted(m.message_id or "" for m in messages) == sorted(ids)


async def test_relay_loop_publishes_new_events_and_stops_gracefully(
    container: AsyncContainer, settings: Settings, broker: RabbitBroker, topology: Topology
) -> None:
    stop = asyncio.Event()
    relay = asyncio.create_task(run_relay(container, settings, stop))
    try:
        [payment_id] = await create_payments(container, 1)

        message = await receive(broker, topology.queue)
        assert message.message_id == payment_id
    finally:
        stop.set()
        await asyncio.wait_for(relay, timeout=2)


async def test_relay_loop_survives_failed_iteration(
    container: AsyncContainer,
    settings: Settings,
    broker: RabbitBroker,
    topology: Topology,
    caplog: pytest.LogCaptureFixture,
) -> None:
    [payment_id] = await create_payments(container, 1)
    flaky = FlakyPublisher(RabbitMessagePublisher(broker, topology), fail_on_call=1)
    flaky_container = create_container(settings, PublisherOverride(flaky), broker=broker)
    stop = asyncio.Event()
    relay = asyncio.create_task(run_relay(flaky_container, settings, stop))
    try:
        message = await receive(broker, topology.queue)
        assert message.message_id == payment_id
    finally:
        stop.set()
        await asyncio.wait_for(relay, timeout=2)
        await flaky_container.close()

    assert "outbox relay iteration failed" in caplog.text
