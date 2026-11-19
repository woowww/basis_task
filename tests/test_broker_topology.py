"""Топология и публикация — против настоящего RabbitMQ (свой vhost на воркер)."""

import json
import uuid
from typing import Any, cast

import pytest
from faststream.rabbit import RabbitBroker

from payment_service.domain.payment import OutboxEvent
from payment_service.infra.broker.publisher import RabbitMessagePublisher
from payment_service.infra.broker.topology import (
    DEAD_LETTER_QUEUE,
    QUEUE,
    Topology,
    retry_queue_name,
)
from tests.conftest import rabbitmq_management_client
from tests.helpers import receive


@pytest.fixture
def publisher(broker: RabbitBroker, topology: Topology) -> RabbitMessagePublisher:
    return RabbitMessagePublisher(broker, topology)


async def queue_info(rabbitmq_url: str, name: str) -> dict[str, Any]:
    vhost = rabbitmq_url.rsplit("/", 1)[1]
    async with rabbitmq_management_client() as api:
        response = await api.get(f"/api/queues/{vhost}/{name}")
        response.raise_for_status()
        data: dict[str, Any] = response.json()
        return data


async def test_declares_queues_with_expected_arguments(
    broker: RabbitBroker, topology: Topology, rabbitmq_url: str
) -> None:
    await topology.declare(broker)  # повторное объявление идемпотентно

    main = await queue_info(rabbitmq_url, QUEUE)
    assert main["durable"] is True
    assert main["arguments"] == {
        "x-dead-letter-exchange": "payments.dlx",
        "x-dead-letter-routing-key": "payments.new",
        "x-queue-type": "classic",
    }
    for n, ttl in [(1, 100), (2, 200)]:
        retry = await queue_info(rabbitmq_url, retry_queue_name(n))
        assert retry["arguments"] == {
            "x-message-ttl": ttl,
            "x-dead-letter-exchange": "payments",
            "x-dead-letter-routing-key": "payments.new",
            "x-queue-type": "classic",
        }
    assert (await queue_info(rabbitmq_url, DEAD_LETTER_QUEUE))["durable"] is True
    assert topology.max_attempts == 3


async def test_payment_event_lands_in_main_queue(
    broker: RabbitBroker, topology: Topology, publisher: RabbitMessagePublisher
) -> None:
    payment_id = uuid.uuid4()
    event = OutboxEvent(
        aggregate_id=payment_id, event_type="payments.new", payload={"payment_id": str(payment_id)}
    )

    await publisher.publish_payment_event(event)

    message = await receive(broker, topology.queue)
    assert json.loads(message.body) == {"payment_id": str(payment_id)}
    assert message.message_id == str(payment_id)
    assert message.headers["x-attempt"] == 1
    assert message.delivery_mode == 2  # persistent


@pytest.mark.parametrize("next_attempt", [2, 3])
async def test_retry_returns_to_main_queue_after_ttl(
    broker: RabbitBroker,
    topology: Topology,
    publisher: RabbitMessagePublisher,
    next_attempt: int,
) -> None:
    message_id = str(uuid.uuid4())

    await publisher.publish_retry(
        {"payment_id": message_id}, message_id=message_id, next_attempt=next_attempt
    )

    message = await receive(broker, topology.queue)
    assert message.message_id == message_id
    assert message.headers["x-attempt"] == next_attempt
    assert message.delivery_mode == 2
    # RabbitMQ фиксирует, через какую delay-очередь прошло сообщение.
    death = cast(list[dict[str, Any]], message.headers["x-death"])[0]
    assert death["queue"] == retry_queue_name(next_attempt - 1)
    assert death["reason"] == "expired"


@pytest.mark.parametrize("next_attempt", [1, 4])
async def test_retry_rejects_out_of_range_attempt(
    publisher: RabbitMessagePublisher, next_attempt: int
) -> None:
    with pytest.raises(ValueError, match="next_attempt"):
        await publisher.publish_retry({}, message_id="m", next_attempt=next_attempt)


async def test_dead_letter_lands_in_dlq_with_reason(
    broker: RabbitBroker, topology: Topology, publisher: RabbitMessagePublisher
) -> None:
    message_id = str(uuid.uuid4())

    await publisher.publish_dead_letter(
        {"payment_id": message_id}, message_id=message_id, attempts=3, reason="webhook: 503"
    )

    message = await receive(broker, topology.dead_letter_queue)
    assert json.loads(message.body) == {"payment_id": message_id}
    assert message.headers["x-attempt"] == 3
    assert message.headers["x-death-reason"] == "webhook: 503"


async def test_rejected_message_falls_into_dlq(
    broker: RabbitBroker, topology: Topology, publisher: RabbitMessagePublisher
) -> None:
    payment_id = uuid.uuid4()
    await publisher.publish_payment_event(
        OutboxEvent(aggregate_id=payment_id, event_type="payments.new", payload={})
    )
    message = await receive(broker, topology.queue, no_ack=False)

    await message.reject(requeue=False)

    dead = await receive(broker, topology.dead_letter_queue)
    assert dead.message_id == str(payment_id)


async def test_unroutable_publish_raises(broker: RabbitBroker, topology: Topology) -> None:
    with pytest.raises(Exception):  # noqa: B017, PT011 — тип зависит от драйвера
        await broker.publish({}, exchange=topology.exchange, routing_key="nowhere")
