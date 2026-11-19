import asyncio
import uuid
from typing import Any

import pytest
from aio_pika.abc import AbstractIncomingMessage
from faststream.rabbit import RabbitBroker, RabbitQueue

from payment_service.config import Settings

API_KEY = "test-api-key"
AUTH = {"X-API-Key": API_KEY}


def make_settings(database_url: str = "postgresql+asyncpg://u:p@localhost/db") -> Settings:
    return Settings(
        api_key=API_KEY,
        database_url=database_url,
        rabbitmq_url="amqp://guest:guest@localhost/",
    )


def payment_body(**overrides: Any) -> dict[str, Any]:
    return {
        "amount": "1500.00",
        "currency": "RUB",
        "description": "Заказ #1001",
        "metadata": {"order_id": 1001, "tags": ["a", "b"]},
        "webhook_url": "https://example.com/hooks/payments",
    } | overrides


def new_idempotency_key() -> str:
    return f"key-{uuid.uuid4()}"


async def receive(
    broker: RabbitBroker, queue: RabbitQueue, within: float = 3.0, *, no_ack: bool = True
) -> AbstractIncomingMessage:
    """Дождаться одного сообщения в очереди (или упасть по таймауту)."""
    declared = await broker.declare_queue(queue)
    loop = asyncio.get_running_loop()
    deadline = loop.time() + within
    while True:
        message = await declared.get(no_ack=no_ack, fail=False)
        if message is not None:
            return message
        if loop.time() > deadline:
            pytest.fail(f"no message in {queue.name} within {within}s")
        await asyncio.sleep(0.02)


async def drain(broker: RabbitBroker, queue: RabbitQueue) -> list[AbstractIncomingMessage]:
    """Забрать все сообщения, которые сейчас лежат в очереди."""
    declared = await broker.declare_queue(queue)
    messages: list[AbstractIncomingMessage] = []
    while (message := await declared.get(no_ack=True, fail=False)) is not None:
        messages.append(message)
    return messages
