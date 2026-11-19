import uuid
from typing import Any

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
