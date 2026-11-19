import uuid
from collections.abc import AsyncIterator

import pytest
from httpx import ASGITransport, AsyncClient

from payment_service.api.app import create_app
from payment_service.ioc import create_container
from tests.helpers import AUTH, make_settings, new_idempotency_key, payment_body

UNREACHABLE_DB = "postgresql+asyncpg://payments:payments@127.0.0.1:1/payments"


@pytest.fixture
async def client_without_db() -> AsyncIterator[AsyncClient]:
    settings = make_settings(UNREACHABLE_DB)
    container = create_container(settings)
    app = create_app(settings, container)
    # Starlette после ответа 500 пробрасывает исключение дальше (для логов сервера).
    transport = ASGITransport(app=app, raise_app_exceptions=False)
    try:
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            yield client
    finally:
        await container.close()


async def test_unexpected_error_uses_unified_format_without_details(
    client_without_db: AsyncClient,
) -> None:
    response = await client_without_db.post(
        "/api/v1/payments",
        json=payment_body(),
        headers=AUTH | {"Idempotency-Key": new_idempotency_key()},
    )

    assert response.status_code == 500
    assert response.json() == {
        "error": {"code": "internal_error", "message": "Internal server error", "details": None}
    }
    assert "127.0.0.1" not in response.text  # детали инфраструктуры наружу не утекают


async def test_get_with_unavailable_db_is_500(client_without_db: AsyncClient) -> None:
    response = await client_without_db.get(f"/api/v1/payments/{uuid.uuid4()}", headers=AUTH)

    assert response.status_code == 500
    assert response.json()["error"]["code"] == "internal_error"
