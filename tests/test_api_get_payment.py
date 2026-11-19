import uuid
from datetime import UTC, datetime

from httpx import AsyncClient
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncEngine

from payment_service.domain.payment import PaymentStatus
from payment_service.infra.db.models import PaymentModel
from tests.helpers import AUTH, new_idempotency_key, payment_body

URL = "/api/v1/payments"


async def create(client: AsyncClient, key: str) -> dict[str, str]:
    response = await client.post(URL, json=payment_body(), headers=AUTH | {"Idempotency-Key": key})
    assert response.status_code == 202
    data: dict[str, str] = response.json()
    return data


async def test_returns_full_payment_details(api_client: AsyncClient) -> None:
    key = new_idempotency_key()
    created = await create(api_client, key)

    response = await api_client.get(f"{URL}/{created['payment_id']}", headers=AUTH)

    assert response.status_code == 200
    assert response.json() == {
        "payment_id": created["payment_id"],
        "amount": "1500.00",
        "currency": "RUB",
        "description": "Заказ #1001",
        "metadata": {"order_id": 1001, "tags": ["a", "b"]},
        "status": "pending",
        "idempotency_key": key,
        "webhook_url": "https://example.com/hooks/payments",
        "created_at": created["created_at"],
        "processed_at": None,
    }


async def test_reflects_processing_result(api_client: AsyncClient, db: AsyncEngine) -> None:
    created = await create(api_client, new_idempotency_key())
    processed_at = datetime(2026, 11, 19, 12, 0, 3, tzinfo=UTC)
    async with db.begin() as conn:
        await conn.execute(
            update(PaymentModel)
            .where(PaymentModel.id == uuid.UUID(created["payment_id"]))
            .values(status=PaymentStatus.SUCCEEDED, processed_at=processed_at)
        )

    data = (await api_client.get(f"{URL}/{created['payment_id']}", headers=AUTH)).json()

    assert data["status"] == "succeeded"
    assert datetime.fromisoformat(data["processed_at"]) == processed_at


async def test_unknown_payment_is_404(api_client: AsyncClient) -> None:
    payment_id = uuid.uuid4()

    response = await api_client.get(f"{URL}/{payment_id}", headers=AUTH)

    assert response.status_code == 404
    assert response.json() == {
        "error": {
            "code": "payment_not_found",
            "message": f"Payment {payment_id} not found",
            "details": None,
        }
    }


async def test_malformed_payment_id_is_422(api_client: AsyncClient) -> None:
    response = await api_client.get(f"{URL}/not-a-uuid", headers=AUTH)

    assert response.status_code == 422
    assert response.json()["error"]["details"][0]["loc"] == ["path", "payment_id"]


async def test_requires_api_key(api_client: AsyncClient) -> None:
    created = await create(api_client, new_idempotency_key())

    response = await api_client.get(f"{URL}/{created['payment_id']}")

    assert response.status_code == 401
