import asyncio
from decimal import Decimal
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from payment_service.infra.db.models import OutboxModel, PaymentModel
from tests.helpers import AUTH, new_idempotency_key, payment_body

URL = "/api/v1/payments"


async def post(
    client: AsyncClient, payload: dict[str, Any], key: str | None, **headers: str
) -> Any:
    hdrs = AUTH | headers | ({"Idempotency-Key": key} if key is not None else {})
    return await client.post(URL, json=payload, headers=hdrs)


async def rows_for_key(db: AsyncEngine, key: str) -> tuple[list[PaymentModel], list[OutboxModel]]:
    async with AsyncSession(db) as session:
        payments = list(
            await session.scalars(select(PaymentModel).where(PaymentModel.idempotency_key == key))
        )
        events = list(
            await session.scalars(
                select(OutboxModel).where(OutboxModel.aggregate_id.in_([p.id for p in payments]))
            )
        )
    return payments, events


async def test_creates_pending_payment_and_outbox_event(
    api_client: AsyncClient, db: AsyncEngine
) -> None:
    key = new_idempotency_key()

    response = await post(api_client, payment_body(), key)

    assert response.status_code == 202
    data = response.json()
    assert set(data) == {"payment_id", "status", "created_at"}
    assert data["status"] == "pending"

    payments, events = await rows_for_key(db, key)
    assert len(payments) == 1
    payment = payments[0]
    assert str(payment.id) == data["payment_id"]
    assert payment.amount == Decimal("1500.00")
    assert payment.currency == "RUB"
    assert payment.description == "Заказ #1001"
    assert payment.metadata_ == {"order_id": 1001, "tags": ["a", "b"]}
    assert payment.webhook_url == "https://example.com/hooks/payments"
    assert payment.status == "pending"
    assert payment.processed_at is None

    assert len(events) == 1
    event = events[0]
    assert event.event_type == "payments.new"
    assert event.payload == {"payment_id": data["payment_id"]}
    assert event.published_at is None


async def test_metadata_is_optional(api_client: AsyncClient, db: AsyncEngine) -> None:
    key = new_idempotency_key()
    payload = payment_body()
    del payload["metadata"]

    response = await post(api_client, payload, key)

    assert response.status_code == 202
    payments, _ = await rows_for_key(db, key)
    assert payments[0].metadata_ == {}


@pytest.mark.parametrize(
    ("overrides", "field"),
    [
        pytest.param({"amount": "0"}, "amount", id="amount-zero"),
        pytest.param({"amount": "-10.00"}, "amount", id="amount-negative"),
        pytest.param({"amount": "1.001"}, "amount", id="amount-3-decimals"),
        pytest.param({"amount": "abc"}, "amount", id="amount-not-number"),
        pytest.param({"amount": "1" * 17 + ".00"}, "amount", id="amount-too-large"),
        pytest.param({"currency": "GBP"}, "currency", id="currency-unsupported"),
        pytest.param({"webhook_url": "not-a-url"}, "webhook_url", id="webhook-invalid"),
        pytest.param({"webhook_url": "ftp://example.com"}, "webhook_url", id="webhook-scheme"),
        pytest.param({"description": ""}, "description", id="description-empty"),
        pytest.param({"metadata": [1, 2]}, "metadata", id="metadata-not-object"),
        pytest.param({"unexpected": 1}, "unexpected", id="extra-field"),
    ],
)
async def test_invalid_body_is_rejected(
    api_client: AsyncClient, db: AsyncEngine, overrides: dict[str, Any], field: str
) -> None:
    key = new_idempotency_key()

    response = await post(api_client, payment_body(**overrides), key)

    assert response.status_code == 422
    error = response.json()["error"]
    assert error["code"] == "validation_error"
    assert ["body", field] in [d["loc"][:2] for d in error["details"]]
    assert await rows_for_key(db, key) == ([], [])


async def test_idempotency_key_header_is_required(api_client: AsyncClient) -> None:
    response = await post(api_client, payment_body(), key=None)

    assert response.status_code == 422
    assert response.json()["error"]["details"][0]["loc"] == ["header", "Idempotency-Key"]


async def test_requires_api_key(api_client: AsyncClient, db: AsyncEngine) -> None:
    key = new_idempotency_key()

    response = await api_client.post(URL, json=payment_body(), headers={"Idempotency-Key": key})

    assert response.status_code == 401
    assert await rows_for_key(db, key) == ([], [])


async def test_repeat_with_same_key_and_body_returns_same_payment(
    api_client: AsyncClient, db: AsyncEngine
) -> None:
    key = new_idempotency_key()

    first = await post(api_client, payment_body(), key)
    second = await post(api_client, payment_body(), key)

    assert first.status_code == second.status_code == 202
    assert first.json() == second.json()
    payments, events = await rows_for_key(db, key)
    assert (len(payments), len(events)) == (1, 1)


async def test_equivalent_body_counts_as_same_request(
    api_client: AsyncClient, db: AsyncEngine
) -> None:
    key = new_idempotency_key()

    first = await post(api_client, payment_body(amount="100.5", metadata={"a": 1, "b": 2}), key)
    second = await post(api_client, payment_body(amount="100.50", metadata={"b": 2, "a": 1}), key)

    assert second.status_code == 202
    assert second.json()["payment_id"] == first.json()["payment_id"]


async def test_same_key_with_different_body_conflicts(
    api_client: AsyncClient, db: AsyncEngine
) -> None:
    key = new_idempotency_key()
    await post(api_client, payment_body(), key)

    response = await post(api_client, payment_body(amount="99.99"), key)

    assert response.status_code == 409
    error = response.json()["error"]
    assert error["code"] == "idempotency_conflict"
    assert key in error["message"]
    payments, events = await rows_for_key(db, key)
    assert (len(payments), len(events)) == (1, 1)
    assert payments[0].amount == Decimal("1500.00")


async def test_concurrent_requests_with_same_key_create_one_payment(
    api_client: AsyncClient, db: AsyncEngine
) -> None:
    key = new_idempotency_key()

    responses = await asyncio.gather(*(post(api_client, payment_body(), key) for _ in range(10)))

    assert {r.status_code for r in responses} == {202}
    assert len({r.json()["payment_id"] for r in responses}) == 1
    payments, events = await rows_for_key(db, key)
    assert (len(payments), len(events)) == (1, 1)
