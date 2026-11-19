"""Ограничения и значения по умолчанию, которые гарантирует сама БД."""

import uuid
from collections.abc import AsyncIterator
from decimal import Decimal

import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from payment_service.domain.payment import Currency, PaymentStatus
from payment_service.infra.db.models import PaymentModel


@pytest.fixture
async def session(migrated_db_url: str) -> AsyncIterator[AsyncSession]:
    engine = create_async_engine(migrated_db_url)
    try:
        async with async_sessionmaker(engine, expire_on_commit=False)() as session:
            yield session
            await session.rollback()
    finally:
        await engine.dispose()


def make_payment(**overrides: object) -> PaymentModel:
    fields: dict[str, object] = {
        "amount": Decimal("100.50"),
        "currency": Currency.RUB,
        "description": "test",
        "idempotency_key": f"key-{uuid.uuid4()}",
        "request_hash": "0" * 64,
        "webhook_url": "https://example.com/hook",
    }
    return PaymentModel(**(fields | overrides))


async def test_new_payment_defaults(session: AsyncSession) -> None:
    payment = make_payment()
    session.add(payment)
    await session.flush()
    await session.refresh(payment)

    assert payment.status is PaymentStatus.PENDING
    assert payment.metadata_ == {}
    assert payment.created_at is not None
    assert payment.processed_at is None
    assert payment.webhook_delivered_at is None
    assert payment.amount == Decimal("100.50")


async def test_idempotency_key_is_unique(session: AsyncSession) -> None:
    session.add(make_payment(idempotency_key="dup"))
    await session.flush()
    session.add(make_payment(idempotency_key="dup"))

    with pytest.raises(IntegrityError, match="uq_payments_idempotency_key"):
        await session.flush()


@pytest.mark.parametrize("amount", [Decimal(0), Decimal("-1.00")])
async def test_amount_must_be_positive(session: AsyncSession, amount: Decimal) -> None:
    session.add(make_payment(amount=amount))

    with pytest.raises(IntegrityError, match="ck_payments_amount_positive"):
        await session.flush()
