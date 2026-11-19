import uuid
from collections.abc import Sequence

from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from payment_service.application.errors import IdempotencyKeyTakenError
from payment_service.domain.payment import OutboxEvent, Payment
from payment_service.infra.db.models import OutboxModel, PaymentModel

IDEMPOTENCY_KEY_CONSTRAINT = "uq_payments_idempotency_key"


def _to_entity(row: PaymentModel) -> Payment:
    return Payment(
        id=row.id,
        amount=row.amount,
        currency=row.currency,
        description=row.description,
        metadata=row.metadata_,
        status=row.status,
        idempotency_key=row.idempotency_key,
        request_hash=row.request_hash,
        webhook_url=row.webhook_url,
        created_at=row.created_at,
        updated_at=row.updated_at,
        processed_at=row.processed_at,
        webhook_delivered_at=row.webhook_delivered_at,
    )


def _to_row(payment: Payment) -> PaymentModel:
    return PaymentModel(
        id=payment.id,
        amount=payment.amount,
        currency=payment.currency,
        description=payment.description,
        metadata_=payment.metadata,
        status=payment.status,
        idempotency_key=payment.idempotency_key,
        request_hash=payment.request_hash,
        webhook_url=payment.webhook_url,
        created_at=payment.created_at,
        updated_at=payment.updated_at,
        processed_at=payment.processed_at,
        webhook_delivered_at=payment.webhook_delivered_at,
    )


class SqlPaymentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    def add(self, payment: Payment) -> None:
        self._session.add(_to_row(payment))

    async def get(self, payment_id: uuid.UUID) -> Payment | None:
        row = await self._session.get(PaymentModel, payment_id)
        return _to_entity(row) if row is not None else None

    async def get_by_idempotency_key(self, idempotency_key: str) -> Payment | None:
        row = await self._session.scalar(
            select(PaymentModel).where(PaymentModel.idempotency_key == idempotency_key)
        )
        return _to_entity(row) if row is not None else None


class SqlOutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def lock_unpublished(self, limit: int) -> list[OutboxEvent]:
        rows = await self._session.scalars(
            select(OutboxModel)
            .where(OutboxModel.published_at.is_(None))
            .order_by(OutboxModel.created_at, OutboxModel.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return [
            OutboxEvent(
                id=row.id,
                aggregate_id=row.aggregate_id,
                event_type=row.event_type,
                payload=row.payload,
            )
            for row in rows
        ]

    async def mark_published(self, event_ids: Sequence[uuid.UUID]) -> None:
        await self._session.execute(
            update(OutboxModel).where(OutboxModel.id.in_(event_ids)).values(published_at=func.now())
        )

    def add(self, event: OutboxEvent) -> None:
        self._session.add(
            OutboxModel(
                id=event.id,
                aggregate_id=event.aggregate_id,
                event_type=event.event_type,
                payload=event.payload,
            )
        )


class SqlUnitOfWork:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def commit(self) -> None:
        try:
            await self._session.commit()
        except IntegrityError as exc:
            if IDEMPOTENCY_KEY_CONSTRAINT in str(exc.orig):
                raise IdempotencyKeyTakenError from exc
            raise

    async def rollback(self) -> None:
        await self._session.rollback()
