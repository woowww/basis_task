import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

MONEY_QUANTUM = Decimal("0.01")


class Currency(StrEnum):
    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class PaymentStatus(StrEnum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"

    @property
    def is_final(self) -> bool:
        return self is not PaymentStatus.PENDING


@dataclass(kw_only=True)
class Payment:
    id: uuid.UUID
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    status: PaymentStatus
    idempotency_key: str
    request_hash: str
    webhook_url: str
    created_at: datetime
    updated_at: datetime
    processed_at: datetime | None = None
    webhook_delivered_at: datetime | None = None

    @classmethod
    def new(
        cls,
        *,
        amount: Decimal,
        currency: Currency,
        description: str,
        metadata: dict[str, Any],
        idempotency_key: str,
        request_hash: str,
        webhook_url: str,
    ) -> "Payment":
        if amount <= 0:
            msg = "Payment amount must be positive"
            raise ValueError(msg)
        now = datetime.now(UTC)
        return cls(
            id=uuid.uuid4(),
            amount=amount.quantize(MONEY_QUANTUM),
            currency=currency,
            description=description,
            metadata=metadata,
            status=PaymentStatus.PENDING,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            webhook_url=webhook_url,
            created_at=now,
            updated_at=now,
        )

    def complete(self, outcome: PaymentStatus, at: datetime) -> None:
        """Зафиксировать итог обработки шлюзом (pending → succeeded|failed)."""
        if self.status.is_final:
            msg = f"Payment {self.id} is already {self.status}"
            raise ValueError(msg)
        if not outcome.is_final:
            msg = f"Processing outcome must be final, got {outcome}"
            raise ValueError(msg)
        self.status = outcome
        self.processed_at = at
        self.updated_at = at

    def mark_webhook_delivered(self, at: datetime) -> None:
        self.webhook_delivered_at = at
        self.updated_at = at

    @property
    def needs_webhook(self) -> bool:
        return self.status.is_final and self.webhook_delivered_at is None


PAYMENT_CREATED_EVENT = "payments.new"


@dataclass(kw_only=True, frozen=True)
class OutboxEvent:
    """Событие для гарантированной публикации через outbox."""

    aggregate_id: uuid.UUID
    event_type: str
    payload: dict[str, Any]
    id: uuid.UUID = field(default_factory=uuid.uuid4)

    @classmethod
    def payment_created(cls, payment: Payment) -> "OutboxEvent":
        return cls(
            aggregate_id=payment.id,
            event_type=PAYMENT_CREATED_EVENT,
            payload={"payment_id": str(payment.id)},
        )
