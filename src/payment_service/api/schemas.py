import uuid
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field, HttpUrl

from payment_service.domain.payment import Currency, Payment, PaymentStatus

Amount = Annotated[
    Decimal,
    Field(
        gt=0,
        max_digits=18,
        decimal_places=2,
        description="Сумма: положительное число, не более 2 знаков после запятой",
        examples=["1500.00"],
    ),
]


class CreatePaymentRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount: Amount
    currency: Currency
    description: str = Field(min_length=1, max_length=1000)
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: HttpUrl


class PaymentResponse(BaseModel):
    payment_id: uuid.UUID
    amount: Decimal = Field(examples=["1500.00"])
    currency: Currency
    description: str
    metadata: dict[str, Any]
    status: PaymentStatus
    idempotency_key: str
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None

    @classmethod
    def from_entity(cls, payment: Payment) -> "PaymentResponse":
        return cls(
            payment_id=payment.id,
            amount=payment.amount,
            currency=payment.currency,
            description=payment.description,
            metadata=payment.metadata,
            status=payment.status,
            idempotency_key=payment.idempotency_key,
            webhook_url=payment.webhook_url,
            created_at=payment.created_at,
            processed_at=payment.processed_at,
        )


class CreatePaymentResponse(BaseModel):
    payment_id: uuid.UUID
    status: PaymentStatus
    created_at: datetime
