import uuid
from typing import Annotated

from dishka import FromDishka
from dishka.integrations.fastapi import inject
from fastapi import APIRouter, Header, status

from payment_service.api.errors import ErrorResponse
from payment_service.api.schemas import (
    CreatePaymentRequest,
    CreatePaymentResponse,
    PaymentResponse,
)
from payment_service.application.create_payment import CreatePayment, CreatePaymentCommand
from payment_service.application.get_payment import GetPayment

router = APIRouter(prefix="/payments", tags=["payments"])

IdempotencyKey = Annotated[
    str,
    Header(
        alias="Idempotency-Key",
        min_length=1,
        max_length=255,
        description="Уникальный ключ запроса: повтор с тем же ключом не создаёт новый платёж",
    ),
]


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        409: {"model": ErrorResponse, "description": "Idempotency-Key reused with another body"},
        422: {"model": ErrorResponse, "description": "Validation error"},
    },
)
@inject
async def create_payment(
    body: CreatePaymentRequest,
    idempotency_key: IdempotencyKey,
    create: FromDishka[CreatePayment],
) -> CreatePaymentResponse:
    payment = await create(
        CreatePaymentCommand(
            idempotency_key=idempotency_key,
            amount=body.amount,
            currency=body.currency,
            description=body.description,
            metadata=body.metadata,
            webhook_url=str(body.webhook_url),
        )
    )
    return CreatePaymentResponse(
        payment_id=payment.id, status=payment.status, created_at=payment.created_at
    )


@router.get(
    "/{payment_id}",
    responses={
        404: {"model": ErrorResponse, "description": "Payment not found"},
        422: {"model": ErrorResponse, "description": "Invalid payment_id"},
    },
)
@inject
async def get_payment(payment_id: uuid.UUID, get: FromDishka[GetPayment]) -> PaymentResponse:
    return PaymentResponse.from_entity(await get(payment_id))
