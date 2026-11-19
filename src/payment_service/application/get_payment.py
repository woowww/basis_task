import uuid

from payment_service.application.errors import PaymentNotFoundError
from payment_service.application.ports import PaymentRepository
from payment_service.domain.payment import Payment


class GetPayment:
    def __init__(self, payments: PaymentRepository) -> None:
        self._payments = payments

    async def __call__(self, payment_id: uuid.UUID) -> Payment:
        payment = await self._payments.get(payment_id)
        if payment is None:
            raise PaymentNotFoundError(payment_id)
        return payment
