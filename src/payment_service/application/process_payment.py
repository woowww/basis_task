import logging
import uuid
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from payment_service.application.errors import PaymentNotFoundError
from payment_service.application.ports import (
    PaymentGateway,
    PaymentRepository,
    UnitOfWork,
    WebhookSender,
)
from payment_service.domain.payment import Payment

logger = logging.getLogger(__name__)


class ProcessingResult(StrEnum):
    PROCESSED = "processed"  # шлюз + статус + webhook
    WEBHOOK_ONLY = "webhook_only"  # статус уже был, дослали webhook
    ALREADY_DONE = "already_done"  # дубль полностью обработанного платежа


def webhook_payload(payment: Payment) -> dict[str, Any]:
    return {
        "payment_id": str(payment.id),
        "status": payment.status.value,
        "amount": str(payment.amount),
        "currency": payment.currency.value,
        "processed_at": payment.processed_at.isoformat() if payment.processed_at else None,
    }


class ProcessPayment:
    """Обработать событие `payments.new` идемпотентно.

    1. Платёж `pending` → шлюз → статус и `processed_at` (строка заблокирована на время вызова
       шлюза: параллельный дубль ждёт и шлюз повторно не вызывает).
    2. Статус финальный, webhook не доставлен → доставить webhook, записать `webhook_delivered_at`.
    3. Всё уже сделано → ничего.

    Технические ошибки (БД, `WebhookDeliveryError`) пробрасываются — повторы решает адаптер.
    """

    def __init__(
        self,
        payments: PaymentRepository,
        uow: UnitOfWork,
        gateway: PaymentGateway,
        webhooks: WebhookSender,
    ) -> None:
        self._payments = payments
        self._uow = uow
        self._gateway = gateway
        self._webhooks = webhooks

    async def __call__(self, payment_id: uuid.UUID) -> ProcessingResult:
        payment = await self._payments.get_for_update(payment_id)
        if payment is None:
            await self._uow.rollback()
            raise PaymentNotFoundError(payment_id)

        processed_now = False
        if payment.status.is_final:
            await self._uow.rollback()  # снять блокировку: менять нечего
        else:
            outcome = await self._gateway.process(payment)
            payment.complete(outcome, at=datetime.now(UTC))
            await self._payments.save(payment)
            await self._uow.commit()
            processed_now = True
            logger.info("payment processed payment_id=%s status=%s", payment.id, payment.status)

        if not payment.needs_webhook:
            return ProcessingResult.ALREADY_DONE

        await self._webhooks.send(payment.webhook_url, webhook_payload(payment))
        payment.mark_webhook_delivered(at=datetime.now(UTC))
        await self._payments.save(payment)
        await self._uow.commit()
        logger.info("webhook delivered payment_id=%s", payment.id)
        return ProcessingResult.PROCESSED if processed_now else ProcessingResult.WEBHOOK_ONLY
