import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from payment_service.application.errors import IdempotencyConflictError, IdempotencyKeyTakenError
from payment_service.application.ports import OutboxRepository, PaymentRepository, UnitOfWork
from payment_service.domain.payment import MONEY_QUANTUM, Currency, OutboxEvent, Payment


@dataclass(frozen=True, kw_only=True)
class CreatePaymentCommand:
    idempotency_key: str
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    webhook_url: str

    def fingerprint(self) -> str:
        """Хэш канонизированного тела запроса — для сравнения повторов по Idempotency-Key.

        `100.5` и `100.50` — одна и та же сумма, порядок ключей metadata не важен.
        """
        canonical = json.dumps(
            {
                "amount": str(self.amount.quantize(MONEY_QUANTUM)),
                "currency": self.currency.value,
                "description": self.description,
                "metadata": self.metadata,
                "webhook_url": self.webhook_url,
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )
        return hashlib.sha256(canonical.encode()).hexdigest()


class CreatePayment:
    """Создать платёж и событие `payments.new` атомарно; повтор по ключу — идемпотентен."""

    def __init__(
        self, payments: PaymentRepository, outbox: OutboxRepository, uow: UnitOfWork
    ) -> None:
        self._payments = payments
        self._outbox = outbox
        self._uow = uow

    async def __call__(self, command: CreatePaymentCommand) -> Payment:
        request_hash = command.fingerprint()

        existing = await self._payments.get_by_idempotency_key(command.idempotency_key)
        if existing is not None:
            return self._replay(existing, command.idempotency_key, request_hash)

        payment = Payment.new(
            amount=command.amount,
            currency=command.currency,
            description=command.description,
            metadata=command.metadata,
            idempotency_key=command.idempotency_key,
            request_hash=request_hash,
            webhook_url=command.webhook_url,
        )
        self._payments.add(payment)
        self._outbox.add(OutboxEvent.payment_created(payment))
        try:
            await self._uow.commit()
        except IdempotencyKeyTakenError:
            # Параллельный запрос с тем же ключом закоммитился первым.
            await self._uow.rollback()
            winner = await self._payments.get_by_idempotency_key(command.idempotency_key)
            if winner is None:  # pragma: no cover — ключ не удаляется, недостижимо
                raise
            return self._replay(winner, command.idempotency_key, request_hash)
        return payment

    @staticmethod
    def _replay(existing: Payment, idempotency_key: str, request_hash: str) -> Payment:
        if existing.request_hash != request_hash:
            raise IdempotencyConflictError(idempotency_key)
        return existing
