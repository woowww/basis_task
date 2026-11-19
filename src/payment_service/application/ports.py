import uuid
from typing import Protocol

from payment_service.domain.payment import OutboxEvent, Payment


class PaymentRepository(Protocol):
    def add(self, payment: Payment) -> None: ...

    async def get(self, payment_id: uuid.UUID) -> Payment | None: ...

    async def get_by_idempotency_key(self, idempotency_key: str) -> Payment | None: ...


class OutboxRepository(Protocol):
    def add(self, event: OutboxEvent) -> None: ...


class UnitOfWork(Protocol):
    async def commit(self) -> None:
        """Зафиксировать изменения.

        Raises:
            IdempotencyKeyTakenError: idempotency key уже занят (параллельный запрос успел раньше).
        """

    async def rollback(self) -> None: ...
