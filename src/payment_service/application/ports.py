import uuid
from typing import Any, Protocol

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


class MessagePublisher(Protocol):
    async def publish_payment_event(self, event: OutboxEvent) -> None:
        """Опубликовать событие (попытка 1). Возвращается после подтверждения брокера."""

    async def publish_retry(
        self, payload: dict[str, Any], *, message_id: str, next_attempt: int
    ) -> None:
        """Отложить сообщение до попытки `next_attempt` (экспоненциальная задержка)."""

    async def publish_dead_letter(
        self, payload: dict[str, Any], *, message_id: str, attempts: int, reason: str
    ) -> None:
        """Отправить окончательно не обработанное сообщение в DLQ."""
