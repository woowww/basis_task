import uuid


class ApplicationError(Exception):
    """Базовое прикладное исключение: адаптеры превращают его в ответ своего протокола."""


class IdempotencyConflictError(ApplicationError):
    def __init__(self, idempotency_key: str) -> None:
        super().__init__(
            f"Idempotency-Key {idempotency_key!r} was already used with a different request"
        )
        self.idempotency_key = idempotency_key


class PaymentNotFoundError(ApplicationError):
    def __init__(self, payment_id: uuid.UUID) -> None:
        super().__init__(f"Payment {payment_id} not found")
        self.payment_id = payment_id


class IdempotencyKeyTakenError(ApplicationError):
    """Порт сообщает: коммит не прошёл, ключ уже занят параллельным запросом."""
