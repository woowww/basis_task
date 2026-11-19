class ApplicationError(Exception):
    """Базовое прикладное исключение: адаптеры превращают его в ответ своего протокола."""


class IdempotencyConflictError(ApplicationError):
    def __init__(self, idempotency_key: str) -> None:
        super().__init__(
            f"Idempotency-Key {idempotency_key!r} was already used with a different request"
        )
        self.idempotency_key = idempotency_key


class IdempotencyKeyTakenError(ApplicationError):
    """Порт сообщает: коммит не прошёл, ключ уже занят параллельным запросом."""
