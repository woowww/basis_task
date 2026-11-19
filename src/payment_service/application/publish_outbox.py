import logging
import uuid

from payment_service.application.ports import MessagePublisher, OutboxRepository, UnitOfWork

logger = logging.getLogger(__name__)


class PublishOutboxBatch:
    """Опубликовать пачку событий outbox (at-least-once).

    Событие помечается опубликованным только после подтверждения брокера. Сбой между
    публикацией и коммитом даст повторную публикацию — consumer к этому готов.
    """

    def __init__(
        self, outbox: OutboxRepository, publisher: MessagePublisher, uow: UnitOfWork
    ) -> None:
        self._outbox = outbox
        self._publisher = publisher
        self._uow = uow

    async def __call__(self, limit: int) -> int:
        """Вернуть число опубликованных событий. Ошибка публикации пробрасывается после
        фиксации уже опубликованной части пачки."""
        events = await self._outbox.lock_unpublished(limit)
        published: list[uuid.UUID] = []
        try:
            for event in events:
                await self._publisher.publish_payment_event(event)
                published.append(event.id)
                logger.info(
                    "outbox event published payment_id=%s event_id=%s",
                    event.aggregate_id,
                    event.id,
                )
        finally:
            if published:
                await self._outbox.mark_published(published)
            await self._uow.commit()  # фиксирует отметки и снимает блокировки строк
        return len(published)
