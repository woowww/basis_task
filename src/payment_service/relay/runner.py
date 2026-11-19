import asyncio
import contextlib
import logging

from dishka import AsyncContainer

from payment_service.application.publish_outbox import PublishOutboxBatch
from payment_service.config import Settings

logger = logging.getLogger(__name__)


async def run_relay(container: AsyncContainer, settings: Settings, stop: asyncio.Event) -> None:
    """Цикл relay: публиковать пачки до установки `stop`.

    Полная пачка — сразу следующая (разбор бэклога), иначе пауза на интервал опроса.
    Ошибка итерации логируется и не останавливает цикл.
    """
    logger.info("outbox relay started")
    while not stop.is_set():
        published = 0
        try:
            async with container() as request:
                publish = await request.get(PublishOutboxBatch)
                published = await publish(settings.outbox_batch_size)
        except Exception:
            logger.exception("outbox relay iteration failed")

        if published < settings.outbox_batch_size:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), settings.outbox_poll_interval_seconds)
    logger.info("outbox relay stopped")
