import asyncio
import logging

from payment_service.config import Settings, get_settings
from payment_service.ioc import create_container
from payment_service.logging_config import configure_logging

logger = logging.getLogger(__name__)


async def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    container = create_container(settings)
    try:
        await container.get(Settings)
        logger.info("outbox-relay started")
        # Заглушка до T7: цикл публикации outbox появится там.
        await asyncio.Event().wait()
    finally:
        await container.close()


if __name__ == "__main__":
    asyncio.run(main())
