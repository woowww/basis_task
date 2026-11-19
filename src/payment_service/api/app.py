from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from dishka import AsyncContainer
from dishka.integrations.fastapi import setup_dishka
from fastapi import FastAPI

from payment_service.config import Settings, get_settings
from payment_service.ioc import create_container
from payment_service.logging_config import configure_logging


def create_app(
    settings: Settings | None = None, container: AsyncContainer | None = None
) -> FastAPI:
    settings = settings or get_settings()
    configure_logging(settings.log_level)
    container = container or create_container(settings)

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        yield
        await container.close()

    app = FastAPI(title="Payment Processing Service", version="0.1.0", lifespan=lifespan)
    setup_dishka(container, app)
    return app
