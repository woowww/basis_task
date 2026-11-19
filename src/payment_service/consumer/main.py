from dishka import AsyncContainer
from dishka_faststream import setup_dishka
from faststream import FastStream
from faststream.rabbit import RabbitBroker

from payment_service.config import Settings, get_settings
from payment_service.ioc import create_container
from payment_service.logging_config import configure_logging


def create_app(
    settings: Settings | None = None, container: AsyncContainer | None = None
) -> FastStream:
    settings = settings or get_settings()
    configure_logging(settings.log_level)
    container = container or create_container(settings)

    broker = RabbitBroker(settings.rabbitmq_url)
    app = FastStream(broker)
    setup_dishka(container, app, finalize_container=True, auto_inject=True)
    return app
