from dishka import AsyncContainer
from dishka_faststream import setup_dishka
from faststream import FastStream

from payment_service.config import Settings, get_settings
from payment_service.infra.broker.provider import create_broker
from payment_service.infra.broker.topology import Topology
from payment_service.ioc import create_container
from payment_service.logging_config import configure_logging


def create_app(
    settings: Settings | None = None, container: AsyncContainer | None = None
) -> FastStream:
    settings = settings or get_settings()
    configure_logging(settings.log_level)
    broker = create_broker(settings)
    container = container or create_container(settings, broker=broker)

    app = FastStream(broker)

    @app.after_startup
    async def declare_topology() -> None:
        await (await container.get(Topology)).declare(broker)

    setup_dishka(container, app, finalize_container=True, auto_inject=True)
    return app
