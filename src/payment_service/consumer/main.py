from dataclasses import dataclass

from dishka import AsyncContainer, Provider
from dishka_faststream import setup_dishka
from faststream import FastStream
from faststream.rabbit import RabbitBroker

from payment_service.config import Settings, get_settings
from payment_service.consumer.handler import handle_payment_created
from payment_service.infra.broker.provider import create_broker
from payment_service.infra.broker.topology import Topology
from payment_service.ioc import create_container
from payment_service.logging_config import configure_logging


@dataclass(frozen=True)
class Consumer:
    app: FastStream
    broker: RabbitBroker
    container: AsyncContainer
    topology: Topology


def create_consumer(settings: Settings, *extra: Provider) -> Consumer:
    """Собрать consumer. `extra` — подменяющие провайдеры (фейки в тестах)."""
    broker = create_broker(settings)
    container = create_container(settings, *extra, broker=broker)
    topology = Topology.from_settings(settings)

    broker.subscriber(topology.queue, topology.exchange)(handle_payment_created)

    app = FastStream(broker)

    @app.after_startup
    async def declare_topology() -> None:
        await topology.declare(broker)

    setup_dishka(container, app, finalize_container=True, auto_inject=True)
    return Consumer(app=app, broker=broker, container=container, topology=topology)


def create_app() -> FastStream:
    """Точка входа: `faststream run --factory payment_service.consumer.main:create_app`."""
    settings = get_settings()
    configure_logging(settings.log_level)
    return create_consumer(settings).app
