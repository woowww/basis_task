from dishka import Provider, Scope, from_context, provide
from faststream.rabbit import RabbitBroker
from faststream.rabbit.schemas import Channel

from payment_service.application.ports import MessagePublisher
from payment_service.config import Settings
from payment_service.infra.broker.publisher import RabbitMessagePublisher
from payment_service.infra.broker.topology import Topology


def create_broker(settings: Settings) -> RabbitBroker:
    return RabbitBroker(
        settings.rabbitmq_url,
        default_channel=Channel(
            prefetch_count=settings.consumer_prefetch,
            publisher_confirms=True,
            on_return_raises=True,
        ),
    )


class BrokerProvider(Provider):
    scope = Scope.APP

    # Брокер создаёт и запускает владелец процесса (FastStream-приложение или relay).
    broker = from_context(provides=RabbitBroker)

    @provide
    def topology(self, settings: Settings) -> Topology:
        return Topology.from_settings(settings)

    publisher = provide(RabbitMessagePublisher, provides=MessagePublisher)
