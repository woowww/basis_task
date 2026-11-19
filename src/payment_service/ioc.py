"""Граф зависимостей (dishka).

Единая точка сборки для всех ролей процесса. `Settings` передаются в контейнер через
контекст — так тесты подставляют свою конфигурацию без переменных окружения.
"""

from typing import Any

from dishka import AsyncContainer, Provider, Scope, from_context, make_async_container
from faststream.rabbit import RabbitBroker

from payment_service.application.provider import UseCaseProvider
from payment_service.config import Settings
from payment_service.infra.broker.provider import BrokerProvider
from payment_service.infra.db.provider import DbProvider
from payment_service.infra.external_provider import ExternalServicesProvider


class ConfigProvider(Provider):
    settings = from_context(provides=Settings, scope=Scope.APP)


def create_container(
    settings: Settings, *extra: Provider, broker: RabbitBroker | None = None
) -> AsyncContainer:
    """Собрать контейнер.

    `extra` — дополнительные или подменяющие провайдеры (например, фейки в тестах).
    `broker` — брокер процесса; нужен ролям, которые публикуют сообщения (relay, consumer).
    """
    context: dict[Any, Any] = {Settings: settings}
    if broker is not None:
        context[RabbitBroker] = broker
    return make_async_container(
        ConfigProvider(),
        DbProvider(),
        BrokerProvider(),
        ExternalServicesProvider(),
        UseCaseProvider(),
        *extra,
        context=context,
    )
