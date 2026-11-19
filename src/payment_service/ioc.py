"""Граф зависимостей (dishka).

Единая точка сборки для всех ролей процесса. `Settings` передаются в контейнер через
контекст — так тесты подставляют свою конфигурацию без переменных окружения.
"""

from dishka import AsyncContainer, Provider, Scope, from_context, make_async_container

from payment_service.application.provider import UseCaseProvider
from payment_service.config import Settings
from payment_service.infra.db.provider import DbProvider


class ConfigProvider(Provider):
    settings = from_context(provides=Settings, scope=Scope.APP)


def create_container(settings: Settings, *extra: Provider) -> AsyncContainer:
    """Собрать контейнер.

    `extra` — дополнительные или подменяющие провайдеры (например, фейки в тестах).
    """
    return make_async_container(
        ConfigProvider(), DbProvider(), UseCaseProvider(), *extra, context={Settings: settings}
    )
