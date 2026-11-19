"""Граф зависимостей (dishka).

Единая точка сборки для всех ролей процесса. `Settings` передаются в контейнер через
контекст — так тесты подставляют свою конфигурацию без переменных окружения.
"""

from dishka import AsyncContainer, Provider, Scope, from_context, make_async_container

from payment_service.config import Settings


class ConfigProvider(Provider):
    settings = from_context(provides=Settings, scope=Scope.APP)


def create_container(settings: Settings, *extra: Provider) -> AsyncContainer:
    """Собрать контейнер.

    `extra` — дополнительные или подменяющие провайдеры (например, фейки в тестах).
    """
    return make_async_container(ConfigProvider(), *extra, context={Settings: settings})
