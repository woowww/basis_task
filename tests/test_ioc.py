from payment_service.config import Settings
from payment_service.ioc import create_container


def make_settings() -> Settings:
    return Settings(
        api_key="secret",
        database_url="postgresql+asyncpg://u:p@localhost/db",
        rabbitmq_url="amqp://guest:guest@localhost/",
    )


async def test_container_provides_given_settings() -> None:
    settings = make_settings()
    container = create_container(settings)
    try:
        assert await container.get(Settings) is settings
    finally:
        await container.close()
