from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from payment_service.config import Settings
from payment_service.ioc import create_container


def make_settings(database_url: str = "postgresql+asyncpg://u:p@localhost/db") -> Settings:
    return Settings(
        api_key="secret",
        database_url=database_url,
        rabbitmq_url="amqp://guest:guest@localhost/",
    )


async def test_container_provides_given_settings() -> None:
    settings = make_settings()
    container = create_container(settings)
    try:
        assert await container.get(Settings) is settings
    finally:
        await container.close()


async def test_container_provides_working_session(migrated_db_url: str) -> None:
    container = create_container(make_settings(migrated_db_url))
    try:
        async with container() as request:
            session = await request.get(AsyncSession)
            assert (await session.execute(text("SELECT count(*) FROM payments"))).scalar_one() == 0
    finally:
        await container.close()
