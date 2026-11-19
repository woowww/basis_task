import asyncio
import itertools
import os
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path

import httpx
import pytest
from alembic import command
from alembic.config import Config
from httpx import ASGITransport, AsyncClient
from sqlalchemy import URL, make_url, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from payment_service.api.app import create_app
from payment_service.ioc import create_container
from tests.helpers import make_settings

DEFAULT_TEST_RABBITMQ_URL = "amqp://guest:guest@localhost:5672/"
DEFAULT_TEST_RABBITMQ_MANAGEMENT_URL = "http://guest:guest@localhost:15672"
DEFAULT_TEST_DATABASE_URL = "postgresql+asyncpg://payments:payments@localhost:5432/payments"
ALEMBIC_INI = Path(__file__).resolve().parent.parent / "alembic.ini"

# Один идентификатор на запуск pytest: у всех xdist-воркеров он общий (передаётся через env
# контроллера), поэтому базы одного прогона легко опознать, а параллельные прогоны не пересекаются.
RUN_ID = os.environ.setdefault("PAYMENTS_TEST_RUN_ID", uuid.uuid4().hex[:8])

CreateDatabase = Callable[[], Awaitable[str]]


def _server_url() -> URL:
    return make_url(os.environ.get("TEST_DATABASE_URL", DEFAULT_TEST_DATABASE_URL))


def alembic_config(database_url: str) -> Config:
    config = Config(str(ALEMBIC_INI))
    config.attributes["database_url"] = database_url
    config.attributes["configure_logger"] = False
    return config


async def run_alembic(database_url: str, cmd: str, revision: str) -> None:
    """`alembic upgrade|downgrade <revision>`; в потоке — env.py сам запускает event loop."""
    fn = {"upgrade": command.upgrade, "downgrade": command.downgrade}[cmd]
    await asyncio.to_thread(fn, alembic_config(database_url), revision)


@pytest.fixture(scope="session")
async def create_database(worker_id: str) -> AsyncIterator[CreateDatabase]:
    """Фабрика свежих пустых баз на сервере из `TEST_DATABASE_URL`.

    Имена `test_<run>_<worker>_<n>` уникальны для прогона и xdist-воркера.
    Все созданные базы удаляются в конце сессии.
    """
    server_url = _server_url()
    admin = create_async_engine(server_url, isolation_level="AUTOCOMMIT")
    counter = itertools.count()
    created: list[str] = []

    async def create() -> str:
        name = f"test_{RUN_ID}_{worker_id}_{next(counter)}"
        try:
            async with admin.connect() as conn:
                await conn.execute(text(f'CREATE DATABASE "{name}"'))
        except OperationalError as exc:
            pytest.exit(
                f"PostgreSQL недоступен по {server_url.render_as_string(hide_password=True)}: "
                f"{exc}\nЗапустите его: docker compose up -d postgres",
                returncode=1,
            )
        created.append(name)
        return server_url.set(database=name).render_as_string(hide_password=False)

    try:
        yield create
    finally:
        async with admin.connect() as conn:
            for name in created:
                await conn.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
        await admin.dispose()


@pytest.fixture(scope="session")
async def postgres_url(create_database: CreateDatabase) -> str:
    """Пустая база (без схемы) на тестовую сессию."""
    return await create_database()


@pytest.fixture(scope="session")
async def migrated_db_url(create_database: CreateDatabase) -> str:
    """База с применёнными миграциями (`alembic upgrade head`) на тестовую сессию."""
    url = await create_database()
    await run_alembic(url, "upgrade", "head")
    return url


@pytest.fixture
async def api_client(migrated_db_url: str) -> AsyncIterator[AsyncClient]:
    """HTTP-клиент к приложению с реальной БД (миграции применены)."""
    settings = make_settings(migrated_db_url)
    container = create_container(settings)
    app = create_app(settings, container)
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            yield client
    finally:
        await container.close()


@pytest.fixture
async def db(migrated_db_url: str) -> AsyncIterator[AsyncEngine]:
    """Engine для проверки состояния БД в тестах."""
    engine = create_async_engine(migrated_db_url)
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture(scope="session")
async def rabbitmq_url(worker_id: str) -> AsyncIterator[str]:
    """AMQP URL на отдельный vhost RabbitMQ (свой на прогон и xdist-воркер).

    vhost создаётся через management API и удаляется вместе со всеми очередями в конце сессии.
    """
    amqp = httpx.URL(os.environ.get("TEST_RABBITMQ_URL", DEFAULT_TEST_RABBITMQ_URL))
    vhost = f"test_{RUN_ID}_{worker_id}"

    async with rabbitmq_management_client() as api:
        try:
            (await api.put(f"/api/vhosts/{vhost}")).raise_for_status()
        except httpx.TransportError as exc:
            pytest.exit(
                f"RabbitMQ management API недоступен по {api.base_url}: {exc}\n"
                "Запустите его: docker compose up -d rabbitmq",
                returncode=1,
            )
        (
            await api.put(
                f"/api/permissions/{vhost}/{amqp.username}",
                json={"configure": ".*", "write": ".*", "read": ".*"},
            )
        ).raise_for_status()
        try:
            yield str(amqp.copy_with(path=f"/{vhost}"))
        finally:
            await api.delete(f"/api/vhosts/{vhost}")


def rabbitmq_management_client() -> httpx.AsyncClient:
    """Клиент management API RabbitMQ (`/api/...`)."""
    management = httpx.URL(
        os.environ.get("TEST_RABBITMQ_MANAGEMENT_URL", DEFAULT_TEST_RABBITMQ_MANAGEMENT_URL)
    )
    return httpx.AsyncClient(
        base_url=str(management.copy_with(username=None, password=None)),
        auth=(management.username, management.password),
        timeout=10,
    )
