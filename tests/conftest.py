import os
import uuid
from collections.abc import AsyncIterator

import pytest
from sqlalchemy import make_url, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import create_async_engine

DEFAULT_TEST_DATABASE_URL = "postgresql+asyncpg://payments:payments@localhost:5432/payments"

# Один идентификатор на запуск pytest: у всех xdist-воркеров он общий (передаётся через env
# контроллера), поэтому базы одного прогона легко опознать, а параллельные прогоны не пересекаются.
RUN_ID = os.environ.setdefault("PAYMENTS_TEST_RUN_ID", uuid.uuid4().hex[:8])


@pytest.fixture(scope="session")
async def postgres_url(worker_id: str) -> AsyncIterator[str]:
    """DSN свежей пустой базы на тестовую сессию (своя база на каждый xdist-воркер).

    Сервер берётся из `TEST_DATABASE_URL` (по умолчанию — postgres из docker-compose).
    База создаётся при старте сессии и удаляется в конце.
    """
    server_url = make_url(os.environ.get("TEST_DATABASE_URL", DEFAULT_TEST_DATABASE_URL))
    db_name = f"test_{RUN_ID}_{worker_id}"

    admin = create_async_engine(server_url, isolation_level="AUTOCOMMIT")
    try:
        async with admin.connect() as conn:
            await conn.execute(text(f'CREATE DATABASE "{db_name}"'))
    except OperationalError as exc:
        await admin.dispose()
        pytest.exit(
            f"PostgreSQL недоступен по {server_url.render_as_string(hide_password=True)}: {exc}\n"
            "Запустите его: docker compose up -d postgres",
            returncode=1,
        )

    try:
        yield server_url.set(database=db_name).render_as_string(hide_password=False)
    finally:
        async with admin.connect() as conn:
            await conn.execute(text(f'DROP DATABASE IF EXISTS "{db_name}" WITH (FORCE)'))
        await admin.dispose()
