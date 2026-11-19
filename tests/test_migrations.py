from typing import Any

from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from sqlalchemy import Connection, text
from sqlalchemy.ext.asyncio import create_async_engine

from payment_service.infra.db.base import Base
from tests.conftest import CreateDatabase, run_alembic

SCHEMA_QUERY = text(
    """
    SELECT
      (SELECT array_agg(table_name::text ORDER BY table_name)
         FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name <> 'alembic_version'),
      (SELECT array_agg(typname::text ORDER BY typname)
         FROM pg_type WHERE typname IN ('currency', 'payment_status'))
    """
)


async def _schema(url: str) -> tuple[list[str] | None, list[str] | None]:
    engine = create_async_engine(url)
    try:
        async with engine.connect() as conn:
            tables, enums = (await conn.execute(SCHEMA_QUERY)).one()
    finally:
        await engine.dispose()
    return tables, enums


async def test_upgrade_downgrade_roundtrip(create_database: CreateDatabase) -> None:
    url = await create_database()

    await run_alembic(url, "upgrade", "head")
    assert await _schema(url) == (["outbox", "payments"], ["currency", "payment_status"])

    await run_alembic(url, "downgrade", "base")
    assert await _schema(url) == (None, None)

    # Повторный upgrade возможен: downgrade убрал за собой и enum-типы.
    await run_alembic(url, "upgrade", "head")
    assert await _schema(url) == (["outbox", "payments"], ["currency", "payment_status"])


async def test_models_match_migrations(migrated_db_url: str) -> None:
    def diff(conn: Connection) -> list[Any]:
        context = MigrationContext.configure(conn, opts={"compare_type": True})
        return list(compare_metadata(context, Base.metadata))

    engine = create_async_engine(migrated_db_url)
    try:
        async with engine.connect() as conn:
            assert await conn.run_sync(diff) == []
    finally:
        await engine.dispose()
