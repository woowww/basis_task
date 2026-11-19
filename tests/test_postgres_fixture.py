from sqlalchemy import make_url, text
from sqlalchemy.ext.asyncio import create_async_engine


async def test_postgres_fixture_gives_fresh_empty_database(postgres_url: str) -> None:
    engine = create_async_engine(postgres_url)
    try:
        async with engine.connect() as conn:
            current = (await conn.execute(text("SELECT current_database()"))).scalar_one()
            tables = (
                await conn.execute(
                    text(
                        "SELECT count(*) FROM information_schema.tables "
                        "WHERE table_schema = 'public'"
                    )
                )
            ).scalar_one()
    finally:
        await engine.dispose()

    assert current == make_url(postgres_url).database
    assert current.startswith("test_")
    assert tables == 0
