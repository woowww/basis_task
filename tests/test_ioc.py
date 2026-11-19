import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from payment_service.application.ports import PaymentGateway, WebhookSender
from payment_service.config import Settings
from payment_service.infra.gateway.emulated import EmulatedPaymentGateway
from payment_service.infra.webhook.sender import HttpxWebhookSender
from payment_service.ioc import create_container
from tests.helpers import make_settings


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
            assert (await session.execute(text("SELECT 1 FROM payments LIMIT 0"))).all() == []
    finally:
        await container.close()


async def test_container_wires_external_services_and_closes_http_client() -> None:
    container = create_container(make_settings())
    gateway = await container.get(PaymentGateway)
    sender = await container.get(WebhookSender)
    client = await container.get(httpx.AsyncClient)

    assert isinstance(gateway, EmulatedPaymentGateway)
    assert isinstance(sender, HttpxWebhookSender)
    assert client.timeout == httpx.Timeout(5.0)

    await container.close()
    assert client.is_closed


@pytest.mark.parametrize(("prefetch", "pool_size"), [(1, 5), (10, 10), (30, 30)])
async def test_db_pool_fits_consumer_concurrency(prefetch: int, pool_size: int) -> None:
    settings = make_settings().model_copy(update={"consumer_prefetch": prefetch})
    container = create_container(settings)
    try:
        engine = await container.get(AsyncEngine)
        assert engine.pool.size() == pool_size  # type: ignore[attr-defined]
    finally:
        await container.close()
