"""Каждая роль процесса собирается из своей фабрики (ловит несовместимость интеграций)."""

from fastapi import FastAPI
from faststream import FastStream
from httpx import ASGITransport, AsyncClient

from payment_service.api.app import create_app as create_api
from payment_service.consumer.main import create_app as create_consumer
from tests.helpers import make_settings


async def test_api_app_serves_openapi() -> None:
    app = create_api(make_settings())
    assert isinstance(app, FastAPI)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/openapi.json")

    assert response.status_code == 200


def test_consumer_app_builds() -> None:
    assert isinstance(create_consumer(make_settings()), FastStream)
