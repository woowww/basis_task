from collections.abc import AsyncIterator

import pytest
from fastapi import FastAPI
from fastapi.routing import APIRoute
from httpx import ASGITransport, AsyncClient

from payment_service.api.app import create_app
from payment_service.api.auth import protected_router, require_api_key
from tests.helpers import API_KEY, make_settings


@pytest.fixture
def app() -> FastAPI:
    app = create_app(make_settings())

    # Тестовый эндпоинт на защищённом роутере: так же, как будут подключаться реальные.
    probe = protected_router(prefix="/api/v1/_probe")

    @probe.get("")
    async def probe_endpoint(n: int = 0) -> dict[str, int]:
        return {"n": n}

    app.include_router(probe)
    return app


@pytest.fixture
async def client(app: FastAPI) -> AsyncIterator[AsyncClient]:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client


async def test_valid_key_is_accepted(client: AsyncClient) -> None:
    response = await client.get("/api/v1/_probe", headers={"X-API-Key": API_KEY})

    assert response.status_code == 200
    assert response.json() == {"n": 0}


@pytest.mark.parametrize(
    "headers",
    [
        pytest.param({}, id="missing"),
        pytest.param({"X-API-Key": "wrong"}, id="wrong"),
        pytest.param({"X-API-Key": ""}, id="empty"),
        pytest.param({"X-API-Key": API_KEY + "x"}, id="prefix-match"),
    ],
)
async def test_missing_or_invalid_key_is_rejected(
    client: AsyncClient, headers: dict[str, str]
) -> None:
    response = await client.get("/api/v1/_probe", headers=headers)

    assert response.status_code == 401
    assert response.json() == {
        "error": {
            "code": "unauthorized",
            "message": "Missing or invalid X-API-Key header",
            "details": None,
        }
    }


async def test_auth_is_checked_before_validation(client: AsyncClient) -> None:
    response = await client.get("/api/v1/_probe", params={"n": "not-a-number"})

    assert response.status_code == 401


async def test_validation_error_format(client: AsyncClient) -> None:
    response = await client.get(
        "/api/v1/_probe", params={"n": "not-a-number"}, headers={"X-API-Key": API_KEY}
    )

    assert response.status_code == 422
    body = response.json()["error"]
    assert body["code"] == "validation_error"
    assert body["details"][0]["loc"] == ["query", "n"]


async def test_unknown_route_uses_error_format(client: AsyncClient) -> None:
    response = await client.get("/api/v1/nope", headers={"X-API-Key": API_KEY})

    assert response.status_code == 404
    assert response.json()["error"]["code"] == "not_found"


async def test_openapi_declares_api_key_scheme(client: AsyncClient) -> None:
    schema = (await client.get("/openapi.json")).json()

    assert schema["components"]["securitySchemes"]["APIKeyHeader"] == {
        "type": "apiKey",
        "in": "header",
        "name": "X-API-Key",
        "description": "Статический API-ключ сервиса",
    }


def unprotected_v1_routes(app: FastAPI) -> list[str]:
    return [
        route.path
        for route in app.routes
        if isinstance(route, APIRoute)
        and route.path.startswith("/api/v1")
        and not any(dep.call is require_api_key for dep in route.dependant.dependencies)
    ]


def test_every_v1_route_requires_api_key() -> None:
    """Страховка: любой эндпоинт под /api/v1 в реальном приложении закрыт ключом."""
    assert unprotected_v1_routes(create_app(make_settings())) == []


def test_guard_detects_unprotected_route(app: FastAPI) -> None:
    @app.get("/api/v1/_open")
    async def open_endpoint() -> None: ...

    assert unprotected_v1_routes(app) == ["/api/v1/_open"]
