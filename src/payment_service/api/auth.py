import secrets
from typing import Any

from dishka import FromDishka
from dishka.integrations.fastapi import inject
from fastapi import APIRouter, Depends, Security
from fastapi.security import APIKeyHeader

from payment_service.api.errors import ApiError, ErrorResponse
from payment_service.config import Settings

API_KEY_HEADER = "X-API-Key"

_api_key_header = APIKeyHeader(
    name=API_KEY_HEADER, auto_error=False, description="Статический API-ключ сервиса"
)


@inject
async def require_api_key(
    settings: FromDishka[Settings],
    api_key: str | None = Security(_api_key_header),
) -> None:
    expected = settings.api_key.get_secret_value().encode()
    if api_key is None or not secrets.compare_digest(api_key.encode(), expected):
        raise ApiError(401, "unauthorized", f"Missing or invalid {API_KEY_HEADER} header")


def protected_router(**kwargs: Any) -> APIRouter:
    """Роутер, все эндпоинты которого требуют валидный `X-API-Key`."""
    responses = {401: {"model": ErrorResponse, "description": "Missing or invalid API key"}}
    return APIRouter(
        dependencies=[Depends(require_api_key)],
        responses=responses | kwargs.pop("responses", {}),
        **kwargs,
    )
