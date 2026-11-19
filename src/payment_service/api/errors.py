"""Единый формат ошибок API: `{"error": {"code", "message", "details"}}`."""

from collections.abc import Mapping
from http import HTTPStatus
from typing import Any

from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from starlette.exceptions import HTTPException as StarletteHTTPException

from payment_service.application.errors import (
    ApplicationError,
    IdempotencyConflictError,
    PaymentNotFoundError,
)

# Прикладная ошибка → (HTTP-статус, код ошибки).
APPLICATION_ERRORS: dict[type[ApplicationError], tuple[int, str]] = {
    IdempotencyConflictError: (409, "idempotency_conflict"),
    PaymentNotFoundError: (404, "payment_not_found"),
}


class ErrorBody(BaseModel):
    code: str
    message: str
    details: Any = None


class ErrorResponse(BaseModel):
    error: ErrorBody


class ApiError(Exception):
    """Ошибка, которую обработчики API превращают в ответ единого формата."""

    def __init__(self, status_code: int, code: str, message: str, details: Any = None) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message
        self.details = details


def error_response(
    status_code: int,
    code: str,
    message: str,
    details: Any = None,
    headers: Mapping[str, str] | None = None,
) -> JSONResponse:
    body = ErrorResponse(error=ErrorBody(code=code, message=message, details=details))
    return JSONResponse(
        status_code=status_code,
        content=jsonable_encoder(body),
        headers=dict(headers) if headers else None,
    )


def _code_for(status_code: int) -> str:
    try:
        return HTTPStatus(status_code).phrase.lower().replace(" ", "_").replace("-", "_")
    except ValueError:
        return "error"


async def _api_error_handler(_: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, ApiError)  # noqa: S101
    return error_response(exc.status_code, exc.code, exc.message, exc.details)


async def _application_error_handler(_: Request, exc: Exception) -> JSONResponse:
    for exc_type, (status_code, code) in APPLICATION_ERRORS.items():
        if isinstance(exc, exc_type):
            return error_response(status_code, code, str(exc))
    raise exc  # не сопоставленная ошибка — 500 через стандартный обработчик


async def _http_exception_handler(_: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, StarletteHTTPException)  # noqa: S101
    return error_response(
        exc.status_code,
        _code_for(exc.status_code),
        str(exc.detail),
        headers=exc.headers,
    )


async def _validation_error_handler(_: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, RequestValidationError)  # noqa: S101
    details = [
        {"loc": list(err["loc"]), "msg": err["msg"], "type": err["type"]} for err in exc.errors()
    ]
    return error_response(422, "validation_error", "Request validation failed", details)


def register_error_handlers(app: FastAPI) -> None:
    app.add_exception_handler(ApiError, _api_error_handler)
    app.add_exception_handler(ApplicationError, _application_error_handler)
    app.add_exception_handler(StarletteHTTPException, _http_exception_handler)
    app.add_exception_handler(RequestValidationError, _validation_error_handler)
