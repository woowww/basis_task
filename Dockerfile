# syntax=docker/dockerfile:1
FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/app/.venv

COPY --from=ghcr.io/astral-sh/uv:0.7.0 /uv /usr/local/bin/uv

WORKDIR /app

# Зависимости отдельным слоем — кэшируются, пока не меняется lock-файл
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project

COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

RUN useradd --create-home --uid 1000 app && chown -R app /app
USER app

ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 8000

# Роль процесса задаётся командой в docker-compose:
#   api:          uvicorn --factory payment_service.api.app:create_app --host 0.0.0.0 --port 8000
#   consumer:     faststream run --factory payment_service.consumer.main:create_app
#   outbox-relay: python -m payment_service.relay
CMD ["uvicorn", "--factory", "payment_service.api.app:create_app", "--host", "0.0.0.0", "--port", "8000"]
