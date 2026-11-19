import pytest
from pydantic import ValidationError

from payment_service.config import Settings

REQUIRED = {
    "API_KEY": "secret",
    "DATABASE_URL": "postgresql+asyncpg://u:p@localhost/db",
    "RABBITMQ_URL": "amqp://guest:guest@localhost/",
}


@pytest.fixture
def env(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    monkeypatch.chdir("/")  # не подхватывать локальный .env
    for key, value in REQUIRED.items():
        monkeypatch.setenv(key, value)
    return monkeypatch


def test_defaults_match_spec(env: pytest.MonkeyPatch) -> None:
    settings = Settings()

    assert settings.api_key.get_secret_value() == "secret"
    assert settings.max_attempts == 3
    assert settings.gateway_success_rate == 0.9
    assert (settings.gateway_min_delay_seconds, settings.gateway_max_delay_seconds) == (2.0, 5.0)
    assert settings.webhook_timeout_seconds == 5.0


@pytest.mark.parametrize("missing", list(REQUIRED))
def test_required_variables(env: pytest.MonkeyPatch, missing: str) -> None:
    env.delenv(missing)

    with pytest.raises(ValidationError):
        Settings()


def test_retry_delay_is_exponential(env: pytest.MonkeyPatch) -> None:
    env.setenv("RETRY_BASE_DELAY_SECONDS", "1.5")
    settings = Settings()

    assert [settings.retry_delay_seconds(n) for n in (1, 2, 3)] == [1.5, 3.0, 6.0]


def test_rejects_inverted_gateway_delay_range(env: pytest.MonkeyPatch) -> None:
    env.setenv("GATEWAY_MIN_DELAY_SECONDS", "6")

    with pytest.raises(ValidationError):
        Settings()
