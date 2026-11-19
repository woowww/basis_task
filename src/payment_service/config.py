from functools import lru_cache

from pydantic import Field, PositiveFloat, PositiveInt, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Конфигурация всех ролей процесса. Читается из переменных окружения и `.env`."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    api_key: SecretStr
    database_url: str
    rabbitmq_url: str

    retry_base_delay_seconds: PositiveFloat = 1.0
    max_attempts: PositiveInt = 3
    consumer_prefetch: PositiveInt = 10

    gateway_success_rate: float = Field(default=0.9, ge=0.0, le=1.0)
    gateway_min_delay_seconds: float = Field(default=2.0, ge=0.0)
    gateway_max_delay_seconds: float = Field(default=5.0, ge=0.0)

    webhook_timeout_seconds: PositiveFloat = 5.0

    outbox_poll_interval_seconds: PositiveFloat = 1.0
    outbox_batch_size: PositiveInt = 100

    log_level: str = "INFO"

    @model_validator(mode="after")
    def _check_gateway_delay_range(self) -> "Settings":
        if self.gateway_min_delay_seconds > self.gateway_max_delay_seconds:
            msg = "GATEWAY_MIN_DELAY_SECONDS must not exceed GATEWAY_MAX_DELAY_SECONDS"
            raise ValueError(msg)
        return self

    def retry_delay_seconds(self, attempt: int) -> float:
        """Задержка перед попыткой `attempt + 1`: база * 2^(attempt-1)."""
        return float(self.retry_base_delay_seconds * 2 ** (attempt - 1))


@lru_cache
def get_settings() -> Settings:
    return Settings()
