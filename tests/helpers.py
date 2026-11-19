from payment_service.config import Settings

API_KEY = "test-api-key"
AUTH = {"X-API-Key": API_KEY}


def make_settings(database_url: str = "postgresql+asyncpg://u:p@localhost/db") -> Settings:
    return Settings(
        api_key=API_KEY,
        database_url=database_url,
        rabbitmq_url="amqp://guest:guest@localhost/",
    )
