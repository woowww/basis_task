from collections.abc import AsyncIterable

from dishka import Provider, Scope, provide
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from payment_service.application.ports import OutboxRepository, PaymentRepository, UnitOfWork
from payment_service.config import Settings
from payment_service.infra.db.repositories import (
    SqlOutboxRepository,
    SqlPaymentRepository,
    SqlUnitOfWork,
)


class DbProvider(Provider):
    @provide(scope=Scope.APP)
    async def engine(self, settings: Settings) -> AsyncIterable[AsyncEngine]:
        # Consumer держит соединение на время вызова шлюза (строка платежа заблокирована),
        # поэтому пул должен вмещать CONSUMER_PREFETCH параллельных обработок.
        engine = create_async_engine(
            settings.database_url,
            pool_pre_ping=True,
            pool_size=max(5, settings.consumer_prefetch),
            max_overflow=10,
        )
        yield engine
        await engine.dispose()

    @provide(scope=Scope.APP)
    def session_factory(self, engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
        return async_sessionmaker(engine, expire_on_commit=False)

    @provide(scope=Scope.REQUEST)
    async def session(
        self, factory: async_sessionmaker[AsyncSession]
    ) -> AsyncIterable[AsyncSession]:
        async with factory() as session:
            yield session

    payments = provide(SqlPaymentRepository, provides=PaymentRepository, scope=Scope.REQUEST)
    outbox = provide(SqlOutboxRepository, provides=OutboxRepository, scope=Scope.REQUEST)
    uow = provide(SqlUnitOfWork, provides=UnitOfWork, scope=Scope.REQUEST)
