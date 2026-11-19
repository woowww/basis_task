import asyncio
import uuid

from dishka import Provider, Scope, provide

from payment_service.application.ports import PaymentGateway
from payment_service.domain.payment import Payment, PaymentStatus


class FakeGateway:
    """Детерминированный шлюз: заданный исход, опциональная задержка, журнал вызовов."""

    def __init__(
        self, outcome: PaymentStatus = PaymentStatus.SUCCEEDED, delay: float = 0.0
    ) -> None:
        self.outcome = outcome
        self.delay = delay
        self.calls: list[uuid.UUID] = []

    async def process(self, payment: Payment) -> PaymentStatus:
        self.calls.append(payment.id)
        if self.delay:
            await asyncio.sleep(self.delay)
        return self.outcome


class GatewayOverride(Provider):
    def __init__(self, gateway: FakeGateway) -> None:
        super().__init__()
        self._gateway = gateway

    @provide(scope=Scope.APP)
    def gateway(self) -> PaymentGateway:
        return self._gateway
