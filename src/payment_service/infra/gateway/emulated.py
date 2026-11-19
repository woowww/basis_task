import asyncio
import random
from collections.abc import Awaitable, Callable

from payment_service.domain.payment import Payment, PaymentStatus


class EmulatedPaymentGateway:
    """Эмуляция внешнего шлюза: задержка в диапазоне и заданная доля успехов."""

    def __init__(
        self,
        *,
        success_rate: float,
        min_delay_seconds: float,
        max_delay_seconds: float,
        rng: random.Random | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._success_rate = success_rate
        self._min_delay = min_delay_seconds
        self._max_delay = max_delay_seconds
        self._rng = rng or random.Random()  # noqa: S311 — не криптография, эмуляция
        self._sleep = sleep

    async def process(self, payment: Payment) -> PaymentStatus:
        await self._sleep(self._rng.uniform(self._min_delay, self._max_delay))
        if self._rng.random() < self._success_rate:
            return PaymentStatus.SUCCEEDED
        return PaymentStatus.FAILED
