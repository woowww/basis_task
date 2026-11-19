import random
from collections import Counter
from decimal import Decimal

import pytest

from payment_service.domain.payment import Currency, Payment, PaymentStatus
from payment_service.infra.gateway.emulated import EmulatedPaymentGateway


def make_payment() -> Payment:
    return Payment.new(
        amount=Decimal("10.00"),
        currency=Currency.EUR,
        description="test",
        metadata={},
        idempotency_key="k",
        request_hash="h",
        webhook_url="https://example.com/hook",
    )


class RecordingSleep:
    def __init__(self) -> None:
        self.delays: list[float] = []

    async def __call__(self, seconds: float) -> None:
        self.delays.append(seconds)


def make_gateway(
    success_rate: float = 0.9, seed: int = 42
) -> tuple[EmulatedPaymentGateway, RecordingSleep]:
    sleep = RecordingSleep()
    gateway = EmulatedPaymentGateway(
        success_rate=success_rate,
        min_delay_seconds=2,
        max_delay_seconds=5,
        rng=random.Random(seed),  # noqa: S311
        sleep=sleep,
    )
    return gateway, sleep


async def test_outcomes_follow_success_rate_and_delay_range() -> None:
    gateway, sleep = make_gateway(success_rate=0.9)
    payment = make_payment()

    outcomes = Counter([await gateway.process(payment) for _ in range(5000)])

    assert set(outcomes) == {PaymentStatus.SUCCEEDED, PaymentStatus.FAILED}
    assert outcomes[PaymentStatus.SUCCEEDED] / 5000 == pytest.approx(0.9, abs=0.02)
    assert len(sleep.delays) == 5000
    assert all(2 <= d <= 5 for d in sleep.delays)
    assert max(sleep.delays) - min(sleep.delays) > 2.5  # задержки распределены по диапазону


@pytest.mark.parametrize(
    ("success_rate", "expected"),
    [(1.0, PaymentStatus.SUCCEEDED), (0.0, PaymentStatus.FAILED)],
)
async def test_extreme_success_rates_are_deterministic(
    success_rate: float, expected: PaymentStatus
) -> None:
    gateway, _ = make_gateway(success_rate=success_rate)

    assert {await gateway.process(make_payment()) for _ in range(200)} == {expected}


async def test_same_seed_gives_same_sequence() -> None:
    first, _ = make_gateway(seed=7)
    second, _ = make_gateway(seed=7)
    payment = make_payment()

    assert [await first.process(payment) for _ in range(50)] == [
        await second.process(payment) for _ in range(50)
    ]
