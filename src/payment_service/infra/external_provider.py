from collections.abc import AsyncIterable

import httpx
from dishka import Provider, Scope, provide

from payment_service.application.ports import PaymentGateway, WebhookSender
from payment_service.config import Settings
from payment_service.infra.gateway.emulated import EmulatedPaymentGateway
from payment_service.infra.webhook.sender import HttpxWebhookSender, create_webhook_client


class ExternalServicesProvider(Provider):
    """Внешние системы: платёжный шлюз (эмуляция) и доставка webhook."""

    scope = Scope.APP

    @provide
    def gateway(self, settings: Settings) -> PaymentGateway:
        return EmulatedPaymentGateway(
            success_rate=settings.gateway_success_rate,
            min_delay_seconds=settings.gateway_min_delay_seconds,
            max_delay_seconds=settings.gateway_max_delay_seconds,
        )

    @provide
    async def webhook_client(self, settings: Settings) -> AsyncIterable[httpx.AsyncClient]:
        async with create_webhook_client(settings.webhook_timeout_seconds) as client:
            yield client

    webhook_sender = provide(HttpxWebhookSender, provides=WebhookSender)
