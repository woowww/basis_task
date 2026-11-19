from typing import Any

import httpx

from payment_service.application.errors import WebhookDeliveryError

USER_AGENT = "payment-service-webhook/0.1"


def create_webhook_client(timeout_seconds: float) -> httpx.AsyncClient:
    # Редиректы не выполняем: 3xx — это не подтверждение доставки.
    return httpx.AsyncClient(
        timeout=timeout_seconds,
        follow_redirects=False,
        headers={"User-Agent": USER_AGENT},
    )


class HttpxWebhookSender:
    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client = client

    async def send(self, url: str, payload: dict[str, Any]) -> None:
        try:
            response = await self._client.post(url, json=payload)
        except httpx.TimeoutException as exc:
            msg = f"webhook timeout: {type(exc).__name__}"
            raise WebhookDeliveryError(msg) from exc
        except httpx.HTTPError as exc:
            msg = f"webhook transport error: {type(exc).__name__}: {exc}"
            raise WebhookDeliveryError(msg) from exc
        if not response.is_success:
            msg = f"webhook responded {response.status_code}"
            raise WebhookDeliveryError(msg)
