import json
from collections.abc import AsyncIterator

import httpx
import pytest
import respx

from payment_service.application.errors import WebhookDeliveryError
from payment_service.infra.webhook.sender import HttpxWebhookSender, create_webhook_client

URL = "https://client.example.com/hooks/payments"
PAYLOAD = {"payment_id": "p-1", "status": "succeeded", "amount": "10.00"}


@pytest.fixture
async def sender() -> AsyncIterator[HttpxWebhookSender]:
    async with create_webhook_client(timeout_seconds=5) as client:
        yield HttpxWebhookSender(client)


@pytest.mark.parametrize("status", [200, 201, 202, 204])
async def test_2xx_is_delivered(
    sender: HttpxWebhookSender, respx_mock: respx.MockRouter, status: int
) -> None:
    route = respx_mock.post(URL).respond(status)

    await sender.send(URL, PAYLOAD)

    request = route.calls.last.request
    assert json.loads(request.content) == PAYLOAD
    assert request.headers["content-type"] == "application/json"
    assert request.headers["user-agent"].startswith("payment-service-webhook/")


@pytest.mark.parametrize("status", [301, 302, 400, 404, 500, 503])
async def test_non_2xx_is_delivery_error(
    sender: HttpxWebhookSender, respx_mock: respx.MockRouter, status: int
) -> None:
    respx_mock.post(URL).respond(status, headers={"Location": "https://elsewhere.example.com"})

    with pytest.raises(WebhookDeliveryError, match=f"responded {status}"):
        await sender.send(URL, PAYLOAD)


@pytest.mark.parametrize(
    ("side_effect", "match"),
    [
        (httpx.ConnectTimeout("connect timeout"), "timeout"),
        (httpx.ReadTimeout("read timeout"), "timeout"),
        (httpx.ConnectError("connection refused"), "transport error"),
        (httpx.RemoteProtocolError("server disconnected"), "transport error"),
    ],
)
async def test_network_failures_are_delivery_errors(
    sender: HttpxWebhookSender,
    respx_mock: respx.MockRouter,
    side_effect: Exception,
    match: str,
) -> None:
    respx_mock.post(URL).mock(side_effect=side_effect)

    with pytest.raises(WebhookDeliveryError, match=match):
        await sender.send(URL, PAYLOAD)


async def test_client_uses_configured_timeout() -> None:
    async with create_webhook_client(timeout_seconds=1.5) as client:
        assert client.timeout == httpx.Timeout(1.5)
        assert client.follow_redirects is False
