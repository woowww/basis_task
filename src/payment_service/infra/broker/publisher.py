from typing import Any

from faststream.rabbit import RabbitBroker

from payment_service.domain.payment import OutboxEvent
from payment_service.infra.broker.topology import ROUTING_KEY, Topology, retry_queue_name

ATTEMPT_HEADER = "x-attempt"
DEATH_REASON_HEADER = "x-death-reason"


class RabbitMessagePublisher:
    """Публикация с publisher confirms; сообщения persistent.

    Канал брокера должен быть открыт с `on_return_raises=True`, чтобы неотмаршрутизированное
    сообщение приводило к исключению, а не терялось молча.
    """

    def __init__(self, broker: RabbitBroker, topology: Topology) -> None:
        self._broker = broker
        self._topology = topology

    async def publish_payment_event(self, event: OutboxEvent) -> None:
        await self._broker.publish(
            event.payload,
            exchange=self._topology.exchange,
            routing_key=ROUTING_KEY,
            message_id=str(event.aggregate_id),
            message_type=event.event_type,
            headers={ATTEMPT_HEADER: 1},
            persist=True,
        )

    async def publish_retry(
        self, payload: dict[str, Any], *, message_id: str, next_attempt: int
    ) -> None:
        """Отложить сообщение до попытки `next_attempt` (2..max_attempts)."""
        if not 2 <= next_attempt <= self._topology.max_attempts:
            msg = f"next_attempt must be in 2..{self._topology.max_attempts}, got {next_attempt}"
            raise ValueError(msg)
        await self._broker.publish(
            payload,
            exchange=self._topology.retry_exchange,
            routing_key=retry_queue_name(next_attempt - 1),
            message_id=message_id,
            headers={ATTEMPT_HEADER: next_attempt},
            persist=True,
        )

    async def publish_dead_letter(
        self,
        payload: dict[str, Any] | bytes,
        *,
        message_id: str,
        attempts: int,
        reason: str,
        content_type: str | None = None,
    ) -> None:
        await self._broker.publish(
            payload,
            content_type=content_type,
            exchange=self._topology.dead_letter_exchange,
            routing_key=ROUTING_KEY,
            message_id=message_id,
            headers={ATTEMPT_HEADER: attempts, DEATH_REASON_HEADER: reason},
            persist=True,
        )
