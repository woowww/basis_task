"""Топология RabbitMQ.

    payments (direct) ──payments.new──► payments.new ──(reject)──► payments.dlx
    payments.retry (direct) ──payments.new.retry.N──► payments.new.retry.N (TTL)
                                                        └─(TTL истёк)─► payments
    payments.dlx (direct) ──payments.new──► payments.new.dlq

Очередь `payments.new.retry.N` держит сообщение перед попыткой N+1.
Важно: аргументы очереди (в т.ч. TTL) неизменяемы — при смене RETRY_BASE_DELAY_SECONDS
или MAX_ATTEMPTS существующие retry-очереди нужно удалить перед стартом.
"""

from dataclasses import dataclass

from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from payment_service.config import Settings

EXCHANGE = "payments"
RETRY_EXCHANGE = "payments.retry"
DEAD_LETTER_EXCHANGE = "payments.dlx"
QUEUE = "payments.new"
ROUTING_KEY = "payments.new"
DEAD_LETTER_QUEUE = "payments.new.dlq"


def retry_queue_name(n: int) -> str:
    return f"{QUEUE}.retry.{n}"


@dataclass(frozen=True)
class Topology:
    exchange: RabbitExchange
    retry_exchange: RabbitExchange
    dead_letter_exchange: RabbitExchange
    queue: RabbitQueue
    retry_queues: tuple[RabbitQueue, ...]
    dead_letter_queue: RabbitQueue

    @classmethod
    def from_settings(cls, settings: Settings) -> "Topology":
        retry_queues = tuple(
            RabbitQueue(
                retry_queue_name(n),
                durable=True,
                routing_key=retry_queue_name(n),
                arguments={
                    "x-message-ttl": round(settings.retry_delay_seconds(n) * 1000),
                    "x-dead-letter-exchange": EXCHANGE,
                    "x-dead-letter-routing-key": ROUTING_KEY,
                },
            )
            for n in range(1, settings.max_attempts)
        )
        return cls(
            exchange=RabbitExchange(EXCHANGE, type=ExchangeType.DIRECT, durable=True),
            retry_exchange=RabbitExchange(RETRY_EXCHANGE, type=ExchangeType.DIRECT, durable=True),
            dead_letter_exchange=RabbitExchange(
                DEAD_LETTER_EXCHANGE, type=ExchangeType.DIRECT, durable=True
            ),
            queue=RabbitQueue(
                QUEUE,
                durable=True,
                routing_key=ROUTING_KEY,
                arguments={
                    "x-dead-letter-exchange": DEAD_LETTER_EXCHANGE,
                    "x-dead-letter-routing-key": ROUTING_KEY,
                },
            ),
            retry_queues=retry_queues,
            dead_letter_queue=RabbitQueue(DEAD_LETTER_QUEUE, durable=True, routing_key=ROUTING_KEY),
        )

    @property
    def max_attempts(self) -> int:
        return len(self.retry_queues) + 1

    async def declare(self, broker: RabbitBroker) -> None:
        """Идемпотентно объявить все обменники, очереди и привязки."""
        exchange = await broker.declare_exchange(self.exchange)
        retry_exchange = await broker.declare_exchange(self.retry_exchange)
        dead_letter_exchange = await broker.declare_exchange(self.dead_letter_exchange)

        bindings = [
            (self.queue, exchange),
            *((q, retry_exchange) for q in self.retry_queues),
            (self.dead_letter_queue, dead_letter_exchange),
        ]
        for queue, target in bindings:
            declared = await broker.declare_queue(queue)
            await declared.bind(target, routing_key=queue.routing())

    async def declare_exchange(self, broker: RabbitBroker) -> None:
        """Только основной обменник — достаточно для публикующей стороны (relay)."""
        await broker.declare_exchange(self.exchange)
