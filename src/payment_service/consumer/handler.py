"""Адаптер `payments.new`: разбор сообщения, вызов юзкейса, политика retry/DLQ.

- технический сбой → повтор через delay-очередь (`x-attempt` + 1), после последней попытки — DLQ;
- неизвестный платёж и битое сообщение → сразу DLQ (повтор не поможет);
- исходное сообщение после перекладывания подтверждается (ack). Если не удалось опубликовать
  повтор/DLQ — исключение пробрасывается, сообщение отклоняется и по DLX попадает в DLQ.
"""

import json
import logging
import uuid
from typing import Any

from dishka import FromDishka
from faststream.rabbit.annotations import RabbitMessage
from pydantic import BaseModel, ValidationError

from payment_service.application.errors import PaymentNotFoundError
from payment_service.application.ports import MessagePublisher
from payment_service.application.process_payment import ProcessPayment
from payment_service.infra.broker.publisher import ATTEMPT_HEADER
from payment_service.infra.broker.topology import Topology

logger = logging.getLogger(__name__)


class PaymentCreatedMessage(BaseModel):
    payment_id: uuid.UUID


class InvalidMessageError(Exception):
    pass


def parse(body: bytes) -> tuple[PaymentCreatedMessage, dict[str, Any]]:
    try:
        payload = json.loads(body)
        return PaymentCreatedMessage.model_validate(payload), payload
    except (ValueError, ValidationError) as exc:  # JSONDecodeError — подкласс ValueError
        msg = f"invalid message: {exc}".splitlines()[0]
        raise InvalidMessageError(msg) from exc


def attempt_of(message: RabbitMessage) -> int:
    try:
        return max(1, int(message.headers.get(ATTEMPT_HEADER, 1)))
    except (TypeError, ValueError):
        return 1


def describe(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}"[:500]


async def handle_payment_created(
    message: RabbitMessage,
    process: FromDishka[ProcessPayment],
    publisher: FromDishka[MessagePublisher],
    topology: FromDishka[Topology],
) -> None:
    attempt = attempt_of(message)
    message_id = message.message_id

    async def dead_letter(reason: str, payment_id: object = None) -> None:
        await publisher.publish_dead_letter(
            message.body,
            message_id=message_id,
            attempts=attempt,
            reason=reason,
            content_type=message.content_type,
        )
        logger.error(
            "message dead-lettered payment_id=%s attempt=%d reason=%s",
            payment_id,
            attempt,
            reason,
        )

    try:
        parsed, payload = parse(message.body)
    except InvalidMessageError as exc:
        await dead_letter(str(exc))
        return

    payment_id = parsed.payment_id
    try:
        result = await process(payment_id)
    except PaymentNotFoundError as exc:
        await dead_letter(describe(exc), payment_id)
    except Exception as exc:  # технический сбой — повторяем
        if attempt < topology.max_attempts:
            await publisher.publish_retry(payload, message_id=message_id, next_attempt=attempt + 1)
            logger.warning(
                "processing failed, retry scheduled payment_id=%s attempt=%d/%d error=%s",
                payment_id,
                attempt,
                topology.max_attempts,
                describe(exc),
            )
        else:
            await dead_letter(describe(exc), payment_id)
    else:
        logger.info(
            "message handled payment_id=%s attempt=%d result=%s", payment_id, attempt, result
        )
