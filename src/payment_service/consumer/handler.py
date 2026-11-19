import logging
import uuid

from dishka import FromDishka
from pydantic import BaseModel

from payment_service.application.process_payment import ProcessPayment

logger = logging.getLogger(__name__)


class PaymentCreatedMessage(BaseModel):
    payment_id: uuid.UUID


async def handle_payment_created(
    message: PaymentCreatedMessage, process: FromDishka[ProcessPayment]
) -> None:
    result = await process(message.payment_id)
    logger.info("message handled payment_id=%s result=%s", message.payment_id, result)
