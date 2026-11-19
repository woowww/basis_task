from dishka import Provider, Scope, provide

from payment_service.application.create_payment import CreatePayment
from payment_service.application.get_payment import GetPayment
from payment_service.application.process_payment import ProcessPayment
from payment_service.application.publish_outbox import PublishOutboxBatch


class UseCaseProvider(Provider):
    scope = Scope.REQUEST

    create_payment = provide(CreatePayment)
    get_payment = provide(GetPayment)
    publish_outbox_batch = provide(PublishOutboxBatch)
    process_payment = provide(ProcessPayment)
