from dishka import Provider, Scope, provide

from payment_service.application.create_payment import CreatePayment
from payment_service.application.get_payment import GetPayment


class UseCaseProvider(Provider):
    scope = Scope.REQUEST

    create_payment = provide(CreatePayment)
    get_payment = provide(GetPayment)
