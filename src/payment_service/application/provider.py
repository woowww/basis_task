from dishka import Provider, Scope, provide

from payment_service.application.create_payment import CreatePayment


class UseCaseProvider(Provider):
    scope = Scope.REQUEST

    create_payment = provide(CreatePayment)
