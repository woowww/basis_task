from payment_service.api.auth import protected_router

# Эндпоинты платежей подключаются сюда в T4/T5.
router = protected_router(prefix="/api/v1")
