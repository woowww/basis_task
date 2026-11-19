from payment_service.api import payments
from payment_service.api.auth import protected_router

router = protected_router(prefix="/api/v1")
router.include_router(payments.router)
