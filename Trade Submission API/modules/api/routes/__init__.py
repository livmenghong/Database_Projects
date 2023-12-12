from fastapi import APIRouter
from modules.api.routes.trades import router as trades_router


router = APIRouter()
router.include_router(trades_router, prefix="/trades", tags=["Trade APIs"])
