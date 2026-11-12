from contextlib import asynccontextmanager

from fastapi import FastAPI

from backend import __version__
from backend.config import get_settings


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    app.state.settings = settings
    yield


def create_app() -> FastAPI:
    settings = get_settings()

    application = FastAPI(
        title=settings.APP_NAME,
        version=__version__,
        description="GOLD BOT local trading system API.",
        lifespan=lifespan,
    )

    @application.get("/health", tags=["System"])
    async def health() -> dict[str, str]:
        return {
            "status": "ok",
            "application": settings.APP_NAME,
            "version": __version__,
            "mode": settings.BOT_MODE,
        }

    @application.get("/api/v1/system/status", tags=["System"])
    async def system_status() -> dict[str, object]:
        return {
            "application": settings.APP_NAME,
            "version": __version__,
            "environment": settings.APP_ENV,
            "mode": settings.BOT_MODE,
            "trading_enabled": settings.TRADING_ENABLED,
            "live_trading_authorized": settings.LIVE_TRADING_AUTHORIZED,
            "emergency_stop": settings.EMERGENCY_STOP,
            "trading_blocked": settings.trading_must_be_blocked,
            "live_trading_blocked": settings.live_trading_must_be_blocked,
            "mt5_enabled": settings.MT5_ENABLED,
        }

    return application


app = create_app()
