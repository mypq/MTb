
import logging
from contextlib import asynccontextmanager

from backend import __version__
from backend.config import get_settings
from backend.market_data.service import MarketDataService
from backend.mt5.adapter import MT5Adapter, MT5AdapterError
from backend.mt5.monitor import MT5ConnectionMonitor
from backend.security import verify_api_token
from fastapi import Depends, FastAPI, HTTPException, Query


logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    adapter = MT5Adapter(settings=settings)
    monitor = MT5ConnectionMonitor(
        adapter=adapter,
        interval_seconds=10.0,
    )

    app.state.settings = settings
    app.state.mt5_adapter = adapter
    app.state.mt5_monitor = monitor
    app.state.market_data_service = MarketDataService(source=adapter)

    try:
        if settings.MT5_ENABLED:
            adapter.connect()
            monitor.start()

        yield
    finally:
        monitor_stopped = False

        try:
            monitor_stopped = monitor.stop()
        except Exception:
            logger.exception(
                "Failed to stop MT5 connection monitor."
            )

        if monitor_stopped is True:
            try:
                adapter.shutdown()
            except Exception:
                logger.exception(
                    "Failed to shut down MT5 adapter."
                )
                raise
        else:
            logger.critical(
                "MT5 adapter shutdown skipped because the monitor "
                "worker may still be running. Manual recovery is required."
            )


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
        return {"status": "ok"}

    @application.get(
        "/api/v1/system/status",
        tags=["System"],
        dependencies=[Depends(verify_api_token)],
    )
    async def system_status() -> dict[str, object]:
        adapter: MT5Adapter = application.state.mt5_adapter
        monitor: MT5ConnectionMonitor = application.state.mt5_monitor
        connection = adapter.connection_status()

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
            "mt5_connection": connection.model_dump(mode="json"),
            "mt5_monitor_running": monitor.is_running,
        }

    @application.get(
        "/api/v1/market-data/candles",
        tags=["Market Data"],
        dependencies=[Depends(verify_api_token)],
    )
    def market_data_candles(
        symbol: str | None = None,
        timeframe: str = settings.DEFAULT_TIMEFRAME,
        count: int = Query(default=300, ge=1, le=10000),
    ) -> dict[str, object]:
        selected_symbol = (symbol or settings.MT5_SYMBOL).strip()

        if not selected_symbol:
            raise HTTPException(
                status_code=400,
                detail="A market symbol must be configured or supplied.",
            )

        service: MarketDataService = application.state.market_data_service

        try:
            snapshot = service.get_snapshot(
                symbol=selected_symbol,
                timeframe=timeframe,
                count=count,
            )
        except ValueError as exc:
            raise HTTPException(
                status_code=400,
                detail=str(exc),
            ) from exc
        except MT5AdapterError as exc:
            raise HTTPException(
                status_code=503,
                detail="Market data is currently unavailable.",
            ) from exc

        return snapshot.model_dump(mode="json")

    return application


app = create_app()
