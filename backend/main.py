
import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, Query

from backend import __version__
from backend.config import get_settings
from backend.database import Database
from backend.database_backup import DatabaseBackupService
from backend.database_migrations import upgrade_database
from backend.market_data.chrony import ChronyStatusService
from backend.market_data.collector import MarketDataCollector
from backend.market_data.repository import (
    MarketDataPersistenceConflict,
    MarketDataRepository,
)
from backend.market_data.service import MarketDataService
from backend.mt5.adapter import MT5Adapter, MT5AdapterError
from backend.mt5.monitor import MT5ConnectionMonitor
from backend.security import verify_api_token

logger = logging.getLogger(__name__)

BACKUP_INTERVAL_SECONDS = 24 * 60 * 60


async def _database_backup_loop(
    backup_service: DatabaseBackupService,
    stop_event: asyncio.Event,
) -> None:
    while not stop_event.is_set():
        try:
            await asyncio.to_thread(
                backup_service.create_backup
            )
            logger.info(
                "Automatic database backup completed."
            )
        except Exception:
            logger.exception(
                "Automatic database backup failed."
            )

        if stop_event.is_set():
            break

        try:
            await asyncio.wait_for(
                stop_event.wait(),
                timeout=BACKUP_INTERVAL_SECONDS,
            )
        except asyncio.TimeoutError:
            continue


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    adapter = MT5Adapter(settings=settings)
    monitor = MT5ConnectionMonitor(
        adapter=adapter,
        interval_seconds=10.0,
    )
    chrony_service = ChronyStatusService()
    database = Database(settings.DATABASE_URL)
    market_data_repository = MarketDataRepository(database)
    market_data_service = MarketDataService(source=adapter)

    backup_service = None
    backup_stop_event = asyncio.Event()
    backup_task = None

    collector = None
    collector_stop_event = asyncio.Event()
    collector_task = None

    app.state.settings = settings
    app.state.mt5_adapter = adapter
    app.state.mt5_monitor = monitor
    app.state.market_data_service = market_data_service
    app.state.market_data_repository = market_data_repository
    app.state.market_data_collector = None
    app.state.chrony_status_service = chrony_service
    app.state.database = database
    app.state.database_backup_service = None

    try:
        database.initialize()
        upgrade_database()

        backup_service = DatabaseBackupService(database)
        app.state.database_backup_service = backup_service

        backup_task = asyncio.create_task(
            _database_backup_loop(
                backup_service,
                backup_stop_event,
            ),
            name="database-backup",
        )

        if settings.MT5_ENABLED:
            adapter.connect()
            monitor.start()

            selected_symbol = settings.MT5_SYMBOL.strip()

            if selected_symbol:
                collector = MarketDataCollector(
                    service=market_data_service,
                    repository=market_data_repository,
                    symbol=selected_symbol,
                    timeframe=settings.DEFAULT_TIMEFRAME,
                    count=300,
                )
                app.state.market_data_collector = collector

                collector_task = asyncio.create_task(
                    collector.run(collector_stop_event),
                    name="market-data-collector",
                )

                logger.info(
                    "Automatic market data collection enabled: "
                    "symbol=%s timeframe=%s interval=%.1f seconds",
                    selected_symbol,
                    settings.DEFAULT_TIMEFRAME,
                    collector.interval_seconds,
                )
            else:
                logger.warning(
                    "Automatic market data collection skipped: "
                    "MT5_SYMBOL is empty."
                )

        yield

    finally:
        try:
            if collector_task is not None:
                collector_stop_event.set()

                try:
                    await collector_task
                except Exception:
                    logger.exception(
                        "Failed to stop market data collector."
                    )

            if backup_task is not None:
                backup_stop_event.set()

                try:
                    await backup_task
                except Exception:
                    logger.exception(
                        "Failed to stop automatic database backup."
                    )

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

        finally:
            try:
                database.dispose()
            except Exception:
                logger.exception(
                    "Failed to dispose database engine."
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
        chrony_service: ChronyStatusService = (
            application.state.chrony_status_service
        )

        connection = adapter.connection_status()
        chrony = chrony_service.snapshot()

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
            "time_sync": {
                "provider": "chrony",
                "available": chrony.available,
                "synchronized": chrony.synchronized,
                "reference_id": chrony.reference_id,
                "stratum": chrony.stratum,
                "leap_status": chrony.leap_status,
                "last_offset_seconds": chrony.last_offset_seconds,
                "root_dispersion_seconds": chrony.root_dispersion_seconds,
                "selected_source": chrony.selected_source,
                "healthy_sources": chrony.healthy_sources,
                "nts_authenticated_sources": (
                    chrony.nts_authenticated_sources
                ),
                "quorum_met": chrony.quorum_met,
                "error": chrony.error,
                "broker_time_verified": False,
            },
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
        selected_symbol = (
            settings.MT5_SYMBOL if symbol is None else symbol
        ).strip()

        if not selected_symbol:
            raise HTTPException(
                status_code=400,
                detail="A market symbol must be configured or supplied.",
            )

        service: MarketDataService = (
            application.state.market_data_service
        )
        repository: MarketDataRepository = (
            application.state.market_data_repository
        )

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

        if snapshot.analysis_eligible:
            try:
                repository.save_snapshot(snapshot)
            except MarketDataPersistenceConflict as exc:
                logger.error(
                    "Market data persistence conflict: %s",
                    exc,
                )
                raise HTTPException(
                    status_code=409,
                    detail="Stored market data conflicts with the new data.",
                ) from exc

        return snapshot.model_dump(mode="json")

    return application


app = create_app()
