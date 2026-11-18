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
from backend.market_data.ntp_monitor import NtpVerificationMonitor
from backend.market_data.ntp_time import NtpTimeVerifier
from backend.market_data.repository import (
    MarketDataPersistenceConflict,
    MarketDataRepository,
)
from backend.market_data.service import MarketDataService
from backend.market_data.session_calendar import (
    MarketSessionCalendar,
    MarketSessionCalendarError,
)
from backend.market_data.validator import MarketDataValidator
from backend.market_data.broker_clock import BrokerClockCalibrator
from backend.market_data.broker_candle_clock import (
    BrokerCandleClockCalibrator,
)
from backend.market_data.broker_offset_detector import (
    BrokerOffsetDetector,
)
from backend.market_data.broker_offset_monitor import (
    BrokerOffsetEvidenceMonitor,
)
from backend.market_data.broker_offset_registry import (
    BrokerOffsetRegistry,
)
from backend.market_data.broker_offset_service import (
    BrokerOffsetEvidenceService,
)
from backend.mt5.adapter import MT5Adapter, MT5AdapterError
from backend.mt5.broker_tick_monitor import BrokerTickMonitor
from backend.mt5.broker_tick_worker import BrokerTickMonitorWorker
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


async def _candle_clock_calibration_loop(
    adapter: MT5Adapter,
    calibrator: BrokerCandleClockCalibrator,
    symbol: str,
    stop_event: asyncio.Event,
) -> None:
    """Calibrate MT5 candle timestamps against independently verified UTC."""
    while not stop_event.is_set():
        try:
            sample = await asyncio.to_thread(
                adapter.get_active_candle_clock_sample,
                symbol,
            )

            snapshot = calibrator.observe(**sample)

            if snapshot.status == "VERIFIED":
                logger.info(
                    "Broker candle clock verified: symbol=%s "
                    "server=%s offset_seconds=%s uncertainty_seconds=%s",
                    snapshot.symbol,
                    snapshot.broker_server,
                    snapshot.offset_seconds,
                    snapshot.uncertainty_seconds,
                )
            else:
                logger.info(
                    "Broker candle clock calibration: status=%s "
                    "samples=%d reason=%s",
                    snapshot.status,
                    snapshot.sample_count,
                    snapshot.reason,
                )

        except Exception as exc:
            # A single failed MT5 read is transient evidence, not proof
            # that an already verified broker-clock calibration is wrong.
            # Identity changes and invalid clock evidence are handled by
            # the calibrator when a valid sample is received.
            logger.warning(
                "Broker candle clock sample failed; "
                "keeping the existing calibration: %s: %s",
                type(exc).__name__,
                exc,
            )

        try:
            await asyncio.wait_for(
                stop_event.wait(),
                timeout=1.0,
            )
        except asyncio.TimeoutError:
            continue


async def _run_collector_after_clock_verification(
    collector: MarketDataCollector,
    calibrator: BrokerCandleClockCalibrator,
    stop_event: asyncio.Event,
) -> None:
    """Do not start persistence until the candle clock is verified."""
    while not stop_event.is_set():
        snapshot = calibrator.snapshot()

        if snapshot.status == "VERIFIED":
            logger.info(
                "Starting market-data collector after candle clock verification."
            )

            await collector.run(stop_event)
            return

        try:
            await asyncio.wait_for(
                stop_event.wait(),
                timeout=1.0,
            )
        except asyncio.TimeoutError:
            continue


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()

    adapter = MT5Adapter(settings=settings)

    broker_offset_registry = BrokerOffsetRegistry(
        getattr(
            settings,
            "BROKER_OFFSET_REGISTRY_FILE",
            "data/broker_offsets.jsonl",
        )
    )

    adapter.set_broker_offset_registry(
        broker_offset_registry
    )

    monitor = MT5ConnectionMonitor(
        adapter=adapter,
        interval_seconds=10.0,
    )

    broker_tick_monitor = BrokerTickMonitor(
        stale_after_seconds=15.0,
    )

    broker_tick_worker = None

    chrony_service = ChronyStatusService()

    ntp_verifier = NtpTimeVerifier()

    ntp_verification_monitor = NtpVerificationMonitor(
        verifier=ntp_verifier,
        interval_seconds=60 * 60,
    )

    broker_clock_calibrator = BrokerClockCalibrator(
        ntp_verifier,
        required_samples=5,
        max_samples=9,
        minimum_span_seconds=3.0,
        maximum_spread_seconds=0.5,
        maximum_offset_change_seconds=1.0,
    )

    broker_candle_clock_calibrator = BrokerCandleClockCalibrator(
        ntp_verifier,
        required_samples=5,
        minimum_span_seconds=3.0,
        maximum_spread_seconds=0.5,
        maximum_sample_age_seconds=30.0,
    )

    adapter.set_candle_clock_calibrator(
        broker_candle_clock_calibrator
    )

    # Broker-offset evidence pipeline.
    # This pipeline records candidates only. It does not authorize
    # offsets for candle timestamps, market calendars, or trading.
    broker_offset_service = None
    broker_offset_monitor = None

    clock_history_file = getattr(
        settings,
        "MT5_CLOCK_HISTORY_FILE",
        None,
    )

    if clock_history_file:
        broker_offset_detector = BrokerOffsetDetector(
            ntp_verifier,
        )

        broker_offset_service = BrokerOffsetEvidenceService(
            clock_history_file,
            broker_offset_detector,
            broker_offset_registry,
        )

        broker_offset_monitor = BrokerOffsetEvidenceMonitor(
            broker_offset_service,
            interval_seconds=(
                settings.BROKER_OFFSET_POLL_INTERVAL_SECONDS
            ),
        )

        logger.info(
            "Broker offset evidence pipeline configured: "
            "history_file=%s registry_file=%s "
            "interval_seconds=%s authorization=DISABLED",
            clock_history_file,
            settings.BROKER_OFFSET_REGISTRY_FILE,
            settings.BROKER_OFFSET_POLL_INTERVAL_SECONDS,
        )
    else:
        logger.warning(
            "Broker offset evidence monitor is disabled: "
            "MT5_CLOCK_HISTORY_FILE is not configured."
        )

    ntp_stop_event = asyncio.Event()
    ntp_task = None

    candle_clock_stop_event = asyncio.Event()
    candle_clock_task = None

    broker_offset_stop_event = asyncio.Event()
    broker_offset_task = None

    database = Database(settings.DATABASE_URL)
    market_data_repository = MarketDataRepository(database)

    # Load the broker session calendar when a file is configured.
    # A configured but missing or invalid file must stop startup.
    sessions_file = getattr(
        settings,
        "MT5_SESSIONS_FILE",
        None,
    )

    session_calendar = None

    if sessions_file:
        try:
            session_calendar = MarketSessionCalendar.load(
                sessions_file,
                max_age_seconds=getattr(
                    settings,
                    "MT5_SESSIONS_MAX_AGE_SECONDS",
                    604800,
                ),
            )
        except MarketSessionCalendarError:
            logger.exception(
                "Configured MT5 session calendar is invalid "
                "or unavailable."
            )
            raise RuntimeError(
                "Cannot start with an invalid MT5 session calendar."
            ) from None

        logger.info(
            "Broker session calendar loaded: symbol=%s file=%s",
            session_calendar.symbol,
            sessions_file,
        )
    else:
        logger.warning(
            "No MT5 session calendar configured. "
            "Unconfirmed market-data gaps will remain blocked."
        )

    market_data_validator = MarketDataValidator(
        session_calendar=session_calendar
    )

    market_data_service = MarketDataService(
        source=adapter,
        validator=market_data_validator,
    )

    backup_service = None
    backup_stop_event = asyncio.Event()
    backup_task = None

    collector = None
    collector_stop_event = asyncio.Event()
    collector_task = None

    startup_stop_event = asyncio.Event()
    startup_task = None

    app.state.settings = settings
    app.state.mt5_adapter = adapter
    app.state.mt5_monitor = monitor
    app.state.broker_tick_monitor = broker_tick_monitor
    app.state.broker_tick_worker = None
    app.state.market_data_service = market_data_service
    app.state.market_data_repository = market_data_repository
    app.state.market_data_collector = None
    app.state.chrony_status_service = chrony_service
    app.state.ntp_verification_monitor = ntp_verification_monitor
    app.state.broker_clock_calibrator = broker_clock_calibrator
    app.state.broker_candle_clock_calibrator = (
        broker_candle_clock_calibrator
    )
    app.state.broker_offset_service = broker_offset_service
    app.state.broker_offset_monitor = broker_offset_monitor
    app.state.database = database
    app.state.database_backup_service = None

    try:
        database.initialize()
        upgrade_database()

        ntp_task = asyncio.create_task(
            ntp_verification_monitor.run(ntp_stop_event),
            name="independent-ntp-verification",
        )

        # Start broker evidence collection only when its history
        # source has explicitly been configured.
        if broker_offset_monitor is not None:
            broker_offset_task = asyncio.create_task(
                broker_offset_monitor.run(
                    broker_offset_stop_event
                ),
                name="broker-offset-evidence-monitor",
            )

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
            # A connection exception is a startup failure.
            # A DISCONNECTED status is not: the monitor will recover it.
            initial_status = adapter.connect()

            monitor.start()

            selected_symbol = settings.MT5_SYMBOL.strip()

            if selected_symbol:

                def start_market_data_workers():
                    nonlocal broker_tick_worker
                    nonlocal collector
                    nonlocal collector_task
                    nonlocal candle_clock_task

                    if broker_tick_worker is not None:
                        return

                    broker_tick_worker = BrokerTickMonitorWorker(
                        adapter=adapter,
                        monitor=broker_tick_monitor,
                        symbol=selected_symbol,
                        interval_seconds=1.0,
                        clock_calibrator=broker_clock_calibrator,
                    )

                    app.state.broker_tick_worker = (
                        broker_tick_worker
                    )

                    broker_tick_worker.start()

                    candle_clock_task = asyncio.create_task(
                        _candle_clock_calibration_loop(
                            adapter,
                            broker_candle_clock_calibrator,
                            selected_symbol,
                            candle_clock_stop_event,
                        ),
                        name="broker-candle-clock-calibration",
                    )

                    logger.info(
                        "Broker tick monitoring enabled: "
                        "symbol=%s interval=1.0 seconds "
                        "clock_verification=UNVERIFIED",
                        selected_symbol,
                    )

                    collector = MarketDataCollector(
                        service=market_data_service,
                        repository=market_data_repository,
                        symbol=selected_symbol,
                        timeframe=settings.DEFAULT_TIMEFRAME,
                        count=300,
                    )

                    app.state.market_data_collector = collector

                    collector_task = asyncio.create_task(
                        _run_collector_after_clock_verification(
                            collector,
                            broker_candle_clock_calibrator,
                            collector_stop_event,
                        ),
                        name="market-data-collector-after-clock-verification",
                    )

                    logger.info(
                        "Automatic market data collection enabled: "
                        "symbol=%s timeframe=%s schedule=candle_close+2s",
                        selected_symbol,
                        settings.DEFAULT_TIMEFRAME,
                    )

                initial_state = getattr(
                    initial_status.state,
                    "value",
                    initial_status.state,
                )

                if initial_state == "CONNECTED":
                    start_market_data_workers()
                else:

                    async def start_market_data_when_connected():
                        while not startup_stop_event.is_set():
                            try:
                                status = await asyncio.to_thread(
                                    adapter.connection_status
                                )

                                state = getattr(
                                    status.state,
                                    "value",
                                    status.state,
                                )

                                if state == "CONNECTED":
                                    break

                            except Exception:
                                logger.exception(
                                    "Failed to check MT5 connection "
                                    "before starting market-data workers."
                                )

                            try:
                                await asyncio.wait_for(
                                    startup_stop_event.wait(),
                                    timeout=0.1,
                                )
                            except asyncio.TimeoutError:
                                pass

                        if startup_stop_event.is_set():
                            return

                        start_market_data_workers()

                    startup_task = asyncio.create_task(
                        start_market_data_when_connected(),
                        name="mt5-market-data-startup",
                    )

                    logger.info(
                        "Market-data workers are waiting for "
                        "MT5 connection recovery."
                    )

            else:
                logger.warning(
                    "Automatic market data collection and broker "
                    "tick monitoring skipped: MT5_SYMBOL is empty."
                )

        yield

    finally:
        try:
            startup_stop_event.set()
            broker_offset_stop_event.set()
            ntp_stop_event.set()
            candle_clock_stop_event.set()

            if broker_offset_task is not None:
                try:
                    await broker_offset_task
                except Exception:
                    logger.exception(
                        "Failed to stop broker offset evidence monitor."
                    )

            if ntp_task is not None:
                try:
                    await ntp_task
                except Exception:
                    logger.exception(
                        "Failed to stop independent NTP verification."
                    )

            if startup_task is not None:
                try:
                    await startup_task
                except Exception:
                    logger.exception(
                        "Failed to stop MT5 market-data startup coordinator."
                    )

            if candle_clock_task is not None:
                try:
                    await candle_clock_task
                except Exception:
                    logger.exception(
                        "Failed to stop broker candle clock calibration."
                    )

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

            broker_tick_worker_stopped = True

            if broker_tick_worker is not None:
                try:
                    broker_tick_worker_stopped = (
                        broker_tick_worker.stop()
                    )
                except Exception:
                    logger.exception(
                        "Failed to stop broker tick monitor worker."
                    )
                    broker_tick_worker_stopped = False

            monitor_stopped = False

            try:
                monitor_stopped = monitor.stop()
            except Exception:
                logger.exception(
                    "Failed to stop MT5 connection monitor."
                )

            if (
                broker_tick_worker_stopped is True
                and monitor_stopped is True
            ):
                try:
                    adapter.shutdown()
                except Exception:
                    logger.exception(
                        "Failed to shut down MT5 adapter."
                    )
                    raise
            else:
                logger.critical(
                    "MT5 adapter shutdown skipped because one or "
                    "more MT5 monitoring workers may still be running. "
                    "Manual recovery is required."
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
        ntp_verification_monitor: NtpVerificationMonitor = (
            application.state.ntp_verification_monitor
        )

        broker_tick_worker = getattr(
            application.state,
            "broker_tick_worker",
            None,
        )

        broker_offset_monitor = getattr(
            application.state,
            "broker_offset_monitor",
            None,
        )

        connection = adapter.connection_status()
        chrony = chrony_service.snapshot()

        if broker_tick_worker is not None:
            broker_tick_status = broker_tick_worker.status()
        else:
            broker_tick_status = {
                "running": False,
                "state": "NO_DATA",
                "clock_verification": "UNVERIFIED",
                "symbol": None,
                "account_login": None,
                "broker_server": None,
                "last_broker_time_msc": None,
                "seconds_since_progress": None,
                "samples_received": 0,
                "timestamp_advances": 0,
                "repeated_timestamps": 0,
                "timestamp_regressions": 0,
                "identity_changes": 0,
                "last_error": None,
            }

        if broker_offset_monitor is not None:
            offset_snapshot = broker_offset_monitor.snapshot()

            broker_offset_status = {
                "enabled": True,
                "running": broker_offset_monitor.is_running,
                "state": offset_snapshot.state,
                "samples_processed": offset_snapshot.samples_processed,
                "last_decision": (
                    offset_snapshot.last_decision.status.value
                    if offset_snapshot.last_decision is not None
                    else None
                ),
                "last_error": offset_snapshot.error,
                "candidate_recorded": (
                    offset_snapshot.last_record is not None
                ),
                "authorization": "DISABLED",
            }
        else:
            broker_offset_status = {
                "enabled": False,
                "running": False,
                "state": "DISABLED",
                "samples_processed": 0,
                "last_decision": None,
                "last_error": None,
                "candidate_recorded": False,
                "authorization": "DISABLED",
            }

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
            "broker_tick_monitor": broker_tick_status,
            "broker_offset_evidence": broker_offset_status,
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
            "ntp_verification": ntp_verification_monitor.snapshot(),
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
