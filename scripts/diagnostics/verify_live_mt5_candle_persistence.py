from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


# Make the repository root importable when this diagnostic is launched by file path.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.config import get_settings
from backend.database import Database
from backend.database_migrations import upgrade_database
from backend.market_data.broker_candle_clock import (
    BrokerCandleClockCalibrator,
)
from backend.market_data.broker_offset_registry import BrokerOffsetRegistry
from backend.market_data.collector import MarketDataCollector
from backend.market_data.ntp_time import NtpTimeVerifier
from backend.market_data.repository import MarketDataRepository
from backend.market_data.service import MarketDataService
from backend.market_data.session_calendar import (
    MarketSessionCalendar,
    MarketSessionCalendarError,
)
from backend.market_data.validator import MarketDataValidator
from backend.mt5.adapter import MT5Adapter
from backend.mt5.timeframes import resolve_mt5_timeframe


UTC = timezone.utc
CALIBRATION_SAMPLES = 5
CALIBRATION_INTERVAL_SECONDS = 1.0
CANDLE_COUNT = 5


def main() -> int:
    settings = get_settings()

    if not settings.MT5_ENABLED:
        raise RuntimeError("MT5_ENABLED must be true in .env.")

    symbol = settings.MT5_SYMBOL.strip()
    if not symbol:
        raise RuntimeError("MT5_SYMBOL is empty in .env.")

    timeframe = settings.DEFAULT_TIMEFRAME

    # 1. Verify independent UTC using live NTP servers.
    verifier = NtpTimeVerifier()
    ntp = verifier.verify()

    if not ntp.verified:
        raise RuntimeError(
            f"Live NTP verification failed: {ntp.error}"
        )

    print(
        "NTP VERIFIED: "
        f"samples={ntp.sample_count}, "
        f"offset={ntp.offset_seconds}, "
        f"uncertainty={ntp.uncertainty_seconds}"
    )

    calibrator = BrokerCandleClockCalibrator(
        verifier,
        required_samples=CALIBRATION_SAMPLES,
        minimum_span_seconds=3.0,
        maximum_spread_seconds=0.5,
        maximum_sample_age_seconds=30.0,
    )

    adapter = MT5Adapter(settings=settings)
    database = Database(settings.DATABASE_URL)

    try:
        # 2. Connect to the actual local MT5 terminal.
        connection = adapter.connect()
        connection_state = getattr(
            connection.state,
            "value",
            connection.state,
        )

        if connection_state != "CONNECTED":
            raise RuntimeError(
                f"MT5 connection failed: {connection.message}"
            )

        adapter.set_candle_clock_calibrator(calibrator)

        # Use the same append-only historical evidence registry as production.
        # The live calibration offset is never substituted for historical records.
        offset_registry = BrokerOffsetRegistry(
            settings.BROKER_OFFSET_REGISTRY_FILE
        )
        adapter.set_broker_offset_registry(offset_registry)

        # 3. Calibrate broker candle time from real M1/M5 boundaries.
        for number in range(CALIBRATION_SAMPLES):
            sample = adapter.get_active_candle_clock_sample(symbol)
            clock = calibrator.observe(**sample)

            print(
                f"CALIBRATION {number + 1}/{CALIBRATION_SAMPLES}: "
                f"status={clock.status}, "
                f"samples={clock.sample_count}, "
                f"offset={clock.offset_seconds}, "
                f"reason={clock.reason}"
            )

            if clock.status == "DEGRADED":
                raise RuntimeError(
                    f"Clock calibration degraded: {clock.reason}"
                )

            if number + 1 < CALIBRATION_SAMPLES:
                time.sleep(CALIBRATION_INTERVAL_SECONDS)

        clock = calibrator.snapshot()

        if (
            clock.status != "VERIFIED"
            or clock.offset_seconds is None
            or clock.symbol != symbol
        ):
            raise RuntimeError(
                "Clock calibration did not reach VERIFIED: "
                f"{clock.reason}"
            )

        # 4. Prepare the same database and validation components
        #    used by the production application.
        database.initialize()
        upgrade_database()

        session_calendar = None

        if settings.MT5_SESSIONS_FILE:
            try:
                session_calendar = MarketSessionCalendar.load(
                    settings.MT5_SESSIONS_FILE,
                    max_age_seconds=(
                        settings.MT5_SESSIONS_MAX_AGE_SECONDS
                    ),
                )
            except MarketSessionCalendarError as exc:
                raise RuntimeError(
                    f"Session calendar is invalid: {exc}"
                ) from exc

        repository = MarketDataRepository(database)

        validator = MarketDataValidator(
            session_calendar=session_calendar
        )

        service = MarketDataService(
            source=adapter,
            validator=validator,
        )

        collector = MarketDataCollector(
            service=service,
            repository=repository,
            symbol=symbol,
            timeframe=timeframe,
            count=CANDLE_COUNT,
        )

        # 5. Independently capture raw MT5 timestamps immediately
        #    before the production collector fetches and saves candles.
        import MetaTrader5 as mt5

        mt5_timeframe = resolve_mt5_timeframe(mt5, timeframe)

        raw_rates = mt5.copy_rates_from_pos(
            symbol,
            mt5_timeframe,
            1,
            CANDLE_COUNT,
        )

        if raw_rates is None or len(raw_rates) != CANDLE_COUNT:
            received = 0 if raw_rates is None else len(raw_rates)
            raise RuntimeError(
                "MT5 did not return the requested closed candles: "
                f"expected={CANDLE_COUNT}, received={received}, "
                f"last_error={mt5.last_error()}"
            )

        account = mt5.account_info()
        terminal = mt5.terminal_info()

        if account is None or terminal is None:
            raise RuntimeError(
                "MT5 account or terminal identity is unavailable."
            )

        identity = (
            int(account.login),
            str(account.server).strip(),
            symbol,
            int(terminal.build),
        )
        raw_epochs = tuple(int(rate["time"]) for rate in raw_rates)
        historical_records = offset_registry.for_broker_times(
            identity,
            raw_epochs,
        )

        if any(record is None for record in historical_records):
            unresolved = [
                raw_epoch
                for raw_epoch, record in zip(raw_epochs, historical_records)
                if record is None
            ]
            raise RuntimeError(
                "Historical broker-offset evidence does not cover every "
                "requested MT5 candle. No live offset fallback is permitted. "
                f"Unresolved raw timestamps: {unresolved}"
            )

        expected_pairs = sorted(
            (
                (
                    raw_epoch,
                    raw_epoch - record.offset_seconds,
                    record.offset_seconds,
                    record.record_sequence,
                )
                for raw_epoch, record in zip(
                    raw_epochs,
                    historical_records,
                )
            ),
            key=lambda item: item[1],
        )
        expected_utc_epochs = [item[1] for item in expected_pairs]

        if any(value <= 0 for value in expected_utc_epochs):
            raise RuntimeError(
                "MT5 produced an invalid expected UTC timestamp."
            )

        # 6. Run the real collector and persist through the repository.
        save_result = collector.collect_once()

        if save_result is None:
            raise RuntimeError(
                "The production collector rejected the live candle batch. "
                "No persistence was accepted; inspect validation logs."
            )

        # 7. Read the persisted candles back from the database.
        stored = repository.get_history(
            symbol=symbol,
            timeframe=timeframe,
            count=CANDLE_COUNT,
        )

        if len(stored) != CANDLE_COUNT:
            raise RuntimeError(
                "Database readback count mismatch: "
                f"expected={CANDLE_COUNT}, received={len(stored)}"
            )

        stored_utc_epochs = []

        for candle in stored:
            if (
                candle.timestamp.tzinfo is None
                or candle.timestamp.utcoffset() is None
                or candle.timestamp.utcoffset().total_seconds() != 0
            ):
                raise RuntimeError(
                    "A stored candle timestamp is not UTC-aware."
                )

            stored_utc_epochs.append(
                int(candle.timestamp.timestamp())
            )

        # This is the critical independent comparison:
        # raw MT5 epoch - dynamically calibrated offset == DB readback.
        if stored_utc_epochs != expected_utc_epochs:
            raise RuntimeError(
                "Persisted timestamps do not match independently "
                "converted MT5 timestamps. "
                f"expected_utc={expected_utc_epochs}, "
                f"stored_utc={stored_utc_epochs}. "
                "A candle boundary may have changed during collection; "
                "if so, rerun the test once."
            )

        account = adapter.get_account_info()

        report = {
            "status": "PASSED",
            "checked_at_utc": datetime.now(UTC).isoformat(),
            "symbol": symbol,
            "timeframe": timeframe,
            "broker_server": account.server,
            "account_identity_verified": True,
            "ntp": {
                "verified": ntp.verified,
                "sample_count": ntp.sample_count,
                "offset_seconds": ntp.offset_seconds,
                "uncertainty_seconds": ntp.uncertainty_seconds,
                "servers": list(ntp.servers),
            },
            "candle_clock": {
                "status": clock.status,
                "offset_seconds": clock.offset_seconds,
                "uncertainty_seconds": clock.uncertainty_seconds,
                "sample_count": clock.sample_count,
                "reason": clock.reason,
            },
            "persistence": {
                "batch_id": save_result.batch_id,
                "inserted_candles": save_result.inserted_candles,
                "duplicate_candles": save_result.duplicate_candles,
                "readback_count": len(stored),
            },
            "candles": [
                {
                    "raw_mt5_epoch": raw_epoch,
                    "historical_offset_seconds": historical_offset,
                    "registry_record_sequence": record_sequence,
                    "expected_utc_epoch": expected_epoch,
                    "stored_utc_epoch": stored_epoch,
                    "stored_timestamp_utc": candle.timestamp.isoformat(),
                }
                for (raw_epoch, expected_epoch, historical_offset, record_sequence), stored_epoch, candle in zip(
                    expected_pairs,
                    stored_utc_epochs,
                    stored,
                )
            ],
        }

        output_path = (
            Path(__file__).resolve().parents[2]
            / "diagnostics"
            / "mt5_live_candle_persistence.json"
        )

        output_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        output_path.write_text(
            json.dumps(report, indent=2),
            encoding="utf-8",
        )

        print("\nLIVE MT5 CANDLE PERSISTENCE RESULT")
        print(json.dumps(report, indent=2))
        print(f"\nEvidence file: {output_path}")

        return 0

    finally:
        database.dispose()
        adapter.shutdown()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(
            "LIVE MT5 CANDLE PERSISTENCE FAILED: "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        raise SystemExit(1)
