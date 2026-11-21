
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text


UTC = timezone.utc

VALIDATION_COLUMNS = {"validation_report"}


CLOCK_COLUMNS = {
    "clock_verified",
    "clock_account_login",
    "clock_broker_server",
    "clock_terminal_build",
    "clock_checked_at",
    "clock_ntp_offset_seconds",
    "clock_ntp_uncertainty_seconds",
    "clock_ntp_sample_count",
    "clock_ntp_servers",
    "clock_candle_offset_seconds",
    "clock_candle_uncertainty_seconds",
    "clock_calibration_sample_count",
}


def make_alembic_config(project_root: Path, database_url: str) -> Config:
    config = Config(str(project_root / "alembic.ini"))

    config.set_main_option(
        "script_location",
        str(project_root / "migrations"),
    )

    config.set_main_option(
        "sqlalchemy.url",
        database_url,
    )

    return config


def test_initial_migration_upgrade_and_downgrade(
    tmp_path,
    monkeypatch,
):
    database_path = tmp_path / "migration_test.db"
    database_url = f"sqlite:///{database_path.as_posix()}"

    import backend.config

    monkeypatch.setattr(
        backend.config,
        "get_settings",
        lambda: SimpleNamespace(DATABASE_URL=database_url),
    )

    project_root = Path(__file__).resolve().parents[1]
    alembic_config = make_alembic_config(
        project_root,
        database_url,
    )

    # First create the database using the original migration only.
    command.upgrade(alembic_config, "0001_market_data")

    engine = create_engine(database_url)

    try:
        inspector = inspect(engine)

        assert "market_data_batches" in inspector.get_table_names()
        assert "market_candles" in inspector.get_table_names()

        old_batch_columns = {
            column["name"]
            for column in inspector.get_columns("market_data_batches")
        }

        # Clock evidence columns must not exist in migration 0001.
        assert CLOCK_COLUMNS.isdisjoint(old_batch_columns)

        # Insert a genuine legacy-format row before migration 0002.
        legacy_received_at = datetime(
            2026,
            11,
            18,
            12,
            0,
            tzinfo=UTC,
        )

        legacy_created_at = datetime(
            2026,
            11,
            18,
            12,
            0,
            1,
            tzinfo=UTC,
        )

        with engine.begin() as connection:
            result = connection.execute(
                text(
                    """
                    INSERT INTO market_data_batches (
                        symbol,
                        timeframe,
                        source,
                        received_at,
                        candle_count,
                        created_at
                    )
                    VALUES (
                        :symbol,
                        :timeframe,
                        :source,
                        :received_at,
                        :candle_count,
                        :created_at
                    )
                    """
                ),
                {
                    "symbol": "XAUUSD",
                    "timeframe": "M5",
                    "source": "MT5",
                    "received_at": legacy_received_at,
                    "candle_count": 1,
                    "created_at": legacy_created_at,
                },
            )

            legacy_batch_id = result.lastrowid

        assert legacy_batch_id is not None

    finally:
        engine.dispose()

    # Upgrade the existing database from 0001 to the latest migration.
    command.upgrade(alembic_config, "head")

    engine = create_engine(database_url)

    try:
        inspector = inspect(engine)

        assert "market_data_batches" in inspector.get_table_names()
        assert "market_candles" in inspector.get_table_names()

        batch_columns = {
            column["name"]
            for column in inspector.get_columns("market_data_batches")
        }

        candle_columns = {
            column["name"]
            for column in inspector.get_columns("market_candles")
        }

        assert {
            "id",
            "symbol",
            "timeframe",
            "source",
            "received_at",
            "candle_count",
            "created_at",
        }.issubset(batch_columns)

        assert CLOCK_COLUMNS.issubset(batch_columns)
        assert VALIDATION_COLUMNS.issubset(batch_columns)

        batch_column_details = {
            column["name"]: column
            for column in inspector.get_columns("market_data_batches")
        }

        for column_name in CLOCK_COLUMNS:
            assert batch_column_details[column_name]["nullable"] is True

        assert {
            "id",
            "batch_id",
            "symbol",
            "timeframe",
            "timestamp",
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "tick_volume",
            "real_volume",
            "volume_source",
            "is_closed",
            "quality",
        }.issubset(candle_columns)

        candle_indexes = {
            index["name"]
            for index in inspector.get_indexes("market_candles")
        }

        assert "ix_market_candles_batch_id" in candle_indexes
        assert (
            "ix_market_candles_symbol_timeframe_timestamp"
            in candle_indexes
        )

        batch_indexes = {
            index["name"]
            for index in inspector.get_indexes("market_data_batches")
        }

        assert (
            "ix_market_data_batches_symbol_timeframe"
            in batch_indexes
        )

        candle_constraints = inspector.get_check_constraints(
            "market_candles"
        )

        constraint_names = {
            constraint["name"]
            for constraint in candle_constraints
        }

        assert "ck_candle_open_positive" in constraint_names
        assert "ck_candle_high_gte_low" in constraint_names
        assert "ck_candle_tick_volume_nonnegative" in constraint_names

        foreign_keys = inspector.get_foreign_keys("market_candles")

        assert any(
            foreign_key["referred_table"] == "market_data_batches"
            and foreign_key["constrained_columns"] == ["batch_id"]
            for foreign_key in foreign_keys
        )

        # The legacy row must survive the migration unchanged.
        with engine.connect() as connection:
            legacy_row = connection.execute(
                text(
                    """
                    SELECT
                        id,
                        symbol,
                        timeframe,
                        source,
                        candle_count,
                        clock_verified,
                        clock_account_login,
                        clock_broker_server,
                        clock_terminal_build,
                        clock_checked_at,
                        clock_ntp_offset_seconds,
                        clock_ntp_uncertainty_seconds,
                        clock_ntp_sample_count,
                        clock_ntp_servers,
                        clock_candle_offset_seconds,
                        clock_candle_uncertainty_seconds,
                        clock_calibration_sample_count,
                        validation_report
                    FROM market_data_batches
                    WHERE id = :batch_id
                    """
                ),
                {"batch_id": legacy_batch_id},
            ).mappings().one()

        assert legacy_row["id"] == legacy_batch_id
        assert legacy_row["symbol"] == "XAUUSD"
        assert legacy_row["timeframe"] == "M5"
        assert legacy_row["source"] == "MT5"
        assert legacy_row["candle_count"] == 1

        # Never invent historical clock or validation evidence for legacy records.
        for column_name in VALIDATION_COLUMNS:
            assert legacy_row[column_name] is None

        # Never invent historical clock evidence for legacy records.
        for column_name in CLOCK_COLUMNS:
            assert legacy_row[column_name] is None

    finally:
        engine.dispose()

    # Downgrade must remove both market-data tables.
    command.downgrade(alembic_config, "base")

    verification_engine = create_engine(database_url)

    try:
        inspector = inspect(verification_engine)
        tables = set(inspector.get_table_names())

        assert "market_data_batches" not in tables
        assert "market_candles" not in tables

    finally:
        verification_engine.dispose()
