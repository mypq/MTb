from pathlib import Path
from types import SimpleNamespace

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect


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

    alembic_config = Config(
        str(project_root / "alembic.ini")
    )

    alembic_config.set_main_option(
        "script_location",
        str(project_root / "migrations"),
    )

    alembic_config.set_main_option(
        "sqlalchemy.url",
        database_url,
    )

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

    finally:
        engine.dispose()

    command.downgrade(alembic_config, "base")

    verification_engine = create_engine(database_url)

    try:
        inspector = inspect(verification_engine)
        tables = set(inspector.get_table_names())

        assert "market_data_batches" not in tables
        assert "market_candles" not in tables

    finally:
        verification_engine.dispose()
