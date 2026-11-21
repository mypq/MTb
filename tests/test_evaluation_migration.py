from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, event, inspect, text
from sqlalchemy.orm import Session

from backend.database_models import (
    EvaluationOutcomeRecord,
    EvaluationPeriodRecord,
    EvaluationRunRecord,
    EvaluationSignalRecord,
)


UTC = timezone.utc
EVALUATION_TABLES = {
    "evaluation_runs",
    "evaluation_periods",
    "evaluation_signals",
    "evaluation_outcomes",
}


def _alembic_config(project_root: Path, database_url: str) -> Config:
    config = Config(str(project_root / "alembic.ini"))
    config.set_main_option("script_location", str(project_root / "migrations"))
    config.set_main_option("sqlalchemy.url", database_url)
    return config


def _enable_foreign_keys(dbapi_connection, _connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


def test_evaluation_migration_round_trip_preserves_market_data(tmp_path, monkeypatch):
    database_path = tmp_path / "evaluation_migration.db"
    database_url = f"sqlite:///{database_path.as_posix()}"

    import backend.config

    monkeypatch.setattr(
        backend.config,
        "get_settings",
        lambda: SimpleNamespace(DATABASE_URL=database_url),
    )

    project_root = Path(__file__).resolve().parents[1]
    config = _alembic_config(project_root, database_url)

    # Start at 0003 and seed a real pre-existing market-data row.
    command.upgrade(config, "0003_validation_provenance")
    seed_engine = create_engine(database_url)
    try:
        with seed_engine.begin() as connection:
            batch_id = connection.execute(
                text(
                    """
                    INSERT INTO market_data_batches
                    (symbol, timeframe, source, received_at, candle_count, created_at,
                     clock_verified, clock_account_login, clock_broker_server,
                     clock_terminal_build, clock_checked_at, clock_ntp_offset_seconds,
                     clock_ntp_uncertainty_seconds, clock_ntp_sample_count, clock_ntp_servers,
                     clock_candle_offset_seconds, clock_candle_uncertainty_seconds,
                     clock_calibration_sample_count, validation_report)
                    VALUES
                    ('XAUUSD', 'M5', 'MT5', :received, 1, :created,
                     1, 123456, 'Broker-Demo', 5000, :checked, 0.125, 0.010, 5,
                     '[\"time.google.com\"]', 7200.0, 0.25, 20, :report)
                    """
                ),
                {
                    "received": datetime(2026, 11, 20, 10, tzinfo=UTC),
                    "created": datetime(2026, 11, 20, 10, 0, 1, tzinfo=UTC),
                    "checked": datetime(2026, 11, 20, 10, 0, 2, tzinfo=UTC),
                    "report": '{"accepted":true,"count":1}',
                },
            ).lastrowid
        assert batch_id is not None
    finally:
        seed_engine.dispose()

    command.upgrade(config, "head")

    engine = create_engine(database_url)
    event.listen(engine, "connect", _enable_foreign_keys)
    try:
        inspector = inspect(engine)
        tables = set(inspector.get_table_names())
        assert EVALUATION_TABLES.issubset(tables)

        expected_indexes = {
            "ix_evaluation_runs_instrument_created",
            "ix_evaluation_periods_run_range",
            "ix_evaluation_signals_run_time",
            "ix_evaluation_signals_period_time",
            "ix_evaluation_outcomes_run_horizon_status",
            "ix_evaluation_outcomes_period_horizon",
        }
        actual_indexes = set()
        for table_name in EVALUATION_TABLES:
            actual_indexes.update(
                index["name"] for index in inspector.get_indexes(table_name)
            )
        assert expected_indexes.issubset(actual_indexes)

        assert any(
            fk["name"] == "fk_evaluation_signal_period_run"
            for fk in inspector.get_foreign_keys("evaluation_signals")
        )
        assert any(
            fk["name"] == "fk_evaluation_outcome_signal_scope"
            for fk in inspector.get_foreign_keys("evaluation_outcomes")
        )

        outcome_columns = {
            column["name"]: column
            for column in inspector.get_columns("evaluation_outcomes")
        }
        for name in ("entry_price", "exit_price", "gross_pnl", "net_pnl"):
            assert "CHAR" in str(outcome_columns[name]["type"]).upper() or "TEXT" in str(outcome_columns[name]["type"]).upper()

        start = datetime(2026, 1, 1, tzinfo=UTC)
        created = datetime(2026, 11, 21, 8, tzinfo=UTC)
        run = EvaluationRunRecord(
            id="run-migration-test",
            contract_version="1.0",
            instrument="XAUUSD",
            created_at=created,
            data_start_at=start,
            data_end_at=start + timedelta(days=30),
            code_revision="migration-test",
            configuration_sha256="a" * 64,
            configuration_snapshot={"test": True},
            sampling_rule="NON_OVERLAPPING",
            period_partition="THREE_EQUAL_TIME_PERIODS",
            horizons=[5, 15, 30, 60],
            minimum_completed_outcomes_per_horizon=300,
            minimum_outcomes_per_period=75,
            status="READY",
        )
        period = EvaluationPeriodRecord(
            id="period-migration-test",
            contract_version="1.0",
            period_index=1,
            start_at=start,
            end_at=start + timedelta(days=10),
            created_at=created,
            partition_method="EQUAL_DURATION",
        )
        signal = EvaluationSignalRecord(
            id="signal-migration-test",
            contract_version="1.0",
            instrument="XAUUSD",
            signal_at=start + timedelta(hours=1),
            input_available_at=start + timedelta(hours=1),
            direction="BUY",
            reference_eligible=True,
            filter_decision="ALLOWED",
            source_candle_ids=[1],
            source_batch_ids=[int(batch_id)],
            input_snapshot={"source": "migration-test"},
            created_at=created,
        )
        outcome = EvaluationOutcomeRecord(
            id="outcome-migration-test",
            contract_version="1.0",
            horizon_minutes=5,
            outcome_type="COUNTERFACTUAL",
            direction="BUY",
            status="COMPLETED",
            measurement_basis="BID_ASK_QUOTES",
            entry_at=start + timedelta(hours=1),
            exit_at=start + timedelta(hours=1, minutes=5),
            entry_price=Decimal("2000.123456789012345678"),
            exit_price=Decimal("2001.223456789012345678"),
            entry_price_side="ASK",
            exit_price_side="BID",
            gross_pnl=Decimal("1.100000000000000000"),
            spread_accounting="EMBEDDED_IN_BID_ASK_PRICES",
            spread_cost=None,
            slippage_cost=None,
            slippage_knowledge="NOT_APPLICABLE",
            commission_cost=None,
            commission_knowledge="NOT_APPLICABLE",
            swap_adjustment=None,
            swap_knowledge="NOT_APPLICABLE",
            costs_complete=True,
            net_pnl=Decimal("1.100000000000000000"),
            created_at=created,
        )
        period.run = run
        signal.run = run
        signal.period = period
        outcome.signal = signal

        with Session(engine) as session:
            session.add(run)
            session.add(period)
            session.add(signal)
            session.add(outcome)
            session.commit()
            loaded = session.get(EvaluationOutcomeRecord, "outcome-migration-test")
            assert loaded is not None
            assert loaded.entry_price == Decimal("2000.123456789012345678")
            assert loaded.exit_price == Decimal("2001.223456789012345678")
            assert loaded.net_pnl == Decimal("1.100000000000000000")

        with engine.connect() as connection:
            market_row = connection.execute(
                text(
                    """
                    SELECT symbol, timeframe, clock_broker_server,
                           clock_ntp_offset_seconds, validation_report
                    FROM market_data_batches WHERE id = :id
                    """
                ),
                {"id": batch_id},
            ).mappings().one()
        assert market_row["symbol"] == "XAUUSD"
        assert market_row["timeframe"] == "M5"
        assert market_row["clock_broker_server"] == "Broker-Demo"
        assert market_row["clock_ntp_offset_seconds"] == 0.125
        assert market_row["validation_report"] == '{"accepted":true,"count":1}'

    finally:
        engine.dispose()

    # Downgrade only 0004: market data and its evidence must remain untouched.
    command.downgrade(config, "0003_validation_provenance")
    verify_engine = create_engine(database_url)
    try:
        inspector = inspect(verify_engine)
        tables = set(inspector.get_table_names())
        assert EVALUATION_TABLES.isdisjoint(tables)
        assert {"market_data_batches", "market_candles"}.issubset(tables)
        with verify_engine.connect() as connection:
            preserved = connection.execute(
                text(
                    """
                    SELECT symbol, clock_broker_server, clock_ntp_offset_seconds,
                           validation_report
                    FROM market_data_batches WHERE id = :id
                    """
                ),
                {"id": batch_id},
            ).mappings().one()
        assert preserved["symbol"] == "XAUUSD"
        assert preserved["clock_broker_server"] == "Broker-Demo"
        assert preserved["clock_ntp_offset_seconds"] == 0.125
        assert preserved["validation_report"] == '{"accepted":true,"count":1}'
    finally:
        verify_engine.dispose()

    # Re-upgrade must recreate the four Evaluation tables cleanly.
    command.upgrade(config, "head")
    final_engine = create_engine(database_url)
    try:
        assert EVALUATION_TABLES.issubset(set(inspect(final_engine).get_table_names()))
    finally:
        final_engine.dispose()
