from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, event, inspect, select
from sqlalchemy.orm import Session

from backend.database_models import (
    EvaluationPeriodRecord,
    EvaluationRunRecord,
    EvaluationSampleSelectionRecord,
    EvaluationSignalRecord,
)


UTC = timezone.utc
BASE_EVALUATION_TABLES = {
    "evaluation_runs",
    "evaluation_periods",
    "evaluation_signals",
    "evaluation_outcomes",
}


def _config(project_root: Path, database_url: str) -> Config:
    config = Config(str(project_root / "alembic.ini"))
    config.set_main_option("script_location", str(project_root / "migrations"))
    config.set_main_option("sqlalchemy.url", database_url)
    return config


def _enable_foreign_keys(dbapi_connection, _connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


def test_sample_selection_migration_round_trip_isolated_from_existing_evaluation_tables(
    tmp_path, monkeypatch
):
    database_path = tmp_path / "sample_selection_migration.db"
    database_url = f"sqlite:///{database_path.as_posix()}"

    import backend.config

    monkeypatch.setattr(
        backend.config,
        "get_settings",
        lambda: SimpleNamespace(DATABASE_URL=database_url),
    )
    project_root = Path(__file__).resolve().parents[1]
    config = _config(project_root, database_url)

    command.upgrade(config, "0004_evaluation_foundation")
    command.upgrade(config, "head")

    engine = create_engine(database_url)
    event.listen(engine, "connect", _enable_foreign_keys)
    try:
        inspector = inspect(engine)
        assert "evaluation_sample_selections" in inspector.get_table_names()
        assert BASE_EVALUATION_TABLES.issubset(set(inspector.get_table_names()))
        assert {
            "ix_eval_sample_selection_run_horizon_selected_order",
            "ix_eval_sample_selection_period_horizon",
        }.issubset(
            {
                index["name"]
                for index in inspector.get_indexes("evaluation_sample_selections")
            }
        )
        assert any(
            fk["name"] == "fk_evaluation_sample_selection_signal_scope"
            for fk in inspector.get_foreign_keys("evaluation_sample_selections")
        )

        start = datetime(2026, 1, 1, tzinfo=UTC)
        created = datetime(2026, 11, 21, 8, tzinfo=UTC)
        run = EvaluationRunRecord(
            id="selection-migration-run",
            contract_version="1.0",
            instrument="XAUUSD",
            created_at=created,
            data_start_at=start,
            data_end_at=start + timedelta(days=30),
            code_revision="migration-test",
            configuration_sha256="b" * 64,
            configuration_snapshot={"test": True},
            sampling_rule="FIRST_ELIGIBLE_NON_OVERLAPPING_V1",
            period_partition="THREE_EQUAL_DURATION_CHRONOLOGICAL",
            horizons=[5, 15, 30, 60],
            minimum_completed_outcomes_per_horizon=300,
            minimum_outcomes_per_period=75,
            status="CREATED",
        )
        period = EvaluationPeriodRecord(
            id="selection-migration-period",
            run_id=run.id,
            contract_version="1.0",
            period_index=1,
            start_at=start,
            end_at=start + timedelta(days=10),
            created_at=created,
            partition_method="THREE_EQUAL_DURATION_CHRONOLOGICAL",
        )
        signal = EvaluationSignalRecord(
            id="selection-migration-signal",
            run_id=run.id,
            period_id=period.id,
            contract_version="1.0",
            instrument="XAUUSD",
            signal_at=start + timedelta(hours=1),
            input_available_at=start + timedelta(hours=1),
            direction="BUY",
            reference_eligible=True,
            filter_decision="BLOCKED",
            filter_reason="TEST_FILTER",
            source_candle_ids=[1],
            source_batch_ids=[1],
            input_snapshot=None,
            created_at=created,
        )
        selection = EvaluationSampleSelectionRecord(
            id="selection-migration-row",
            run_id=run.id,
            signal_id=signal.id,
            period_id=period.id,
            horizon_minutes=5,
            candidate_order=1,
            selected=True,
            decision_reason="SELECTED",
        )

        with Session(engine) as session:
            session.add_all([run, period, signal, selection])
            session.commit()
            loaded = session.scalar(
                select(EvaluationSampleSelectionRecord).where(
                    EvaluationSampleSelectionRecord.id == selection.id
                )
            )
            assert loaded is not None
            assert loaded.decision_reason == "SELECTED"
            assert loaded.selected is True
    finally:
        engine.dispose()

    # Downgrading 0005 must remove only its ledger, not the four 0004 tables.
    command.downgrade(config, "0004_evaluation_foundation")
    verify_engine = create_engine(database_url)
    event.listen(verify_engine, "connect", _enable_foreign_keys)
    try:
        tables = set(inspect(verify_engine).get_table_names())
        assert "evaluation_sample_selections" not in tables
        assert BASE_EVALUATION_TABLES.issubset(tables)
        with Session(verify_engine) as session:
            assert session.get(EvaluationRunRecord, "selection-migration-run") is not None
            assert session.get(EvaluationPeriodRecord, "selection-migration-period") is not None
            assert session.get(EvaluationSignalRecord, "selection-migration-signal") is not None
    finally:
        verify_engine.dispose()

    command.upgrade(config, "head")
    final_engine = create_engine(database_url)
    try:
        assert "evaluation_sample_selections" in inspect(final_engine).get_table_names()
    finally:
        final_engine.dispose()
