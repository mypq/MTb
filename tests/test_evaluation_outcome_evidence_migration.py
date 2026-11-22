from pathlib import Path
from types import SimpleNamespace

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect


def _config(project_root: Path, database_url: str) -> Config:
    config = Config(str(project_root / "alembic.ini"))
    config.set_main_option("script_location", str(project_root / "migrations"))
    config.set_main_option("sqlalchemy.url", database_url)
    return config


def test_evaluation_outcome_evidence_migration_isolated_round_trip(
    tmp_path, monkeypatch
):
    database_path = tmp_path / "outcome_evidence_migration.db"
    database_url = f"sqlite:///{database_path.as_posix()}"

    import backend.config

    monkeypatch.setattr(
        backend.config,
        "get_settings",
        lambda: SimpleNamespace(DATABASE_URL=database_url),
    )
    project_root = Path(__file__).resolve().parents[1]
    config = _config(project_root, database_url)

    command.upgrade(config, "0005_evaluation_sample_selections")
    command.upgrade(config, "head")

    engine = create_engine(database_url)
    try:
        inspector = inspect(engine)
        tables = set(inspector.get_table_names())
        assert "evaluation_outcome_evidence" in tables
        assert {
            "evaluation_runs",
            "evaluation_periods",
            "evaluation_signals",
            "evaluation_outcomes",
            "evaluation_sample_selections",
        }.issubset(tables)

        columns = {
            item["name"] for item in inspector.get_columns("evaluation_outcome_evidence")
        }
        assert {
            "outcome_id",
            "instrument",
            "timeframe",
            "requested_start_at",
            "requested_end_at",
            "available_at_cutoff",
            "latest_available_at",
            "source_candles",
            "observed_gaps",
            "duplicate_event_times",
            "is_contiguous",
            "evidence_schema_version",
            "evidence_sha256",
        }.issubset(columns)
        assert any(
            fk["name"] == "fk_eval_outcome_evidence_outcome"
            for fk in inspector.get_foreign_keys("evaluation_outcome_evidence")
        )
        with engine.connect() as connection:
            trigger_names = {
                row[0] for row in connection.exec_driver_sql(
                    "SELECT name FROM sqlite_master WHERE type = 'trigger'"
                ).all()
            }
        assert "trg_eval_outcome_evidence_immutable_update" in trigger_names
        with engine.connect() as connection:
            connection.connection.driver_connection.execute("PRAGMA foreign_keys=OFF")
            transaction = connection.begin()
            try:
                connection.exec_driver_sql(
                    "INSERT INTO evaluation_outcome_evidence "
                    "(outcome_id, instrument, timeframe, requested_start_at, requested_end_at, "
                    "available_at_cutoff, latest_available_at, source_candles, observed_gaps, "
                    "duplicate_event_times, is_contiguous, evidence_schema_version, evidence_sha256) "
                    "VALUES ('trigger-test', 'XAUUSD', 'M1', '2026-01-01', '2026-01-02', "
                    "'2026-01-01', NULL, '[]', '[]', '[]', 1, 1, '"
                    + "0" * 64 + "')"
                )
                import pytest
                from sqlalchemy.exc import IntegrityError
                with pytest.raises(IntegrityError):
                    connection.exec_driver_sql(
                        "UPDATE evaluation_outcome_evidence SET is_contiguous = 0 "
                        "WHERE outcome_id = 'trigger-test'"
                    )
            finally:
                transaction.rollback()
        assert "ix_eval_outcome_evidence_cutoff" in {
            item["name"] for item in inspector.get_indexes("evaluation_outcome_evidence")
        }
    finally:
        engine.dispose()

    command.downgrade(config, "0005_evaluation_sample_selections")
    verify_engine = create_engine(database_url)
    try:
        tables = set(inspect(verify_engine).get_table_names())
        assert "evaluation_outcome_evidence" not in tables
        assert "evaluation_outcomes" in tables
        assert "evaluation_sample_selections" in tables
    finally:
        verify_engine.dispose()

    command.upgrade(config, "head")
    final_engine = create_engine(database_url)
    try:
        assert "evaluation_outcome_evidence" in inspect(final_engine).get_table_names()
    finally:
        final_engine.dispose()
