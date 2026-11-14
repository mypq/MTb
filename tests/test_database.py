
from pathlib import Path

import pytest
from sqlalchemy import text

from backend.database import Database, DatabaseError


def test_database_requires_non_empty_url():
    with pytest.raises(DatabaseError):
        Database("")

    with pytest.raises(DatabaseError):
        Database("   ")


def test_database_must_be_initialized_before_access():
    database = Database("sqlite:///:memory:")

    with pytest.raises(DatabaseError):
        _ = database.engine

    with pytest.raises(DatabaseError):
        with database.session():
            pass


def test_initialize_creates_sqlite_directory(tmp_path: Path):
    database_path = tmp_path / "storage" / "gold_bot.db"
    database = Database(f"sqlite:///{database_path.as_posix()}")

    try:
        database.initialize()

        assert database_path.parent.is_dir()

        with database.session() as session:
            result = session.execute(text("SELECT 1")).scalar_one()

        assert result == 1
        assert database_path.is_file()
    finally:
        database.dispose()


def test_session_commits_successful_transaction(tmp_path: Path):
    database = Database(
        f"sqlite:///{(tmp_path / 'commit.db').as_posix()}"
    )

    try:
        database.initialize()

        with database.session() as session:
            session.execute(
                text("CREATE TABLE records (value INTEGER NOT NULL)")
            )
            session.execute(
                text("INSERT INTO records (value) VALUES (42)")
            )

        with database.session() as session:
            value = session.execute(
                text("SELECT value FROM records")
            ).scalar_one()

        assert value == 42
    finally:
        database.dispose()


def test_session_rolls_back_failed_transaction(tmp_path: Path):
    database = Database(
        f"sqlite:///{(tmp_path / 'rollback.db').as_posix()}"
    )

    try:
        database.initialize()

        with database.session() as session:
            session.execute(
                text("CREATE TABLE records (value INTEGER NOT NULL)")
            )

        with pytest.raises(RuntimeError, match="force rollback"):
            with database.session() as session:
                session.execute(
                    text("INSERT INTO records (value) VALUES (99)")
                )
                raise RuntimeError("force rollback")

        with database.session() as session:
            count = session.execute(
                text("SELECT COUNT(*) FROM records")
            ).scalar_one()

        assert count == 0
    finally:
        database.dispose()


def test_dispose_releases_database_engine():
    database = Database("sqlite:///:memory:")
    database.initialize()

    database.dispose()

    with pytest.raises(DatabaseError):
        _ = database.engine


def test_dispose_is_safe_when_not_initialized():
    database = Database("sqlite:///:memory:")

    database.dispose()
    database.dispose()
