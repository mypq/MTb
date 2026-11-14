
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

from threading import Event, Thread


def test_maintenance_waits_for_active_sessions(tmp_path: Path):
    database = Database(
        f"sqlite:///{(tmp_path / 'maintenance.db').as_posix()}"
    )
    database.initialize()

    session_entered = Event()
    release_session = Event()
    maintenance_attempted = Event()
    maintenance_entered = Event()
    errors = []

    def hold_session():
        try:
            with database.session():
                session_entered.set()
                if not release_session.wait(timeout=10):
                    raise RuntimeError("Session release timed out")
        except Exception as exc:
            errors.append(exc)

    def enter_maintenance():
        maintenance_attempted.set()
        try:
            with database.maintenance():
                maintenance_entered.set()
        except Exception as exc:
            errors.append(exc)

    session_thread = Thread(target=hold_session)
    maintenance_thread = Thread(target=enter_maintenance)

    try:
        session_thread.start()
        assert session_entered.wait(timeout=5)

        maintenance_thread.start()
        assert maintenance_attempted.wait(timeout=5)

        assert not maintenance_entered.wait(timeout=0.2)

        release_session.set()

        assert maintenance_entered.wait(timeout=5)

        session_thread.join(timeout=5)
        maintenance_thread.join(timeout=5)

        assert not session_thread.is_alive()
        assert not maintenance_thread.is_alive()
        assert errors == []
    finally:
        release_session.set()
        session_thread.join(timeout=5)
        if maintenance_thread.ident is not None:
            maintenance_thread.join(timeout=5)
        database.dispose()


def test_new_sessions_wait_during_maintenance(tmp_path: Path):
    database = Database(
        f"sqlite:///{(tmp_path / 'blocked-sessions.db').as_posix()}"
    )
    database.initialize()

    session_attempted = Event()
    session_entered = Event()
    errors = []

    def open_session():
        session_attempted.set()
        try:
            with database.session():
                session_entered.set()
        except Exception as exc:
            errors.append(exc)

    session_thread = None

    try:
        with database.maintenance():
            session_thread = Thread(target=open_session)
            session_thread.start()

            assert session_attempted.wait(timeout=5)
            assert not session_entered.wait(timeout=0.2)

        assert session_entered.wait(timeout=5)

        session_thread.join(timeout=5)

        assert not session_thread.is_alive()
        assert errors == []
    finally:
        if session_thread is not None:
            session_thread.join(timeout=5)
        database.dispose()


def test_maintenance_releases_after_exception(tmp_path: Path):
    database = Database(
        f"sqlite:///{(tmp_path / 'maintenance-error.db').as_posix()}"
    )
    database.initialize()

    try:
        with pytest.raises(RuntimeError, match="forced maintenance failure"):
            with database.maintenance():
                raise RuntimeError("forced maintenance failure")

        with database.session() as session:
            result = session.execute(text("SELECT 1")).scalar_one()

        assert result == 1
    finally:
        database.dispose()


def test_dispose_rejects_active_sessions(tmp_path: Path):
    database = Database(
        f"sqlite:///{(tmp_path / 'active-session.db').as_posix()}"
    )
    database.initialize()

    session_entered = Event()
    release_session = Event()
    errors = []

    def hold_session():
        try:
            with database.session():
                session_entered.set()
                if not release_session.wait(timeout=10):
                    raise RuntimeError("Session release timed out")
        except Exception as exc:
            errors.append(exc)

    thread = Thread(target=hold_session)

    try:
        thread.start()
        assert session_entered.wait(timeout=5)

        with pytest.raises(DatabaseError, match="sessions are active"):
            database.dispose()

        release_session.set()
        thread.join(timeout=5)

        assert not thread.is_alive()
        assert errors == []

        database.dispose()

        with pytest.raises(DatabaseError):
            _ = database.engine
    finally:
        release_session.set()
        thread.join(timeout=5)
        database.dispose()
def test_dispose_rejects_active_maintenance(tmp_path: Path):
    database = Database(
        f"sqlite:///{(tmp_path / 'active-maintenance.db').as_posix()}"
    )
    database.initialize()

    try:
        with database.maintenance():
            with pytest.raises(DatabaseError):
                database.dispose()

            assert database.engine is not None

        with database.session() as session:
            result = session.execute(text("SELECT 1")).scalar_one()

        assert result == 1
    finally:
        database.dispose()
