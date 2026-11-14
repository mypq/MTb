
from pathlib import Path
import sqlite3
from threading import Event, Thread

import pytest
from sqlalchemy import text

from backend.database import Database
from backend.database_backup import (
    DatabaseBackupError,
    DatabaseBackupService,
)


@pytest.fixture
def database(tmp_path: Path):
    database_path = tmp_path / "gold_bot.db"
    db = Database(f"sqlite:///{database_path.as_posix()}")
    db.initialize()

    try:
        with db.session() as session:
            session.execute(
                text(
                    "CREATE TABLE records "
                    "(id INTEGER PRIMARY KEY, value TEXT NOT NULL)"
                )
            )
            session.execute(
                text(
                    "INSERT INTO records (value) VALUES ('backup-test')"
                )
            )

        yield db
    finally:
        db.dispose()


def test_create_backup_creates_verified_database(database, tmp_path):
    backup_directory = tmp_path / "backups"
    service = DatabaseBackupService(database, backup_directory)

    backup_path = service.create_backup()

    assert backup_path.is_file()
    assert backup_path.parent == backup_directory
    assert backup_path.stat().st_size > 0
    assert backup_path.name.endswith(".sqlite3")

    connection = sqlite3.connect(backup_path)
    try:
        integrity = connection.execute(
            "PRAGMA integrity_check"
        ).fetchall()

        value = connection.execute(
            "SELECT value FROM records"
        ).fetchone()
    finally:
        connection.close()

    assert integrity == [("ok",)]
    assert value == ("backup-test",)


def test_create_backup_does_not_modify_source_database(
    database,
    tmp_path,
):
    service = DatabaseBackupService(
        database,
        tmp_path / "backups",
    )

    service.create_backup()

    with database.session() as session:
        value = session.execute(
            text("SELECT value FROM records")
        ).scalar_one()

    assert value == "backup-test"


def test_failed_backup_does_not_leave_temporary_files(
    database,
    tmp_path,
    monkeypatch,
):
    backup_directory = tmp_path / "backups"
    service = DatabaseBackupService(
        database,
        backup_directory,
    )

    def fail_verification(path):
        raise DatabaseBackupError("forced verification failure")

    monkeypatch.setattr(
        service,
        "_verify_backup",
        fail_verification,
    )

    with pytest.raises(DatabaseBackupError):
        service.create_backup()

    assert list(backup_directory.glob("*.sqlite3")) == []
    assert list(backup_directory.glob("*.tmp")) == []
    assert list(backup_directory.glob(".*.tmp")) == []


def test_backup_rejects_in_memory_database():
    database = Database("sqlite:///:memory:")
    database.initialize()

    try:
        with pytest.raises(DatabaseBackupError):
            DatabaseBackupService(database)
    finally:
        database.dispose()


def test_backup_requires_initialized_database():
    database = Database("sqlite:///:memory:")

    with pytest.raises(DatabaseBackupError):
        DatabaseBackupService(database)


def test_backup_rejects_missing_source_database(tmp_path):
    database_path = tmp_path / "missing.db"
    database = Database(
        f"sqlite:///{database_path.as_posix()}"
    )
    database.initialize()

    try:
        database.engine.dispose()
        database_path.unlink(missing_ok=True)

        service = DatabaseBackupService(
            database,
            tmp_path / "backups",
        )

        with pytest.raises(DatabaseBackupError):
            service.create_backup()
    finally:
        database.dispose()


def test_restore_backup_restores_primary_database(database, tmp_path):
    service = DatabaseBackupService(
        database,
        tmp_path / "backups",
    )

    backup_path = service.create_backup()

    with database.session() as session:
        session.execute(
            text("UPDATE records SET value = 'changed-data'")
        )

    service.restore_backup(backup_path)

    with database.session() as session:
        value = session.execute(
            text("SELECT value FROM records")
        ).scalar_one()

    assert value == "backup-test"


def test_restore_rejects_corrupt_backup_without_changing_primary(
    database,
    tmp_path,
):
    service = DatabaseBackupService(
        database,
        tmp_path / "backups",
    )

    corrupt_backup = tmp_path / "corrupt.sqlite3"
    corrupt_backup.write_bytes(b"not-a-valid-sqlite-database")

    with database.session() as session:
        session.execute(
            text("UPDATE records SET value = 'original-primary'")
        )

    with pytest.raises(DatabaseBackupError):
        service.restore_backup(corrupt_backup)

    with database.session() as session:
        value = session.execute(
            text("SELECT value FROM records")
        ).scalar_one()

    assert value == "original-primary"


def test_restore_rejects_missing_backup(database, tmp_path):
    service = DatabaseBackupService(database)

    missing_backup = tmp_path / "missing.sqlite3"

    with pytest.raises(DatabaseBackupError):
        service.restore_backup(missing_backup)


def test_restore_rejects_primary_database_as_backup(database):
    service = DatabaseBackupService(database)

    with pytest.raises(DatabaseBackupError):
        service.restore_backup(service._database_path)


def test_restore_does_not_leave_temporary_files(
    database,
    tmp_path,
):
    service = DatabaseBackupService(
        database,
        tmp_path / "backups",
    )

    backup_path = service.create_backup()

    with database.session() as session:
        session.execute(
            text("UPDATE records SET value = 'changed-data'")
        )

    service.restore_backup(backup_path)

    temporary_files = list(
        service._database_path.parent.glob(
            f".{service._database_path.name}.restore.*.tmp"
        )
    )

    assert temporary_files == []


def test_backup_retention_keeps_only_latest_30_valid_backups(
    database,
    tmp_path,
):
    service = DatabaseBackupService(
        database,
        tmp_path / "backups",
    )

    backup_paths = [
        service.create_backup()
        for _ in range(31)
    ]

    existing_backups = sorted(
        service._backup_directory.glob("gold_bot_*.sqlite3")
    )

    assert len(existing_backups) == 30
    assert backup_paths[-1].is_file()
    assert not backup_paths[0].exists()


def test_backup_retention_removes_corrupt_backups(
    database,
    tmp_path,
):
    backup_directory = tmp_path / "backups"
    service = DatabaseBackupService(
        database,
        backup_directory,
    )

    corrupt_backup = (
        backup_directory
        / "gold_bot_00000000T000000000000Z.sqlite3"
    )
    backup_directory.mkdir(parents=True, exist_ok=True)
    corrupt_backup.write_bytes(b"invalid-sqlite-database")

    service.create_backup()

    assert not corrupt_backup.exists()
    assert len(list(backup_directory.glob("gold_bot_*.sqlite3"))) == 1


def test_restore_waits_until_backup_finishes(
    database,
    tmp_path,
    monkeypatch,
):
    service = DatabaseBackupService(
        database,
        tmp_path / "backups",
    )

    restore_source = service.create_backup()

    with database.session() as session:
        session.execute(
            text("UPDATE records SET value = 'changed-data'")
        )

    backup_started = Event()
    release_backup = Event()
    restore_started = Event()
    restore_finished = Event()
    errors = []

    original_write_backup = service._write_backup

    def blocking_write_backup(destination_path):
        backup_started.set()
        if not release_backup.wait(timeout=10):
            raise RuntimeError("Timed out waiting to release backup")
        original_write_backup(destination_path)

    monkeypatch.setattr(
        service,
        "_write_backup",
        blocking_write_backup,
    )

    def run_backup():
        try:
            service.create_backup()
        except Exception as exc:
            errors.append(exc)

    def run_restore():
        restore_started.set()
        try:
            service.restore_backup(restore_source)
        except Exception as exc:
            errors.append(exc)
        finally:
            restore_finished.set()

    backup_thread = Thread(target=run_backup)
    restore_thread = Thread(target=run_restore)

    backup_thread.start()

    try:
        assert backup_started.wait(timeout=5)

        restore_thread.start()
        assert restore_started.wait(timeout=5)

        assert not restore_finished.wait(timeout=0.2)
    finally:
        release_backup.set()

    backup_thread.join(timeout=10)
    restore_thread.join(timeout=10)

    assert not backup_thread.is_alive()
    assert not restore_thread.is_alive()
    assert errors == []
    assert restore_finished.is_set()

    with database.session() as session:
        value = session.execute(
            text("SELECT value FROM records")
        ).scalar_one()

    assert value == "backup-test"


def test_backup_waits_until_restore_finishes(
    database,
    tmp_path,
    monkeypatch,
):
    service = DatabaseBackupService(
        database,
        tmp_path / "backups",
    )

    restore_source = service.create_backup()

    with database.session() as session:
        session.execute(
            text("UPDATE records SET value = 'changed-data'")
        )

    restore_started = Event()
    release_restore = Event()
    backup_started = Event()
    backup_finished = Event()
    errors = []

    original_verify = service._verify_backup
    blocked_once = False

    def blocking_verify(path):
        nonlocal blocked_once

        if path == restore_source and not blocked_once:
            blocked_once = True
            restore_started.set()

            if not release_restore.wait(timeout=10):
                raise RuntimeError("Timed out waiting to release restore")

        return original_verify(path)

    monkeypatch.setattr(
        service,
        "_verify_backup",
        blocking_verify,
    )

    def run_restore():
        try:
            service.restore_backup(restore_source)
        except Exception as exc:
            errors.append(exc)

    def run_backup():
        backup_started.set()
        try:
            service.create_backup()
        except Exception as exc:
            errors.append(exc)
        finally:
            backup_finished.set()

    restore_thread = Thread(target=run_restore)
    backup_thread = Thread(target=run_backup)

    restore_thread.start()

    try:
        assert restore_started.wait(timeout=5)

        backup_thread.start()
        assert backup_started.wait(timeout=5)

        assert not backup_finished.wait(timeout=0.2)
    finally:
        release_restore.set()

    restore_thread.join(timeout=10)
    backup_thread.join(timeout=10)

    assert not restore_thread.is_alive()
    assert not backup_thread.is_alive()
    assert errors == []
    assert backup_finished.is_set()

    with database.session() as session:
        value = session.execute(
            text("SELECT value FROM records")
        ).scalar_one()

    assert value == "backup-test"


def test_operation_lock_is_released_after_backup_failure(
    database,
    tmp_path,
    monkeypatch,
):
    service = DatabaseBackupService(
        database,
        tmp_path / "backups",
    )

    restore_source = service.create_backup()

    original_write_backup = service._write_backup
    failed_once = False

    def fail_once(destination_path):
        nonlocal failed_once

        if not failed_once:
            failed_once = True
            raise RuntimeError("forced backup failure")

        original_write_backup(destination_path)

    monkeypatch.setattr(
        service,
        "_write_backup",
        fail_once,
    )

    with pytest.raises(DatabaseBackupError):
        service.create_backup()

    service.restore_backup(restore_source)

    with database.session() as session:
        value = session.execute(
            text("SELECT value FROM records")
        ).scalar_one()

    assert value == "backup-test"

def test_restore_waits_until_active_database_sessions_finish(
    database,
    backup_service,
    tmp_path,
):
    from threading import Event, Thread

    backup_path = backup_service.create_backup()

    session_entered = Event()
    release_session = Event()
    restore_started = Event()
    restore_finished = Event()
    errors = []

    def hold_session():
        try:
            with database.session():
                session_entered.set()
                if not release_session.wait(timeout=5):
                    raise TimeoutError("Session release timed out.")
        except Exception as exc:
            errors.append(exc)

    def restore():
        restore_started.set()
        try:
            backup_service.restore_backup(backup_path)
        except Exception as exc:
            errors.append(exc)
        finally:
            restore_finished.set()

    session_thread = Thread(target=hold_session)
    restore_thread = Thread(target=restore)

    session_thread.start()
    assert session_entered.wait(timeout=5)

    restore_thread.start()
    assert restore_started.wait(timeout=5)

    try:
        assert not restore_finished.wait(timeout=0.2)
    finally:
        release_session.set()
        session_thread.join(timeout=5)
        restore_thread.join(timeout=5)

    assert not session_thread.is_alive()
    assert not restore_thread.is_alive()
    assert not errors
    assert restore_finished.is_set()


def test_new_database_sessions_wait_during_backup_restore(
    database,
    backup_service,
):
    from threading import Event, Thread

    backup_path = backup_service.create_backup()

    maintenance_entered = Event()
    release_maintenance = Event()
    session_started = Event()
    session_entered = Event()
    errors = []

    def hold_maintenance():
        try:
            with database.maintenance():
                maintenance_entered.set()
                if not release_maintenance.wait(timeout=5):
                    raise TimeoutError("Maintenance release timed out.")
        except Exception as exc:
            errors.append(exc)

    def open_session():
        session_started.set()
        try:
            with database.session():
                session_entered.set()
        except Exception as exc:
            errors.append(exc)

    maintenance_thread = Thread(target=hold_maintenance)
    session_thread = Thread(target=open_session)

    maintenance_thread.start()
    assert maintenance_entered.wait(timeout=5)

    session_thread.start()
    assert session_started.wait(timeout=5)

    try:
        assert not session_entered.wait(timeout=0.2)
    finally:
        release_maintenance.set()
        maintenance_thread.join(timeout=5)
        session_thread.join(timeout=5)

    assert not maintenance_thread.is_alive()
    assert not session_thread.is_alive()
    assert not errors
    assert session_entered.is_set()
