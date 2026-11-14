
from pathlib import Path
import sqlite3

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


def test_restore_rejects_primary_database_as_backup(
    database,
):
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

    corrupt_backup = backup_directory / "gold_bot_00000000T000000000000Z.sqlite3"
    backup_directory.mkdir(parents=True, exist_ok=True)
    corrupt_backup.write_bytes(b"invalid-sqlite-database")

    service.create_backup()

    assert not corrupt_backup.exists()
    assert len(list(backup_directory.glob("gold_bot_*.sqlite3"))) == 1
