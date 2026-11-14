
from pathlib import Path
import sqlite3

import pytest
from sqlalchemy import text

from backend.database import Database
from backend.database_backup import (
    DatabaseBackupError,
    DatabaseBackupService,
)


def test_create_backup_produces_verified_sqlite_database(tmp_path: Path):
    database_path = tmp_path / "gold_bot.db"
    database = Database(
        f"sqlite:///{database_path.as_posix()}"
    )

    try:
        database.initialize()

        with database.session() as session:
            session.execute(
                text(
                    "CREATE TABLE records "
                    "(id INTEGER PRIMARY KEY, value TEXT NOT NULL)"
                )
            )
            session.execute(
                text(
                    "INSERT INTO records (value) VALUES ('GOLD BOT')"
                )
            )

        service = DatabaseBackupService(database)
        backup_path = service.create_backup()

        assert backup_path.is_file()
        assert backup_path.stat().st_size > 0
        assert backup_path.parent == tmp_path / "backups"

        with sqlite3.connect(backup_path) as connection:
            result = connection.execute(
                "PRAGMA integrity_check"
            ).fetchall()

            value = connection.execute(
                "SELECT value FROM records"
            ).fetchone()

        assert result == [("ok",)]
        assert value == ("GOLD BOT",)

    finally:
        database.dispose()


def test_backup_preserves_existing_database_data(tmp_path: Path):
    database_path = tmp_path / "source.db"
    database = Database(
        f"sqlite:///{database_path.as_posix()}"
    )

    try:
        database.initialize()

        with database.session() as session:
            session.execute(
                text("CREATE TABLE values_table (value INTEGER)")
            )
            session.execute(
                text("INSERT INTO values_table VALUES (100)")
            )

        backup_path = DatabaseBackupService(
            database,
            tmp_path / "custom_backups",
        ).create_backup()

        with sqlite3.connect(backup_path) as connection:
            value = connection.execute(
                "SELECT value FROM values_table"
            ).fetchone()

        assert value == (100,)

    finally:
        database.dispose()


def test_backup_rejects_in_memory_database():
    database = Database("sqlite:///:memory:")

    try:
        database.initialize()

        with pytest.raises(
            DatabaseBackupError,
            match="file-based SQLite",
        ):
            DatabaseBackupService(database)

    finally:
        database.dispose()


def test_backup_requires_initialized_database():
    database = Database("sqlite:///gold_bot.db")

    with pytest.raises(DatabaseBackupError):
        DatabaseBackupService(database)


def test_backup_rejects_missing_source_database(tmp_path: Path):
    database_path = tmp_path / "missing.db"
    database = Database(
        f"sqlite:///{database_path.as_posix()}"
    )

    try:
        database.initialize()

        service = DatabaseBackupService(database)

        with pytest.raises(
            DatabaseBackupError,
            match="source database file does not exist",
        ):
            service.create_backup()

    finally:
        database.dispose()


def test_failed_backup_does_not_leave_temporary_files(
    tmp_path: Path,
    monkeypatch,
):
    database_path = tmp_path / "source.db"
    database = Database(
        f"sqlite:///{database_path.as_posix()}"
    )

    try:
        database.initialize()

        with database.session() as session:
            session.execute(
                text("CREATE TABLE records (value INTEGER)")
            )

        service = DatabaseBackupService(database)

        def fail_write(destination_path):
            destination_path.write_bytes(b"invalid backup")

        monkeypatch.setattr(
            service,
            "_write_backup",
            fail_write,
        )

        with pytest.raises(DatabaseBackupError):
            service.create_backup()

        backup_files = list(
            service._backup_directory.glob("*.sqlite3")
        )
        temporary_files = list(
            service._backup_directory.glob("*.tmp")
        )

        assert backup_files == []
        assert temporary_files == []

    finally:
        database.dispose()
