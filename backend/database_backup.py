from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from sqlalchemy.engine import make_url

from backend.database import Database


class DatabaseBackupError(RuntimeError):
    """Raised when a database backup cannot be created safely."""


class DatabaseBackupService:
    """Creates verified online backups of a SQLite database."""

    def __init__(
        self,
        database: Database,
        backup_directory: str | Path | None = None,
    ) -> None:
        self._database = database

        try:
            url = make_url(str(database.engine.url))
        except Exception as exc:
            raise DatabaseBackupError(
                "Database must be initialized before backup."
            ) from exc

        if url.get_backend_name() != "sqlite":
            raise DatabaseBackupError(
                "Online backup currently supports SQLite only."
            )

        if not url.database or url.database == ":memory:":
            raise DatabaseBackupError(
                "A file-based SQLite database is required."
            )

        if url.query.get("mode") == "memory":
            raise DatabaseBackupError(
                "In-memory SQLite databases cannot be backed up."
            )

        self._database_path = Path(
            url.database
        ).expanduser().resolve()

        if backup_directory is None:
            self._backup_directory = (
                self._database_path.parent / "backups"
            )
        else:
            self._backup_directory = Path(
                backup_directory
            ).expanduser().resolve()

    def create_backup(self) -> Path:
        """Create and verify one atomic SQLite backup file."""

        if not self._database_path.is_file():
            raise DatabaseBackupError(
                "The source database file does not exist."
            )

        temporary_path: Path | None = None

        try:
            self._backup_directory.mkdir(
                parents=True,
                exist_ok=True,
            )

            timestamp = datetime.now(timezone.utc).strftime(
                "%Y%m%dT%H%M%S%fZ"
            )

            backup_path = (
                self._backup_directory
                / f"gold_bot_{timestamp}.sqlite3"
            )

            temporary_path = (
                self._backup_directory
                / f".{backup_path.name}.{uuid4().hex}.tmp"
            )

            self._write_backup(temporary_path)
            self._verify_backup(temporary_path)

            os.replace(temporary_path, backup_path)

            return backup_path

        except DatabaseBackupError:
            raise
        except Exception as exc:
            raise DatabaseBackupError(
                "Failed to create a verified database backup."
            ) from exc
        finally:
            if temporary_path is not None:
                try:
                    temporary_path.unlink(missing_ok=True)
                except OSError:
                    pass

    def _write_backup(self, destination_path: Path) -> None:
        source_connection = None
        destination_connection = None

        try:
            source_connection = (
                self._database.engine.raw_connection()
            )

            source_sqlite = (
                source_connection.driver_connection
            )

            destination_connection = sqlite3.connect(
                str(destination_path),
                timeout=30.0,
            )

            source_sqlite.backup(
                destination_connection,
                pages=256,
                sleep=0.05,
            )

            destination_connection.commit()

        finally:
            if destination_connection is not None:
                destination_connection.close()

            if source_connection is not None:
                source_connection.close()

    @staticmethod
    def _verify_backup(path: Path) -> None:
        if not path.is_file() or path.stat().st_size == 0:
            raise DatabaseBackupError(
                "Backup file is missing or empty."
            )

        connection = None

        try:
            connection = sqlite3.connect(
                f"file:{path.as_posix()}?mode=ro",
                uri=True,
            )

            result = connection.execute(
                "PRAGMA integrity_check"
            ).fetchall()

            if result != [("ok",)]:
                raise DatabaseBackupError(
                    "SQLite integrity check failed."
                )

        except DatabaseBackupError:
            raise
        except Exception as exc:
            raise DatabaseBackupError(
                "Unable to verify the backup database."
            ) from exc
        finally:
            if connection is not None:
                connection.close()
