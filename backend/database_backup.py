
from __future__ import annotations

import os
import shutil
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock
from uuid import uuid4

from sqlalchemy.engine import make_url

from backend.database import Database


class DatabaseBackupError(RuntimeError):
    """Raised when a database backup or restore operation fails."""


class DatabaseBackupService:
    """Creates verified backups and safely restores the primary SQLite database."""

    BACKUP_RETENTION_LIMIT = 30

    def __init__(
        self,
        database: Database,
        backup_directory: str | Path | None = None,
    ) -> None:
        self._database = database
        self._operation_lock = RLock()

        try:
            url = make_url(str(database.engine.url))
        except Exception as exc:
            raise DatabaseBackupError(
                "Database must be initialized before backup or restore."
            ) from exc

        if url.get_backend_name() != "sqlite":
            raise DatabaseBackupError(
                "Backup and restore currently support SQLite only."
            )

        if not url.database or url.database == ":memory:":
            raise DatabaseBackupError(
                "A file-based SQLite database is required."
            )

        if url.query.get("mode") == "memory":
            raise DatabaseBackupError(
                "In-memory SQLite databases are not supported."
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
        """Create a backup while excluding concurrent restore operations."""
        with self._operation_lock:
            return self._create_backup()

    def _create_backup(self) -> Path:
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
            self._prune_backups()

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

    def restore_backup(self, backup_path: str | Path) -> None:
        """Restore a verified backup while excluding concurrent backups."""
        with self._operation_lock:
            self._restore_backup(backup_path)

    def _restore_backup(self, backup_path: str | Path) -> None:
        """Safely replace the primary database with a verified backup."""

        try:
            selected_backup = Path(
                backup_path
            ).expanduser().resolve()
        except Exception as exc:
            raise DatabaseBackupError(
                "Invalid backup path."
            ) from exc

        if selected_backup == self._database_path:
            raise DatabaseBackupError(
                "The primary database cannot be restored from itself."
            )

        if not selected_backup.is_file():
            raise DatabaseBackupError(
                "The selected backup file does not exist."
            )

        self._verify_backup(selected_backup)

        temporary_path = (
            self._database_path.parent
            / f".{self._database_path.name}.restore.{uuid4().hex}.tmp"
        )

        database_disposed = False

        try:
            self._database_path.parent.mkdir(
                parents=True,
                exist_ok=True,
            )

            with selected_backup.open("rb") as source:
                with temporary_path.open("xb") as destination:
                    shutil.copyfileobj(source, destination)
                    destination.flush()
                    os.fsync(destination.fileno())

            self._verify_backup(temporary_path)

            self._database.dispose()
            database_disposed = True

            os.replace(
                temporary_path,
                self._database_path,
            )

            self._database.initialize()
            database_disposed = False

        except DatabaseBackupError:
            raise
        except Exception as exc:
            raise DatabaseBackupError(
                "Failed to restore the primary database."
            ) from exc
        finally:
            try:
                temporary_path.unlink(missing_ok=True)
            except OSError:
                pass

            if database_disposed:
                try:
                    self._database.initialize()
                except Exception as exc:
                    raise DatabaseBackupError(
                        "Restore failed and the primary database "
                        "could not be reinitialized."
                    ) from exc

    def _prune_backups(self) -> None:
        backups = sorted(
            self._backup_directory.glob("gold_bot_*.sqlite3"),
            key=lambda path: path.name,
            reverse=True,
        )

        valid_backups: list[Path] = []

        for backup_path in backups:
            try:
                self._verify_backup(backup_path)
            except DatabaseBackupError:
                try:
                    backup_path.unlink(missing_ok=True)
                except OSError as exc:
                    raise DatabaseBackupError(
                        f"Failed to remove invalid backup: {backup_path.name}"
                    ) from exc
                continue

            valid_backups.append(backup_path)

        for old_backup in valid_backups[self.BACKUP_RETENTION_LIMIT:]:
            try:
                old_backup.unlink(missing_ok=True)
            except OSError as exc:
                raise DatabaseBackupError(
                    f"Failed to remove expired backup: {old_backup.name}"
                ) from exc

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
