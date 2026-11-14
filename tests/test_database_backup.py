from datetime import datetime, timezone
import os
from pathlib import Path
import sqlite3
from uuid import uuid4

from sqlalchemy.engine.url import make_url

from backend.database import Database


class DatabaseBackupError(Exception):
    """Raised when a database backup operation fails."""

    pass


class DatabaseBackupService:
    def __init__(
        self,
        database: Database,
        backup_directory: Path | None = None,
    ):
        self._database = database
        
        # استخراج مسار ملف قاعدة البيانات من رابط SQLAlchemy
        url = make_url(str(database.engine.url))
        if url.drivername not in ("sqlite", "sqlite+pysqlite"):
            raise DatabaseBackupError("Only SQLite databases are supported.")
        
        database_file = url.database
        if not database_file or database_file == ":memory:":
            raise DatabaseBackupError(
                "Database backup requires a file-based SQLite database, not in-memory."
            )
        
        self._database_path = Path(database_file)
        
        if not self._database_path.is_file():
            raise DatabaseBackupError(
                "The source database file does not exist or has not been initialized."
            )
            
        if backup_directory is not None:
            self._backup_directory = Path(backup_directory)
        else:
            self._backup_directory = self._database_path.parent / "backups"

    def _write_backup(self, destination_path: Path) -> None:
        """نسخ قاعدة البيانات بشكل آمن باستخدام واجهة النسخ الاحتياطي لـ SQLite."""
        try:
            with sqlite3.connect(self._database_path) as source_conn:
                with sqlite3.connect(destination_path) as dest_conn:
                    source_conn.backup(dest_conn)
        except Exception as exc:
            raise DatabaseBackupError("Failed to write backup file.") from exc

    def _verify_backup(self, backup_path: Path) -> None:
        """التحقق من سلامة ملف النسخ الاحتياطي."""
        try:
            with sqlite3.connect(backup_path) as connection:
                cursor = connection.execute("PRAGMA integrity_check")
                result = cursor.fetchall()
                if result != [("ok",)]:
                    raise DatabaseBackupError("Backup integrity check failed.")
        except Exception as exc:
            if isinstance(exc, DatabaseBackupError):
                raise
            raise DatabaseBackupError("Failed to verify backup database.") from exc

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
