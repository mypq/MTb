from __future__ import annotations

from pathlib import Path

from alembic import command
from alembic.config import Config


class DatabaseMigrationError(RuntimeError):
    """Raised when database migrations cannot be applied."""


def upgrade_database() -> None:
    """Apply all pending database migrations."""

    project_root = Path(__file__).resolve().parents[1]
    config_path = project_root / "alembic.ini"
    migrations_path = project_root / "migrations"

    if not config_path.is_file():
        raise DatabaseMigrationError(
            "Alembic configuration file was not found."
        )

    if not migrations_path.is_dir():
        raise DatabaseMigrationError(
            "Database migrations directory was not found."
        )

    try:
        config = Config(str(config_path))

        config.set_main_option(
            "script_location",
            str(migrations_path),
        )

        command.upgrade(config, "head")

    except Exception as exc:
        raise DatabaseMigrationError(
            "Failed to apply database migrations."
        ) from exc
