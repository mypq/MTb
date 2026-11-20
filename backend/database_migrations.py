from __future__ import annotations

from pathlib import Path

from backend.config import get_settings
from alembic import command
from alembic.config import Config


class DatabaseMigrationError(RuntimeError):
    """Raised when database migrations cannot be applied."""


def upgrade_database(database_url: str | None = None) -> None:
    """Apply all pending migrations to the selected database."""

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
        selected_url = database_url or get_settings().DATABASE_URL
        config.set_main_option(
            "sqlalchemy.url",
            selected_url.replace("%", "%%"),
        )

        command.upgrade(config, "head")

    except Exception as exc:
        raise DatabaseMigrationError(
            "Failed to apply database migrations."
        ) from exc
