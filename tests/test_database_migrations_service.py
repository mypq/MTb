from pathlib import Path
from unittest.mock import Mock

import pytest

import backend.database_migrations as migrations


def test_upgrade_database_calls_alembic(monkeypatch):
    command = Mock()
    monkeypatch.setattr(migrations, "command", command)

    migrations.upgrade_database()

    command.upgrade.assert_called_once()

    config, revision = command.upgrade.call_args.args

    assert revision == "head"
    assert isinstance(config, migrations.Config)


def test_upgrade_database_rejects_missing_config(monkeypatch):
    monkeypatch.setattr(
        migrations.Path,
        "is_file",
        lambda self: False,
    )

    with pytest.raises(
        migrations.DatabaseMigrationError,
        match="configuration file was not found",
    ):
        migrations.upgrade_database()


def test_upgrade_database_rejects_missing_migrations_directory(
    monkeypatch,
):
    original_is_file = Path.is_file

    monkeypatch.setattr(
        Path,
        "is_file",
        lambda self: (
            True
            if self.name == "alembic.ini"
            else original_is_file(self)
        ),
    )

    monkeypatch.setattr(
        Path,
        "is_dir",
        lambda self: False,
    )

    with pytest.raises(
        migrations.DatabaseMigrationError,
        match="migrations directory was not found",
    ):
        migrations.upgrade_database()


def test_upgrade_database_wraps_alembic_failure(monkeypatch):
    command = Mock()
    command.upgrade.side_effect = RuntimeError("Migration failed")

    monkeypatch.setattr(migrations, "command", command)

    with pytest.raises(
        migrations.DatabaseMigrationError,
        match="Failed to apply database migrations",
    ):
        migrations.upgrade_database()
