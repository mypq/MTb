
from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, make_url
from sqlalchemy.orm import Session, sessionmaker


class DatabaseError(RuntimeError):
    """Raised when database initialization or access fails."""


class Database:
    """Owns the SQLAlchemy engine and session lifecycle."""

    def __init__(self, database_url: str) -> None:
        if not isinstance(database_url, str) or not database_url.strip():
            raise DatabaseError("DATABASE_URL must be a non-empty string.")

        self._database_url = database_url.strip()
        self._engine: Engine | None = None
        self._session_factory: sessionmaker[Session] | None = None

    @property
    def engine(self) -> Engine:
        if self._engine is None:
            raise DatabaseError("Database has not been initialized.")
        return self._engine

    def initialize(self) -> None:
        if self._engine is not None:
            return

        try:
            url = make_url(self._database_url)
            self._prepare_sqlite_directory(url)

            connect_args: dict[str, object] = {}

            if url.get_backend_name() == "sqlite":
                connect_args["check_same_thread"] = False

            engine = create_engine(
                url,
                connect_args=connect_args,
                pool_pre_ping=True,
            )

            factory = sessionmaker(
                bind=engine,
                autoflush=False,
                expire_on_commit=False,
            )

            self._engine = engine
            self._session_factory = factory

        except Exception as exc:
            raise DatabaseError(
                "Failed to initialize the database."
            ) from exc

    @staticmethod
    def _prepare_sqlite_directory(url) -> None:
        if url.get_backend_name() != "sqlite":
            return

        database_name = url.database

        if not database_name or database_name == ":memory:":
            return

        database_path = Path(database_name).expanduser()
        parent = database_path.parent

        if str(parent) not in ("", "."):
            parent.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def session(self) -> Iterator[Session]:
        if self._session_factory is None:
            raise DatabaseError("Database has not been initialized.")

        db_session = self._session_factory()

        try:
            yield db_session
            db_session.commit()
        except Exception:
            db_session.rollback()
            raise
        finally:
            db_session.close()

    def dispose(self) -> None:
        if self._engine is None:
            return

        self._engine.dispose()
        self._engine = None
        self._session_factory = None
