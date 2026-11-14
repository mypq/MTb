
from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from threading import Condition, RLock, get_ident
from typing import Iterator

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, make_url
from sqlalchemy.orm import Session, sessionmaker


class DatabaseError(RuntimeError):
    """Raised when database initialization or access fails."""


class Database:
    """Owns the SQLAlchemy engine, sessions, and exclusive maintenance."""

    def __init__(self, database_url: str) -> None:
        if not isinstance(database_url, str) or not database_url.strip():
            raise DatabaseError("DATABASE_URL must be a non-empty string.")

        self._database_url = database_url.strip()
        self._engine: Engine | None = None
        self._session_factory: sessionmaker[Session] | None = None

        self._condition = Condition(RLock())
        self._active_sessions = 0
        self._maintenance_active = False
        self._maintenance_owner: int | None = None

    @property
    def engine(self) -> Engine:
        with self._condition:
            if self._engine is None:
                raise DatabaseError("Database has not been initialized.")
            return self._engine

    def initialize(self) -> None:
        with self._condition:
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
        with self._condition:
            while self._maintenance_active:
                self._condition.wait()

            if self._session_factory is None:
                raise DatabaseError(
                    "Database has not been initialized."
                )

            factory = self._session_factory
            self._active_sessions += 1

        db_session = None

        try:
            db_session = factory()

            try:
                yield db_session
                db_session.commit()
            except Exception:
                db_session.rollback()
                raise

        finally:
            try:
                if db_session is not None:
                    db_session.close()
            finally:
                with self._condition:
                    self._active_sessions -= 1
                    self._condition.notify_all()

    @contextmanager
    def maintenance(self) -> Iterator[None]:
        """Block new sessions and wait for active sessions to finish."""

        owner = get_ident()

        with self._condition:
            while self._maintenance_active:
                self._condition.wait()

            self._maintenance_active = True
            self._maintenance_owner = owner

            try:
                while self._active_sessions > 0:
                    self._condition.wait()
            except BaseException:
                self._maintenance_active = False
                self._maintenance_owner = None
                self._condition.notify_all()
                raise

        try:
            yield
        finally:
            with self._condition:
                self._maintenance_active = False
                self._maintenance_owner = None
                self._condition.notify_all()

    def dispose(self) -> None:
        with self._condition:
            if (
                self._maintenance_active
                and self._maintenance_owner != get_ident()
            ):
                raise DatabaseError(
                    "Cannot dispose database during active maintenance."
                )

            if self._engine is None:
                return

            if self._active_sessions > 0:
                raise DatabaseError(
                    "Cannot dispose database while sessions are active."
                )

            engine = self._engine
            self._engine = None
            self._session_factory = None

        engine.dispose()
