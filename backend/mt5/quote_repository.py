from __future__ import annotations

from dataclasses import dataclass
from datetime import timezone

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from backend.database_models import BrokerQuoteRecord
from backend.mt5.time_models import BrokerTimeSample


@dataclass(frozen=True)
class BrokerQuoteSaveResult:
    record_id: int
    inserted: bool
    duplicate: bool


class BrokerQuoteRepository:
    """Persist raw broker quotes without converting broker timestamps."""

    def __init__(self, database) -> None:
        # Accept the application's database abstraction and test doubles that
        # implement the same session() contract; validate at first use.
        if not callable(getattr(database, "session", None)):
            raise TypeError("database must provide a session() context manager.")
        self._database = database

    @staticmethod
    def _filters(sample: BrokerTimeSample):
        return (
            BrokerQuoteRecord.symbol == sample.symbol,
            BrokerQuoteRecord.account_login == sample.account_login,
            BrokerQuoteRecord.broker_server == sample.broker_server,
            BrokerQuoteRecord.broker_time_msc == sample.broker_time_msc,
            BrokerQuoteRecord.bid == sample.bid,
            BrokerQuoteRecord.ask == sample.ask,
        )

    def save_sample(self, sample: BrokerTimeSample) -> BrokerQuoteSaveResult:
        if not isinstance(sample, BrokerTimeSample):
            raise TypeError("sample must be a BrokerTimeSample instance.")
        try:
            with self._database.session() as session:
                existing = session.scalar(
                    select(BrokerQuoteRecord).where(*self._filters(sample))
                )
                if existing is not None:
                    return BrokerQuoteSaveResult(existing.id, False, True)
                record = BrokerQuoteRecord(
                    symbol=sample.symbol,
                    broker_time_msc=sample.broker_time_msc,
                    received_at_utc=sample.received_at_utc,
                    received_monotonic_ns=sample.received_monotonic_ns,
                    bid=sample.bid,
                    ask=sample.ask,
                    account_login=sample.account_login,
                    broker_server=sample.broker_server,
                    source=sample.source,
                )
                session.add(record)
                session.flush()
                return BrokerQuoteSaveResult(record.id, True, False)
        except IntegrityError:
            # A concurrent writer may have inserted this exact quote.
            existing = self.get_exact_sample(sample)
            if existing is None:
                raise
            return BrokerQuoteSaveResult(existing.id, False, True)

    def get_exact_sample(self, sample: BrokerTimeSample) -> BrokerQuoteRecord | None:
        if not isinstance(sample, BrokerTimeSample):
            raise TypeError("sample must be a BrokerTimeSample instance.")
        with self._database.session() as session:
            record = session.scalar(
                select(BrokerQuoteRecord).where(*self._filters(sample))
            )
            if record is not None:
                session.expunge(record)
            return record

    def get_latest_sample(
        self, *, symbol: str, account_login: int, broker_server: str
    ) -> BrokerTimeSample | None:
        if not isinstance(symbol, str) or not symbol.strip():
            raise ValueError("symbol must be a non-empty string.")
        if isinstance(account_login, bool) or not isinstance(account_login, int) or account_login <= 0:
            raise ValueError("account_login must be a positive integer.")
        if not isinstance(broker_server, str) or not broker_server.strip():
            raise ValueError("broker_server must be a non-empty string.")
        with self._database.session() as session:
            record = session.scalar(
                select(BrokerQuoteRecord)
                .where(
                    BrokerQuoteRecord.symbol == symbol.strip(),
                    BrokerQuoteRecord.account_login == account_login,
                    BrokerQuoteRecord.broker_server == broker_server.strip(),
                )
                .order_by(
                    BrokerQuoteRecord.received_at_utc.desc(),
                    BrokerQuoteRecord.id.desc(),
                )
                .limit(1)
            )
            if record is None:
                return None
            received_at = record.received_at_utc
            if received_at.tzinfo is None or received_at.utcoffset() is None:
                received_at = received_at.replace(tzinfo=timezone.utc)
            else:
                received_at = received_at.astimezone(timezone.utc)
            return BrokerTimeSample(
                symbol=record.symbol,
                broker_time_msc=record.broker_time_msc,
                received_at_utc=received_at,
                received_monotonic_ns=record.received_monotonic_ns,
                bid=record.bid,
                ask=record.ask,
                account_login=record.account_login,
                broker_server=record.broker_server,
                source=record.source,
            )

    def count_for_identity(
        self, *, symbol: str, account_login: int, broker_server: str
    ) -> int:
        with self._database.session() as session:
            return int(
                session.scalar(
                    select(func.count())
                    .select_from(BrokerQuoteRecord)
                    .where(
                        BrokerQuoteRecord.symbol == symbol,
                        BrokerQuoteRecord.account_login == account_login,
                        BrokerQuoteRecord.broker_server == broker_server,
                    )
                ) or 0
            )
