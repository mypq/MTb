from datetime import datetime, timezone

import pytest

from backend.database import Database
from backend.database_models import DatabaseBase
from backend.mt5.quote_repository import BrokerQuoteRepository
from backend.mt5.time_models import BrokerTimeSample


def make_sample(*, timestamp=1795406052640, bid=3603.30, ask=3603.87):
    return BrokerTimeSample(
        symbol="GOLD",
        broker_time_msc=timestamp,
        received_at_utc=datetime(2026, 11, 23, 1, 54, 12, tzinfo=timezone.utc),
        received_monotonic_ns=123456789,
        bid=bid,
        ask=ask,
        account_login=12345678,
        broker_server="XMGlobal-MT5 9",
    )


@pytest.fixture
def repository(tmp_path):
    database = Database(f"sqlite:///{(tmp_path / 'quotes.db').as_posix()}")
    database.initialize()
    DatabaseBase.metadata.create_all(database.engine)
    yield BrokerQuoteRepository(database)
    database.dispose()


def test_quote_is_persisted_and_read_back(repository):
    sample = make_sample()
    saved = repository.save_sample(sample)
    loaded = repository.get_latest_sample(
        symbol="GOLD", account_login=12345678, broker_server="XMGlobal-MT5 9"
    )
    assert saved.inserted is True
    assert saved.duplicate is False
    assert loaded is not None
    assert loaded.symbol == sample.symbol
    assert loaded.broker_time_msc == sample.broker_time_msc
    assert loaded.received_at_utc == sample.received_at_utc
    assert loaded.received_monotonic_ns == sample.received_monotonic_ns
    assert loaded.bid == sample.bid
    assert loaded.ask == sample.ask
    assert loaded.account_login == sample.account_login
    assert loaded.broker_server == sample.broker_server


def test_exact_repeated_quote_is_idempotent(repository):
    sample = make_sample()
    first = repository.save_sample(sample)
    second = repository.save_sample(sample)
    assert first.inserted is True
    assert second.inserted is False
    assert second.duplicate is True
    assert second.record_id == first.record_id
    assert repository.count_for_identity(
        symbol="GOLD", account_login=12345678, broker_server="XMGlobal-MT5 9"
    ) == 1


def test_distinct_quote_at_same_broker_millisecond_is_preserved(repository):
    first = repository.save_sample(make_sample())
    second = repository.save_sample(make_sample(bid=3603.31, ask=3603.88))
    assert first.inserted is True
    assert second.inserted is True
    assert repository.count_for_identity(
        symbol="GOLD", account_login=12345678, broker_server="XMGlobal-MT5 9"
    ) == 2


def test_repository_rejects_non_sample_input(repository):
    with pytest.raises(TypeError, match="BrokerTimeSample"):
        repository.save_sample(object())
