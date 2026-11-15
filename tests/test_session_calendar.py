from datetime import datetime, timedelta, timezone
import json

import pytest

from backend.market_data.session_calendar import (
    MarketSessionCalendar,
    MarketSessionCalendarError,
    TradingSession,
)

UTC = timezone.utc


def make_calendar(
    *,
    offset: int = 0,
    monday_sessions: tuple[TradingSession, ...] = (
        TradingSession(3600, 82800),
    ),
    tuesday_sessions: tuple[TradingSession, ...] = (),
    friday_sessions: tuple[TradingSession, ...] = (),
) -> MarketSessionCalendar:
    sessions = (
        (),  # Sunday
        monday_sessions,
        tuesday_sessions,
        (),  # Wednesday
        (),  # Thursday
        friday_sessions,
        (),  # Saturday
    )

    return MarketSessionCalendar(
        symbol="GOLD",
        generated_at_utc=datetime(2026, 1, 1, tzinfo=UTC),
        server_utc_offset_seconds=offset,
        trade_sessions=sessions,
    )


def make_export() -> str:
    return json.dumps(
        {
            "schema_version": 1,
            "status": "ok",
            "symbol": "GOLD",
            "generated_at_utc_epoch": 1767225600,
            "server_utc_offset_seconds": 7200,
            "schedule_kind": "weekly_recurring",
            "days": [
                {
                    "day_of_week": day,
                    "trade": (
                        [{"from_seconds": 3720, "to_seconds": 86280}]
                        if day in (1, 2, 3, 4, 5)
                        else []
                    ),
                    "quote": [],
                }
                for day in range(7)
            ],
        }
    )


def test_valid_export_is_parsed():
    calendar = MarketSessionCalendar.from_json(make_export())

    assert calendar.symbol == "GOLD"
    assert calendar.server_utc_offset_seconds == 7200
    assert len(calendar.trade_sessions) == 7
    assert calendar.trade_sessions[1] == (
        TradingSession(3720, 86280),
    )


def test_load_reads_export_file(tmp_path):
    path = tmp_path / "sessions.json"
    path.write_text(make_export(), encoding="utf-8")

    calendar = MarketSessionCalendar.load(
        path,
        now_utc=datetime(2026, 1, 1, tzinfo=UTC),
    )

    assert calendar.symbol == "GOLD"


def test_stale_session_file_is_rejected(tmp_path):
    path = tmp_path / "sessions.json"
    path.write_text(make_export(), encoding="utf-8")

    with pytest.raises(
        MarketSessionCalendarError,
        match="stale",
    ):
        MarketSessionCalendar.load(
            path,
            max_age_seconds=3600,
            now_utc=datetime(2026, 1, 2, tzinfo=UTC),
        )


def test_future_session_file_is_rejected(tmp_path):
    path = tmp_path / "sessions.json"
    path.write_text(make_export(), encoding="utf-8")

    with pytest.raises(
        MarketSessionCalendarError,
        match="future",
    ):
        MarketSessionCalendar.load(
            path,
            now_utc=datetime(2025, 12, 31, 23, 54, tzinfo=UTC),
        )


def test_session_file_with_small_future_clock_skew_is_accepted(tmp_path):
    path = tmp_path / "sessions.json"
    path.write_text(make_export(), encoding="utf-8")

    calendar = MarketSessionCalendar.load(
        path,
        now_utc=datetime(2025, 12, 31, 23, 58, tzinfo=UTC),
    )

    assert calendar.symbol == "GOLD"


@pytest.mark.parametrize("max_age", [0, -1, True, 1.5, "3600"])
def test_invalid_max_age_is_rejected(tmp_path, max_age):
    path = tmp_path / "sessions.json"
    path.write_text(make_export(), encoding="utf-8")

    with pytest.raises(MarketSessionCalendarError):
        MarketSessionCalendar.load(
            path,
            max_age_seconds=max_age,
            now_utc=datetime(2026, 1, 1, tzinfo=UTC),
        )


def test_naive_now_utc_is_rejected(tmp_path):
    path = tmp_path / "sessions.json"
    path.write_text(make_export(), encoding="utf-8")

    with pytest.raises(MarketSessionCalendarError):
        MarketSessionCalendar.load(
            path,
            now_utc=datetime(2026, 1, 1),
        )


@pytest.mark.parametrize(
    "content",
    [
        "",
        "not-json",
        "[]",
        "{}",
    ],
)
def test_invalid_export_is_rejected(content):
    with pytest.raises(MarketSessionCalendarError):
        MarketSessionCalendar.from_json(content)


def test_unsupported_schema_version_is_rejected():
    data = json.loads(make_export())
    data["schema_version"] = 2

    with pytest.raises(MarketSessionCalendarError):
        MarketSessionCalendar.from_json(json.dumps(data))


def test_non_ok_export_status_is_rejected():
    data = json.loads(make_export())
    data["status"] = "error"

    with pytest.raises(MarketSessionCalendarError):
        MarketSessionCalendar.from_json(json.dumps(data))


def test_wrong_schedule_kind_is_rejected():
    data = json.loads(make_export())
    data["schedule_kind"] = "one_time"

    with pytest.raises(MarketSessionCalendarError):
        MarketSessionCalendar.from_json(json.dumps(data))


def test_wrong_symbol_type_is_rejected():
    data = json.loads(make_export())
    data["symbol"] = 123

    with pytest.raises(MarketSessionCalendarError):
        MarketSessionCalendar.from_json(json.dumps(data))


def test_missing_weekday_is_rejected():
    data = json.loads(make_export())
    data["days"].pop()

    with pytest.raises(MarketSessionCalendarError):
        MarketSessionCalendar.from_json(json.dumps(data))


def test_duplicate_weekday_is_rejected():
    data = json.loads(make_export())
    data["days"][1]["day_of_week"] = 0

    with pytest.raises(MarketSessionCalendarError):
        MarketSessionCalendar.from_json(json.dumps(data))


def test_invalid_session_values_are_rejected():
    data = json.loads(make_export())
    data["days"][1]["trade"] = [
        {"from_seconds": 90000, "to_seconds": 95000}
    ]

    with pytest.raises(MarketSessionCalendarError):
        MarketSessionCalendar.from_json(json.dumps(data))


def test_missing_session_file_is_rejected(tmp_path):
    with pytest.raises(MarketSessionCalendarError):
        MarketSessionCalendar.load(tmp_path / "missing.json")


def test_empty_weekly_trading_schedule_is_rejected():
    with pytest.raises(MarketSessionCalendarError):
        MarketSessionCalendar(
            symbol="GOLD",
            generated_at_utc=datetime(2026, 1, 1, tzinfo=UTC),
            server_utc_offset_seconds=0,
            trade_sessions=((), (), (), (), (), (), ()),
        )


def test_is_trading_time_uses_broker_utc_offset():
    calendar = make_calendar(
        offset=7200,
        monday_sessions=(TradingSession(9 * 3600, 17 * 3600),),
    )

    timestamp = datetime(2026, 1, 5, 7, 0, tzinfo=UTC)

    assert calendar.is_trading_time(timestamp) is True


def test_is_trading_time_excludes_session_end():
    calendar = make_calendar(
        monday_sessions=(TradingSession(9 * 3600, 17 * 3600),),
    )

    timestamp = datetime(2026, 1, 5, 17, 0, tzinfo=UTC)

    assert calendar.is_trading_time(timestamp) is False


def test_is_trading_time_handles_overnight_session():
    calendar = make_calendar(
        monday_sessions=(TradingSession(22 * 3600, 2 * 3600),),
    )

    timestamp = datetime(2026, 1, 6, 1, 0, tzinfo=UTC)

    assert calendar.is_trading_time(timestamp) is True


def test_weekend_gap_is_confirmed_closed():
    calendar = make_calendar(
        friday_sessions=(TradingSession(3600, 82800),),
    )

    previous = datetime(2026, 1, 2, 23, 0, tzinfo=UTC)
    next_candle = datetime(2026, 1, 5, 1, 0, tzinfo=UTC)

    assert calendar.gap_is_confirmed_closed(
        previous,
        next_candle,
        300,
    ) is True


def test_gap_overlapping_trading_session_is_not_confirmed_closed():
    calendar = make_calendar(
        friday_sessions=(TradingSession(3600, 84600),),
    )

    previous = datetime(2026, 1, 2, 23, 0, tzinfo=UTC)
    next_candle = datetime(2026, 1, 2, 23, 40, tzinfo=UTC)

    assert calendar.gap_is_confirmed_closed(
        previous,
        next_candle,
        300,
    ) is False


def test_gap_ending_exactly_at_session_open_is_closed():
    calendar = make_calendar(
        monday_sessions=(TradingSession(9 * 3600, 17 * 3600),),
    )

    previous = datetime(2026, 1, 5, 8, 50, tzinfo=UTC)
    next_candle = datetime(2026, 1, 5, 9, 0, tzinfo=UTC)

    assert calendar.gap_is_confirmed_closed(
        previous,
        next_candle,
        300,
    ) is True


def test_gap_inside_overnight_session_is_not_confirmed_closed():
    calendar = make_calendar(
        monday_sessions=(TradingSession(22 * 3600, 2 * 3600),),
    )

    previous = datetime(2026, 1, 5, 23, 0, tzinfo=UTC)
    next_candle = datetime(2026, 1, 6, 0, 0, tzinfo=UTC)

    assert calendar.gap_is_confirmed_closed(
        previous,
        next_candle,
        300,
    ) is False


def test_non_aligned_gap_is_not_confirmed_closed():
    calendar = make_calendar()

    previous = datetime(2026, 1, 5, 8, 0, tzinfo=UTC)
    next_candle = previous + timedelta(seconds=650)

    assert calendar.gap_is_confirmed_closed(
        previous,
        next_candle,
        300,
    ) is False


@pytest.mark.parametrize(
    "timeframe_seconds",
    [0, -1, True, 300.0, "300"],
)
def test_invalid_timeframe_is_rejected(timeframe_seconds):
    calendar = make_calendar()

    previous = datetime(2026, 1, 5, 8, 0, tzinfo=UTC)
    next_candle = datetime(2026, 1, 5, 8, 10, tzinfo=UTC)

    with pytest.raises(ValueError):
        calendar.gap_is_confirmed_closed(
            previous,
            next_candle,
            timeframe_seconds,
        )


def test_naive_candle_timestamp_is_rejected():
    calendar = make_calendar()

    with pytest.raises(ValueError):
        calendar.gap_is_confirmed_closed(
            datetime(2026, 1, 5, 8, 0),
            datetime(2026, 1, 5, 8, 10, tzinfo=UTC),
            300,
        )


def test_next_candle_must_be_later():
    calendar = make_calendar()
    timestamp = datetime(2026, 1, 5, 8, 0, tzinfo=UTC)

    with pytest.raises(ValueError):
        calendar.gap_is_confirmed_closed(
            timestamp,
            timestamp,
            300,
        )


def test_misaligned_previous_candle_is_not_confirmed_closed():
    calendar = make_calendar()

    previous = datetime(2026, 1, 5, 8, 1, tzinfo=UTC)
    next_candle = datetime(2026, 1, 5, 8, 11, tzinfo=UTC)

    assert calendar.gap_is_confirmed_closed(
        previous,
        next_candle,
        300,
    ) is False
