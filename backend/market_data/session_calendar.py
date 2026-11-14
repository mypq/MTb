"""Validated reader for the broker's weekly MT5 session bridge export."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


class MarketSessionCalendarError(ValueError):
    """Raised when the broker session schedule is missing or invalid."""


@dataclass(frozen=True, slots=True)
class TradingSession:
    from_seconds: int
    to_seconds: int

    def __post_init__(self) -> None:
        for name, value in (
            ("from_seconds", self.from_seconds),
            ("to_seconds", self.to_seconds),
        ):
            if isinstance(value, bool) or not isinstance(value, int):
                raise MarketSessionCalendarError(
                    f"{name} must be an integer."
                )
            if not 0 <= value <= 86400:
                raise MarketSessionCalendarError(
                    f"{name} must be between 0 and 86400."
                )

        if self.from_seconds == self.to_seconds:
            raise MarketSessionCalendarError(
                "A session cannot have identical start and end times."
            )


@dataclass(frozen=True, slots=True)
class MarketSessionCalendar:
    """Weekly broker sessions using the exported fixed UTC offset.

    Holidays and historical DST transitions are not included.
    """

    symbol: str
    generated_at_utc: datetime
    server_utc_offset_seconds: int
    trade_sessions: tuple[tuple[TradingSession, ...], ...]

    def __post_init__(self) -> None:
        if not isinstance(self.symbol, str) or not self.symbol.strip():
            raise MarketSessionCalendarError("symbol must be non-empty.")

        if (
            not isinstance(self.generated_at_utc, datetime)
            or self.generated_at_utc.tzinfo is None
            or self.generated_at_utc.utcoffset() != timedelta(0)
        ):
            raise MarketSessionCalendarError(
                "generated_at_utc must be UTC-aware."
            )

        offset = self.server_utc_offset_seconds
        if isinstance(offset, bool) or not isinstance(offset, int):
            raise MarketSessionCalendarError(
                "server_utc_offset_seconds must be an integer."
            )
        if abs(offset) > 14 * 60 * 60:
            raise MarketSessionCalendarError(
                "Broker UTC offset is outside the supported range."
            )

        if (
            not isinstance(self.trade_sessions, tuple)
            or len(self.trade_sessions) != 7
        ):
            raise MarketSessionCalendarError(
                "Exactly seven weekday session lists are required."
            )

        if not any(self.trade_sessions):
            raise MarketSessionCalendarError(
                "The weekly schedule contains no trading sessions."
            )

        for sessions in self.trade_sessions:
            if not isinstance(sessions, tuple) or any(
                not isinstance(session, TradingSession)
                for session in sessions
            ):
                raise MarketSessionCalendarError(
                    "Every weekday must contain valid trading sessions."
                )

    @classmethod
    def from_json(cls, content: str) -> "MarketSessionCalendar":
        if not isinstance(content, str) or not content.strip():
            raise MarketSessionCalendarError("Session JSON is empty.")

        try:
            data = json.loads(content)
        except (json.JSONDecodeError, TypeError) as exc:
            raise MarketSessionCalendarError(
                "Session file is not valid JSON."
            ) from exc

        if not isinstance(data, dict):
            raise MarketSessionCalendarError(
                "Session JSON root must be an object."
            )

        required = {
            "schema_version",
            "status",
            "symbol",
            "generated_at_utc_epoch",
            "server_utc_offset_seconds",
            "schedule_kind",
            "days",
        }
        if set(data) != required:
            raise MarketSessionCalendarError(
                "Session JSON has missing or unexpected root fields."
            )

        version = data["schema_version"]
        if isinstance(version, bool) or version != 1:
            raise MarketSessionCalendarError(
                "Unsupported session schema version."
            )

        if data["status"] != "ok":
            raise MarketSessionCalendarError(
                "Broker session export status is not ok."
            )

        if data["schedule_kind"] != "weekly_recurring":
            raise MarketSessionCalendarError(
                "Unsupported broker session schedule kind."
            )

        if not isinstance(data["symbol"], str) or not data["symbol"].strip():
            raise MarketSessionCalendarError(
                "symbol must be a non-empty string."
            )

        epoch = data["generated_at_utc_epoch"]
        offset = data["server_utc_offset_seconds"]

        if isinstance(epoch, bool) or not isinstance(epoch, int) or epoch <= 0:
            raise MarketSessionCalendarError(
                "generated_at_utc_epoch must be a positive integer."
            )

        if isinstance(offset, bool) or not isinstance(offset, int):
            raise MarketSessionCalendarError(
                "server_utc_offset_seconds must be an integer."
            )

        raw_days = data["days"]
        if not isinstance(raw_days, list) or len(raw_days) != 7:
            raise MarketSessionCalendarError(
                "Session export must contain exactly seven days."
            )

        by_day: dict[int, tuple[TradingSession, ...]] = {}

        for item in raw_days:
            if not isinstance(item, dict) or set(item) != {
                "day_of_week", "trade", "quote"
            }:
                raise MarketSessionCalendarError(
                    "A day entry has missing or unexpected fields."
                )

            day = item["day_of_week"]
            if isinstance(day, bool) or not isinstance(day, int):
                raise MarketSessionCalendarError(
                    "day_of_week must be an integer."
                )

            if not 0 <= day <= 6 or day in by_day:
                raise MarketSessionCalendarError(
                    "Weekday values must be unique integers from 0 to 6."
                )

            for key in ("trade", "quote"):
                if not isinstance(item[key], list):
                    raise MarketSessionCalendarError(
                        f"{key} sessions must be an array."
                    )

            sessions = tuple(
                cls._parse_session(session)
                for session in item["trade"]
            )

            for session in item["quote"]:
                cls._parse_session(session)

            by_day[day] = sessions

        if set(by_day) != set(range(7)):
            raise MarketSessionCalendarError(
                "Every weekday from 0 through 6 must be present."
            )

        try:
            generated_at = datetime.fromtimestamp(
                epoch, tz=timezone.utc
            )
        except (OverflowError, OSError, ValueError) as exc:
            raise MarketSessionCalendarError(
                "generated_at_utc_epoch is outside the supported range."
            ) from exc

        return cls(
            symbol=data["symbol"],
            generated_at_utc=generated_at,
            server_utc_offset_seconds=offset,
            trade_sessions=tuple(by_day[day] for day in range(7)),
        )

    @staticmethod
    def _parse_session(value: Any) -> TradingSession:
        if not isinstance(value, dict) or set(value) != {
            "from_seconds", "to_seconds"
        }:
            raise MarketSessionCalendarError(
                "A session must contain from_seconds and to_seconds."
            )

        return TradingSession(
            from_seconds=value["from_seconds"],
            to_seconds=value["to_seconds"],
        )

    @classmethod
    def load(
        cls,
        path: str | Path,
        *,
        max_age_seconds: int = 604800,
        now_utc: datetime | None = None,
    ) -> "MarketSessionCalendar":
        if (
            isinstance(max_age_seconds, bool)
            or not isinstance(max_age_seconds, int)
            or max_age_seconds <= 0
        ):
            raise MarketSessionCalendarError(
                "max_age_seconds must be a positive integer."
            )

        if now_utc is None:
            now_utc = datetime.now(timezone.utc)

        if (
            not isinstance(now_utc, datetime)
            or now_utc.tzinfo is None
            or now_utc.utcoffset() != timedelta(0)
        ):
            raise MarketSessionCalendarError(
                "now_utc must be UTC-aware."
            )

        try:
            content = Path(path).read_text(encoding="utf-8")
        except OSError as exc:
            raise MarketSessionCalendarError(
                "Unable to read the broker session file."
            ) from exc

        calendar = cls.from_json(content)

        age_seconds = (
            now_utc - calendar.generated_at_utc
        ).total_seconds()

        if age_seconds < -300:
            raise MarketSessionCalendarError(
                "Broker session file has a future generation timestamp."
            )

        if age_seconds > max_age_seconds:
            raise MarketSessionCalendarError(
                "Broker session file is stale."
            )

        return calendar

    @staticmethod
    def _mql5_weekday(value: datetime) -> int:
        return (value.weekday() + 1) % 7

    def is_trading_time(self, timestamp_utc: datetime) -> bool:
        if (
            not isinstance(timestamp_utc, datetime)
            or timestamp_utc.tzinfo is None
            or timestamp_utc.utcoffset() != timedelta(0)
        ):
            raise ValueError("timestamp_utc must be UTC-aware.")

        broker_time = timestamp_utc + timedelta(
            seconds=self.server_utc_offset_seconds
        )

        day = self._mql5_weekday(broker_time)
        seconds = (
            broker_time.hour * 3600
            + broker_time.minute * 60
            + broker_time.second
        )

        for session in self.trade_sessions[day]:
            start = session.from_seconds
            end = session.to_seconds

            if start < end and start <= seconds < end:
                return True

            if start > end and seconds >= start:
                return True

        previous_day = (day - 1) % 7

        for session in self.trade_sessions[previous_day]:
            if (
                session.from_seconds > session.to_seconds
                and seconds < session.to_seconds
            ):
                return True

        return False

    def _interval_overlaps_trading_session(
        self,
        interval_start_utc: datetime,
        interval_end_utc: datetime,
    ) -> bool:
        """Check exact overlap with weekly sessions using the broker offset."""

        offset = timedelta(seconds=self.server_utc_offset_seconds)

        start = (
            interval_start_utc + offset
        ).replace(tzinfo=None)

        end = (
            interval_end_utc + offset
        ).replace(tzinfo=None)

        day = start.date() - timedelta(days=1)
        last_day = end.date()

        while day <= last_day:
            weekday = (day.weekday() + 1) % 7

            for session in self.trade_sessions[weekday]:
                session_start = datetime.combine(
                    day,
                    datetime.min.time(),
                ) + timedelta(seconds=session.from_seconds)

                session_end = datetime.combine(
                    day,
                    datetime.min.time(),
                ) + timedelta(seconds=session.to_seconds)

                if session.from_seconds > session.to_seconds:
                    session_end += timedelta(days=1)

                if (
                    session_start < end
                    and session_end > start
                ):
                    return True

            day += timedelta(days=1)

        return False

    def gap_is_confirmed_closed(
        self,
        previous_candle_utc: datetime,
        next_candle_utc: datetime,
        timeframe_seconds: int,
    ) -> bool:
        """True only if every missing candle interval is outside trading."""

        for value in (previous_candle_utc, next_candle_utc):
            if (
                not isinstance(value, datetime)
                or value.tzinfo is None
                or value.utcoffset() != timedelta(0)
            ):
                raise ValueError("Candle timestamps must be UTC-aware.")

        if (
            isinstance(timeframe_seconds, bool)
            or not isinstance(timeframe_seconds, int)
            or timeframe_seconds <= 0
        ):
            raise ValueError(
                "timeframe_seconds must be a positive integer."
            )

        if next_candle_utc <= previous_candle_utc:
            raise ValueError(
                "next candle must be later than previous candle."
            )

        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)

        if (
            previous_candle_utc.microsecond != 0
            or next_candle_utc.microsecond != 0
        ):
            return False

        previous_seconds = int(
            (previous_candle_utc - epoch).total_seconds()
        )
        next_seconds = int(
            (next_candle_utc - epoch).total_seconds()
        )

        if (
            previous_seconds % timeframe_seconds != 0
            or next_seconds % timeframe_seconds != 0
        ):
            return False

        actual_seconds = (
            next_candle_utc - previous_candle_utc
        ).total_seconds()

        if actual_seconds % timeframe_seconds != 0:
            return False

        cursor = previous_candle_utc + timedelta(
            seconds=timeframe_seconds
        )

        while cursor < next_candle_utc:
            interval_end = min(
                cursor + timedelta(seconds=timeframe_seconds),
                next_candle_utc,
            )

            if self._interval_overlaps_trading_session(
                cursor,
                interval_end,
            ):
                return False

            cursor += timedelta(seconds=timeframe_seconds)

        return True
