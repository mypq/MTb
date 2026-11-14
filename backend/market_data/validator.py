
from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field

from backend.market_data.models import (
    CandleBatch,
    CandleQuality,
    VolumeSource,
)
from backend.market_data.session_calendar import MarketSessionCalendar


class MarketDataIssueCode(str, Enum):
    FUTURE_CANDLE = "FUTURE_CANDLE"
    FUTURE_RECEIVE_TIME = "FUTURE_RECEIVE_TIME"
    UNCLOSED_CANDLE = "UNCLOSED_CANDLE"
    UNVERIFIED_CANDLE = "UNVERIFIED_CANDLE"
    UNKNOWN_VOLUME_SOURCE = "UNKNOWN_VOLUME_SOURCE"
    TIME_GAP = "TIME_GAP"
    SESSION_CLOSED_GAP = "SESSION_CLOSED_GAP"
    SESSION_CALENDAR_SYMBOL_MISMATCH = "SESSION_CALENDAR_SYMBOL_MISMATCH"
    STALE_DATA = "STALE_DATA"
    FUTURE_EXPECTED_CLOSE = "FUTURE_EXPECTED_CLOSE"


class MarketDataIssue(BaseModel):
    model_config = ConfigDict(frozen=True)

    code: MarketDataIssueCode
    message: str = Field(min_length=1)
    candle_timestamp: datetime | None = None
    details: dict[str, int | float | str] = Field(default_factory=dict)


class MarketDataGap(BaseModel):
    model_config = ConfigDict(frozen=True)

    previous_candle: datetime
    next_candle: datetime
    expected_interval_seconds: int = Field(gt=0)
    actual_interval_seconds: int = Field(gt=0)
    missing_intervals: int = Field(ge=1)
    confirmed_closed: bool = False


class MarketDataValidationReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    symbol: str
    timeframe: str
    source: str
    checked_at: datetime

    candle_count: int = Field(ge=1)

    structurally_valid: bool
    analysis_eligible: bool
    volume_data_available: bool

    expected_latest_close_at: datetime
    latest_candle_close_delay_seconds: float
    data_age_seconds: float

    freshness_eligible: bool
    freshness_verified: bool = False

    issues: tuple[MarketDataIssue, ...] = ()
    gaps: tuple[MarketDataGap, ...] = ()


class MarketDataValidator:
    """
    Conservative market-data validation.

    A configured broker session calendar may classify missing candle
    intervals as confirmed market closures. Unclassified gaps remain
    blocking. The calendar never repairs or fabricates candle data.
    """

    MAX_STALENESS_TIMEFRAME_MULTIPLIER = 2

    def __init__(
        self,
        session_calendar: MarketSessionCalendar | None = None,
    ) -> None:
        if (
            session_calendar is not None
            and not isinstance(session_calendar, MarketSessionCalendar)
        ):
            raise TypeError(
                "session_calendar must be a MarketSessionCalendar or None."
            )

        self._session_calendar = session_calendar

    def validate(
        self,
        batch: CandleBatch,
        timeframe_seconds: int,
        checked_at: datetime | None = None,
    ) -> MarketDataValidationReport:
        if isinstance(timeframe_seconds, bool) or not isinstance(
            timeframe_seconds, int
        ):
            raise TypeError("timeframe_seconds must be an integer.")

        if timeframe_seconds <= 0:
            raise ValueError("timeframe_seconds must be greater than zero.")

        validation_time = checked_at or datetime.now(timezone.utc)

        if (
            validation_time.tzinfo is None
            or validation_time.utcoffset() is None
            or validation_time.utcoffset().total_seconds() != 0
        ):
            raise ValueError("checked_at must be a UTC-aware datetime.")

        validation_time = validation_time.astimezone(timezone.utc)

        issues: list[MarketDataIssue] = []
        gaps: list[MarketDataGap] = []

        structurally_valid = True
        volume_data_available = True
        calendar_matches_symbol = False

        if self._session_calendar is not None:
            calendar_matches_symbol = (
                self._session_calendar.symbol.strip().casefold()
                == batch.symbol.strip().casefold()
            )

            if not calendar_matches_symbol:
                structurally_valid = False
                issues.append(
                    MarketDataIssue(
                        code=MarketDataIssueCode.SESSION_CALENDAR_SYMBOL_MISMATCH,
                        message=(
                            "The configured broker session calendar symbol "
                            "does not match the market-data batch symbol."
                        ),
                        details={
                            "calendar_symbol": self._session_calendar.symbol,
                            "batch_symbol": batch.symbol,
                        },
                    )
                )

        if batch.received_at > validation_time:
            structurally_valid = False
            issues.append(
                MarketDataIssue(
                    code=MarketDataIssueCode.FUTURE_RECEIVE_TIME,
                    message="Batch receive time is later than validation time.",
                    details={
                        "received_at": batch.received_at.isoformat(),
                        "checked_at": validation_time.isoformat(),
                    },
                )
            )

        candles = batch.candles
        latest_candle = candles[-1]

        expected_latest_close_at = datetime.fromtimestamp(
            latest_candle.timestamp.timestamp() + timeframe_seconds,
            tz=timezone.utc,
        )

        latest_candle_close_delay_seconds = (
            validation_time - expected_latest_close_at
        ).total_seconds()

        data_age_seconds = (
            validation_time - batch.received_at
        ).total_seconds()

        max_staleness_seconds = (
            timeframe_seconds * self.MAX_STALENESS_TIMEFRAME_MULTIPLIER
        )

        freshness_eligible = (
            0 <= latest_candle_close_delay_seconds
            <= max_staleness_seconds
        )

        if latest_candle_close_delay_seconds < 0:
            issues.append(
                MarketDataIssue(
                    code=MarketDataIssueCode.FUTURE_EXPECTED_CLOSE,
                    message=(
                        "The expected close time of the latest candle "
                        "is later than the validation time."
                    ),
                    candle_timestamp=latest_candle.timestamp,
                    details={
                        "delay_seconds": latest_candle_close_delay_seconds,
                        "expected_close_at": (
                            expected_latest_close_at.isoformat()
                        ),
                    },
                )
            )

        elif latest_candle_close_delay_seconds > max_staleness_seconds:
            issues.append(
                MarketDataIssue(
                    code=MarketDataIssueCode.STALE_DATA,
                    message=(
                        "The latest candle exceeds the maximum permitted "
                        "data delay and is blocked from analysis."
                    ),
                    candle_timestamp=latest_candle.timestamp,
                    details={
                        "delay_seconds": latest_candle_close_delay_seconds,
                        "maximum_delay_seconds": max_staleness_seconds,
                        "timeframe_seconds": timeframe_seconds,
                    },
                )
            )

        for candle in candles:
            if candle.timestamp > validation_time:
                structurally_valid = False
                issues.append(
                    MarketDataIssue(
                        code=MarketDataIssueCode.FUTURE_CANDLE,
                        message="Candle timestamp is later than validation time.",
                        candle_timestamp=candle.timestamp,
                    )
                )

            if not candle.is_closed:
                structurally_valid = False
                issues.append(
                    MarketDataIssue(
                        code=MarketDataIssueCode.UNCLOSED_CANDLE,
                        message="An unclosed candle cannot be used for analysis.",
                        candle_timestamp=candle.timestamp,
                    )
                )

            if candle.quality != CandleQuality.VERIFIED:
                structurally_valid = False
                issues.append(
                    MarketDataIssue(
                        code=MarketDataIssueCode.UNVERIFIED_CANDLE,
                        message=(
                            "Candle quality is not VERIFIED. "
                            "The candle is blocked from analysis."
                        ),
                        candle_timestamp=candle.timestamp,
                        details={"quality": candle.quality.value},
                    )
                )

            if candle.volume_source == VolumeSource.UNKNOWN:
                volume_data_available = False
                issues.append(
                    MarketDataIssue(
                        code=MarketDataIssueCode.UNKNOWN_VOLUME_SOURCE,
                        message=(
                            "Volume provenance is unknown. "
                            "Volume-dependent analysis must not use this candle."
                        ),
                        candle_timestamp=candle.timestamp,
                    )
                )

        for previous, current in zip(candles, candles[1:]):
            actual_seconds = int(
                (current.timestamp - previous.timestamp).total_seconds()
            )

            if actual_seconds > timeframe_seconds:
                missing_intervals = max(
                    1,
                    (actual_seconds // timeframe_seconds) - 1,
                )

                confirmed_closed = False

                if (
                    self._session_calendar is not None
                    and calendar_matches_symbol
                ):
                    confirmed_closed = (
                        self._session_calendar.gap_is_confirmed_closed(
                            previous_candle_utc=previous.timestamp,
                            next_candle_utc=current.timestamp,
                            timeframe_seconds=timeframe_seconds,
                        )
                    )

                gap = MarketDataGap(
                    previous_candle=previous.timestamp,
                    next_candle=current.timestamp,
                    expected_interval_seconds=timeframe_seconds,
                    actual_interval_seconds=actual_seconds,
                    missing_intervals=missing_intervals,
                    confirmed_closed=confirmed_closed,
                )
                gaps.append(gap)

                if confirmed_closed:
                    issues.append(
                        MarketDataIssue(
                            code=MarketDataIssueCode.SESSION_CLOSED_GAP,
                            message=(
                                "The missing candle intervals are outside "
                                "the configured broker trading sessions."
                            ),
                            candle_timestamp=current.timestamp,
                            details={
                                "expected_interval_seconds": timeframe_seconds,
                                "actual_interval_seconds": actual_seconds,
                                "missing_intervals": missing_intervals,
                                "calendar_symbol": self._session_calendar.symbol,
                            },
                        )
                    )
                else:
                    issues.append(
                        MarketDataIssue(
                            code=MarketDataIssueCode.TIME_GAP,
                            message=(
                                "A time gap was detected and was not "
                                "confirmed as a broker market closure."
                            ),
                            candle_timestamp=current.timestamp,
                            details={
                                "expected_interval_seconds": timeframe_seconds,
                                "actual_interval_seconds": actual_seconds,
                                "missing_intervals": missing_intervals,
                            },
                        )
                    )

        has_unclassified_gaps = any(
            not gap.confirmed_closed for gap in gaps
        )

        analysis_eligible = (
            structurally_valid
            and freshness_eligible
            and not has_unclassified_gaps
        )

        return MarketDataValidationReport(
            symbol=batch.symbol,
            timeframe=batch.timeframe,
            source=batch.source,
            checked_at=validation_time,
            candle_count=len(candles),
            structurally_valid=structurally_valid,
            analysis_eligible=analysis_eligible,
            volume_data_available=volume_data_available,
            expected_latest_close_at=expected_latest_close_at,
            latest_candle_close_delay_seconds=(
                latest_candle_close_delay_seconds
            ),
            data_age_seconds=data_age_seconds,
            freshness_eligible=freshness_eligible,
            freshness_verified=False,
            issues=tuple(issues),
            gaps=tuple(gaps),
        )
