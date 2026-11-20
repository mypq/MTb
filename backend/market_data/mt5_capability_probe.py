from __future__ import annotations

from datetime import datetime, timezone
from typing import Protocol

from backend.market_data.capabilities import (
    CapabilityRegistry,
    CapabilityStatus,
    MarketDataCapability,
)
from backend.config import SUPPORTED_TIMEFRAMES


class MT5CapabilitySource(Protocol):
    def get_account_info(self): ...
    def get_symbol_info(self, symbol: str): ...
    def get_broker_time_sample(self, symbol: str): ...
    def probe_tick_history(self, symbol: str, *, lookback_days: int = 1, sample_count: int = 1000): ...


def _normalize_timeframe(value: str) -> str:
    if not isinstance(value, str):
        raise TypeError("Timeframe must be a string.")
    normalized = value.strip().upper()
    if normalized not in SUPPORTED_TIMEFRAMES:
        raise ValueError(f"Unsupported timeframe: {value!r}.")
    return normalized


class MT5CapabilityProbe:
    """Record point-in-time MT5 source capabilities without persisting data.

    Historical tick probing is bounded and read-only. Broker tick timestamps
    remain raw values and are not fed into candle-clock calibration.
    Historical candle availability is established only by the normal
    clock-verified collection pipeline.
    """

    def __init__(
        self,
        source: MT5CapabilitySource,
        registry: CapabilityRegistry,
    ) -> None:
        self._source = source
        self._registry = registry

    def probe(
        self,
        symbol: str,
        timeframe: str,
        *,
        include_live_quote: bool = True,
        timeframes: tuple[str, ...] | list[str] | None = None,
    ) -> tuple[MarketDataCapability, ...]:
        normalized_symbol = symbol.strip()
        normalized_timeframe = _normalize_timeframe(timeframe)
        if not normalized_symbol:
            raise ValueError("Symbol cannot be empty.")

        configured_timeframes = (
            (normalized_timeframe,)
            if timeframes is None
            else timeframes
        )
        if (
            isinstance(configured_timeframes, (str, bytes))
            or not isinstance(configured_timeframes, (tuple, list))
            or not configured_timeframes
        ):
            raise ValueError(
                "timeframes must be a non-empty tuple or list."
            )
        normalized_timeframes = tuple(
            _normalize_timeframe(value)
            for value in configured_timeframes
        )
        if len(set(normalized_timeframes)) != len(normalized_timeframes):
            raise ValueError("timeframes must not contain duplicates.")
        if normalized_timeframe not in normalized_timeframes:
            raise ValueError(
                "The primary timeframe must be included in timeframes."
            )

        checked_at = datetime.now(timezone.utc)
        account_login = None
        broker_server = None
        observations: list[MarketDataCapability] = []

        try:
            account = self._source.get_account_info()
            observed_login = account.login
            observed_server = account.server.strip()
            if observed_login <= 0 or not observed_server:
                raise ValueError("MT5 returned incomplete account identity.")
            account_login, broker_server = observed_login, observed_server
            observations.append(self._record(
                capability="account_identity",
                status=CapabilityStatus.AVAILABLE,
                checked_at=checked_at,
                evidence=("MT5 account_info returned a validated account identity.",),
                account_login=account_login,
                broker_server=broker_server,
            ))
        except Exception as exc:
            observations.append(self._record(
                capability="account_identity",
                status=CapabilityStatus.UNAVAILABLE,
                checked_at=checked_at,
                evidence=(f"MT5 account identity probe failed: {type(exc).__name__}.",),
                limitations=("Point-in-time failure; retry after MT5 reconnects.",),
            ))

        try:
            info = self._source.get_symbol_info(normalized_symbol)
            observations.append(self._record(
                capability="symbol_metadata",
                status=CapabilityStatus.AVAILABLE,
                checked_at=checked_at,
                symbol=normalized_symbol,
                evidence=(
                    f"MT5 symbol_info returned validated metadata for {info.name}.",
                    f"Symbol precision digits: {info.digits}.",
                ),
                account_login=account_login,
                broker_server=broker_server,
            ))
        except Exception as exc:
            observations.append(self._record(
                capability="symbol_metadata",
                status=CapabilityStatus.UNAVAILABLE,
                checked_at=checked_at,
                symbol=normalized_symbol,
                evidence=(f"MT5 symbol metadata probe failed: {type(exc).__name__}.",),
                limitations=("Point-in-time failure; retry after MT5 reconnects.",),
                account_login=account_login,
                broker_server=broker_server,
            ))

        if include_live_quote:
            try:
                sample = self._source.get_broker_time_sample(normalized_symbol)
                observations.append(self._record(
                    capability="live_quote_sample",
                    status=CapabilityStatus.AVAILABLE,
                    checked_at=checked_at,
                    symbol=normalized_symbol,
                    evidence=(
                        "MT5 symbol_info_tick returned a validated live quote sample.",
                        "broker_time_msc is retained as a raw broker timestamp, not asserted to be UTC.",
                    ),
                    account_login=sample.account_login,
                    broker_server=sample.broker_server,
                ))
            except Exception as exc:
                observations.append(self._record(
                    capability="live_quote_sample",
                    status=CapabilityStatus.UNAVAILABLE,
                    checked_at=checked_at,
                    symbol=normalized_symbol,
                    evidence=(f"MT5 live quote probe failed: {type(exc).__name__}.",),
                    limitations=("Retry the live quote probe later; a later quote may become available and this is not a permanent broker restriction.",),
                    account_login=account_login,
                    broker_server=broker_server,
                ))
        else:
            observations.append(self._record(
                capability="live_quote_sample",
                status=CapabilityStatus.UNKNOWN,
                checked_at=checked_at,
                symbol=normalized_symbol,
                limitations=("The broker tick monitor records this capability when its first live sample arrives.",),
                account_login=account_login,
                broker_server=broker_server,
            ))

        try:
            tick_history = self._source.probe_tick_history(
                normalized_symbol,
                lookback_days=1,
                sample_count=1000,
            )
            tick_status = (
                CapabilityStatus.AVAILABLE
                if tick_history.all_ticks_count > 0
                else CapabilityStatus.PARTIAL
            )
            quote_status = (
                CapabilityStatus.AVAILABLE
                if tick_history.quote_ticks_count > 0
                else CapabilityStatus.PARTIAL
            )
            history_identity = {
                "account_login": account_login,
                "broker_server": broker_server,
            }
            observations.append(self._record(
                capability="tick_history_source",
                status=tick_status,
                checked_at=checked_at,
                symbol=normalized_symbol,
                evidence=(
                    "MT5 copy_ticks_from(COPY_TICKS_ALL) returned "
                    f"{tick_history.all_ticks_count} records from a "
                    f"{tick_history.lookback_days}-day bounded lookback "
                    f"(maximum {tick_history.requested_count_per_query}).",
                    "Historical API time_msc values are retained as probe evidence; "
                    "they are not used to calibrate the broker clock.",
                    "The request is bounded to one day and may synchronize "
                    "that period into the MT5 terminal's own history cache.",
                ),
                limitations=(
                    ()
                    if tick_status is CapabilityStatus.AVAILABLE
                    else (
                        "The bounded request returned no ticks. This does not "
                        "prove the broker has no history; market closure, symbol "
                        "history synchronization, or the requested window may "
                        "explain the empty result.",
                    )
                ),
                **history_identity,
            ))
            observations.append(self._record(
                capability="quote_history_source",
                status=quote_status,
                checked_at=checked_at,
                symbol=normalized_symbol,
                evidence=(
                    "MT5 copy_ticks_from(COPY_TICKS_INFO) returned "
                    f"{tick_history.quote_ticks_count} Bid/Ask-change records "
                    f"from a {tick_history.lookback_days}-day bounded lookback "
                    f"(maximum {tick_history.requested_count_per_query}).",
                ),
                limitations=(
                    ()
                    if quote_status is CapabilityStatus.AVAILABLE
                    else (
                        "The bounded Bid/Ask request returned no records. "
                        "Retry during an active market session before treating "
                        "this as a broker history restriction.",
                    )
                ),
                **history_identity,
            ))
        except Exception as exc:
            failure_evidence = (
                "The bounded MT5 historical tick probe did not complete: "
                f"{type(exc).__name__}.",
            )
            retry_limitations = (
                "This is a point-in-time probe failure, not proof that the "
                "broker permanently withholds historical ticks. Retry after "
                "confirming the MT5 terminal and symbol connection.",
            )
            for capability in (
                "tick_history_source",
                "quote_history_source",
            ):
                observations.append(self._record(
                    capability=capability,
                    status=CapabilityStatus.PARTIAL,
                    checked_at=checked_at,
                    symbol=normalized_symbol,
                    evidence=failure_evidence,
                    limitations=retry_limitations,
                    account_login=account_login,
                    broker_server=broker_server,
                ))

        observations.extend(
            self._record(
                capability="candle_history",
                status=CapabilityStatus.UNKNOWN,
                checked_at=checked_at,
                symbol=normalized_symbol,
                timeframe=frame,
                limitations=(
                    "Only the normal clock-verified and validated collector "
                    "can establish availability.",
                ),
            )
            for frame in normalized_timeframes
        )
        observations.extend((
            self._record(
                capability="tick_history_archive",
                status=CapabilityStatus.UNAVAILABLE,
                checked_at=checked_at,
                symbol=normalized_symbol,
                evidence=("The MT5 source can be probed, but the application does not yet ingest or persist historical ticks.",),
                limitations=("Source availability and persistent archive availability are separate capabilities.",),
                account_login=account_login,
                broker_server=broker_server,
            ),
            self._record(
                capability="quote_archive",
                status=CapabilityStatus.UNAVAILABLE,
                checked_at=checked_at,
                symbol=normalized_symbol,
                evidence=("Live quotes are monitored and historical Bid/Ask source probing is available, but no persistent quote archive is configured.",),
                limitations=("Source availability and persistent archive availability are separate capabilities.",),
                account_login=account_login,
                broker_server=broker_server,
            ),
        ))
        return tuple(observations)

    def _record(
        self,
        *,
        capability: str,
        status: CapabilityStatus,
        checked_at: datetime,
        evidence: tuple[str, ...] = (),
        limitations: tuple[str, ...] = (),
        symbol: str | None = None,
        timeframe: str | None = None,
        account_login: int | None = None,
        broker_server: str | None = None,
    ) -> MarketDataCapability:
        observation = MarketDataCapability(
            capability=capability,
            status=status,
            source="MT5",
            checked_at=checked_at,
            symbol=symbol,
            timeframe=timeframe,
            account_login=account_login,
            broker_server=broker_server,
            evidence=evidence,
            limitations=limitations,
        )
        self._registry.record(observation)
        return observation
