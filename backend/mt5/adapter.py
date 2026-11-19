from __future__ import annotations

from datetime import datetime, timezone
from math import isfinite
from operator import index
from threading import RLock
from time import monotonic_ns
from typing import Any

from backend.config import Settings, get_settings
from backend.market_data import (
    CandleBatch,
    CandleQuality,
    MarketCandle,
    VolumeSource,
)
from backend.market_data.broker_candle_clock import (
    BrokerCandleClockCalibrator,
)
from backend.market_data.broker_offset_registry import (
    BrokerOffsetRegistry,
)
from backend.market_data.models import MarketDataClockEvidence
from backend.mt5.models import (
    MT5AccountInfo,
    MT5ConnectionState,
    MT5ConnectionStatus,
    MT5SymbolInfo,
)
from backend.mt5.time_models import BrokerTimeSample
from backend.mt5.timeframes import (
    normalize_timeframe,
    resolve_mt5_timeframe,
)


class MT5AdapterError(RuntimeError):
    """Raised when a MetaTrader 5 operation cannot be completed."""


class MT5Adapter:
    """Read-only adapter for the local MetaTrader 5 terminal.

    No trade execution methods are provided.
    All terminal access is serialized through a lock.
    """

    def __init__(
        self,
        settings: Settings | None = None,
        mt5_module: Any | None = None,
    ) -> None:
        self._settings = settings or get_settings()
        self._mt5 = mt5_module
        self._initialized_by_client = False
        self._lock = RLock()
        self._candle_clock_calibrator: BrokerCandleClockCalibrator | None = None
        self._broker_offset_registry = None

    def set_candle_clock_calibrator(
        self,
        calibrator: BrokerCandleClockCalibrator,
    ) -> None:
        if not isinstance(calibrator, BrokerCandleClockCalibrator):
            raise TypeError(
                "calibrator must be a BrokerCandleClockCalibrator."
            )

        with self._lock:
            self._candle_clock_calibrator = calibrator

    def mark_candle_clock_connection_lost(self) -> None:
        """Invalidate candle-clock calibration after MT5 disconnects."""
        with self._lock:
            calibrator = self._candle_clock_calibrator

        if calibrator is not None:
            calibrator.mark_connection_lost()

    def set_broker_offset_registry(self, registry) -> None:
        if not callable(getattr(registry, "latest", None)):
            raise TypeError(
                "registry must provide a callable latest method."
            )

        with self._lock:
            self._broker_offset_registry = registry

    def get_active_candle_clock_sample(self, symbol: str) -> dict:
        """Read active M1/M5 candle boundaries and broker identity."""
        normalized_symbol = symbol.strip()

        if not normalized_symbol:
            raise ValueError("Symbol name cannot be empty.")

        with self._lock:
            mt5 = self._require_connection()

            try:
                account = mt5.account_info()

                if account is None:
                    raise MT5AdapterError(
                        "MT5 account identity is unavailable."
                    )

                account_login = index(account.login)
                broker_server = str(account.server).strip()

                if account_login <= 0 or not broker_server:
                    raise MT5AdapterError(
                        "MT5 returned invalid account identity."
                    )

                read_started = datetime.now(timezone.utc)

                m1 = mt5.copy_rates_from_pos(
                    normalized_symbol,
                    mt5.TIMEFRAME_M1,
                    0,
                    1,
                )

                m5 = mt5.copy_rates_from_pos(
                    normalized_symbol,
                    mt5.TIMEFRAME_M5,
                    0,
                    1,
                )

                read_finished = datetime.now(timezone.utc)

                if m1 is None or len(m1) != 1:
                    raise MT5AdapterError(
                        "MT5 did not return the active M1 candle."
                    )

                if m5 is None or len(m5) != 1:
                    raise MT5AdapterError(
                        "MT5 did not return the active M5 candle."
                    )

                m1_open = index(m1[0]["time"])
                m5_open = index(m5[0]["time"])

                if m1_open <= 0 or m5_open <= 0:
                    raise MT5AdapterError(
                        "MT5 returned invalid active candle timestamps."
                    )

                if (
                    read_finished.timestamp()
                    - read_started.timestamp()
                    > 1.0
                ):
                    raise MT5AdapterError(
                        "Active candle clock read took too long."
                    )

                return {
                    "symbol": normalized_symbol,
                    "account_login": account_login,
                    "broker_server": broker_server,
                    "m1_active_open_epoch": m1_open,
                    "m5_active_open_epoch": m5_open,
                    "observed_at_utc": read_started
                    + (read_finished - read_started) / 2,
                }

            except MT5AdapterError:
                raise
            except Exception as exc:
                raise MT5AdapterError(
                    "Active candle clock sampling failed: "
                    f"{type(exc).__name__}."
                ) from exc

    def connect(self) -> MT5ConnectionStatus:
        with self._lock:
            if not self._settings.MT5_ENABLED:
                return self._status(
                    MT5ConnectionState.DISABLED,
                    "MetaTrader 5 integration is disabled.",
                )

            if self._initialized_by_client:
                current_status = self.connection_status()

                if current_status.state == MT5ConnectionState.CONNECTED:
                    return current_status

                mt5 = self._mt5

                if mt5 is None:
                    self._initialized_by_client = False
                    return self._status(
                        MT5ConnectionState.ERROR,
                        "MT5 module is unavailable during reconnection.",
                    )

                try:
                    mt5.shutdown()
                except Exception as exc:
                    return self._status(
                        MT5ConnectionState.ERROR,
                        "MT5 cleanup before reconnection raised "
                        f"{type(exc).__name__}.",
                    )
                finally:
                    self._initialized_by_client = False

            mt5 = self._mt5

            if mt5 is None:
                try:
                    import MetaTrader5 as mt5
                except ImportError:
                    return self._status(
                        MT5ConnectionState.ERROR,
                        "MetaTrader5 package is not installed.",
                    )

            initialize_args: dict[str, object] = {
                "timeout": self._settings.MT5_TIMEOUT_MS,
            }

            if self._settings.MT5_TERMINAL_PATH:
                initialize_args["path"] = (
                    self._settings.MT5_TERMINAL_PATH
                )

            if self._settings.MT5_LOGIN is not None:
                initialize_args["login"] = self._settings.MT5_LOGIN

            password = (
                self._settings.MT5_PASSWORD.get_secret_value()
            )
            if password:
                initialize_args["password"] = password

            if self._settings.MT5_SERVER:
                initialize_args["server"] = self._settings.MT5_SERVER

            try:
                initialized = mt5.initialize(**initialize_args)
            except Exception as exc:
                return self._status(
                    MT5ConnectionState.ERROR,
                    f"MT5 initialization raised {type(exc).__name__}.",
                )

            if not initialized:
                return self._status(
                    MT5ConnectionState.DISCONNECTED,
                    self._last_error_message(mt5),
                )

            self._mt5 = mt5
            self._initialized_by_client = True

            return self.connection_status()

    def connection_status(self) -> MT5ConnectionStatus:
        with self._lock:
            if not self._settings.MT5_ENABLED:
                return self._status(
                    MT5ConnectionState.DISABLED,
                    "MetaTrader 5 integration is disabled.",
                )

            if not self._initialized_by_client or self._mt5 is None:
                return self._status(
                    MT5ConnectionState.DISCONNECTED,
                    "MetaTrader 5 is not initialized by this adapter.",
                )

            try:
                terminal = self._mt5.terminal_info()
                account = self._mt5.account_info()

                if terminal is None or account is None:
                    return self._status(
                        MT5ConnectionState.DISCONNECTED,
                        self._last_error_message(self._mt5),
                    )

                if not bool(terminal.connected):
                    return self._status(
                        MT5ConnectionState.DISCONNECTED,
                        "MT5 terminal is not connected to the trade server.",
                    )

                return MT5ConnectionStatus(
                    state=MT5ConnectionState.CONNECTED,
                    terminal_connected=True,
                    trading_allowed=bool(
                        terminal.trade_allowed
                        and account.trade_allowed
                    ),
                    message="MetaTrader 5 connection is available.",
                )

            except Exception as exc:
                return self._status(
                    MT5ConnectionState.ERROR,
                    f"MT5 status check raised {type(exc).__name__}.",
                )

    def get_account_info(self) -> MT5AccountInfo:
        with self._lock:
            mt5 = self._require_connection()
            try:
                account = mt5.account_info()
            except Exception as exc:
                raise MT5AdapterError(
                    "MT5 account information request raised "
                    f"{type(exc).__name__}."
                ) from exc

            if account is None:
                raise MT5AdapterError(
                    self._last_error_message(mt5)
                )

            try:
                raw_financial_values = {
                    "balance": account.balance,
                    "equity": account.equity,
                    "margin": account.margin,
                    "free_margin": account.margin_free,
                }
                financial_values: dict[str, float] = {}

                for field, raw_value in raw_financial_values.items():
                    if isinstance(raw_value, bool):
                        raise MT5AdapterError(
                            f"MT5 returned invalid account field: {field}."
                        )

                    try:
                        value = float(raw_value)
                    except (TypeError, ValueError, OverflowError) as exc:
                        raise MT5AdapterError(
                            f"MT5 returned invalid account field: {field}."
                        ) from exc

                    if not isfinite(value):
                        raise MT5AdapterError(
                            f"MT5 returned non-finite account field: {field}."
                        )

                    financial_values[field] = value

                if financial_values["margin"] < 0:
                    raise MT5AdapterError(
                        "MT5 returned a negative account margin."
                    )

                return MT5AccountInfo(
                    login=int(account.login),
                    server=str(account.server),
                    currency=str(account.currency),
                    balance=financial_values["balance"],
                    equity=financial_values["equity"],
                    margin=financial_values["margin"],
                    free_margin=financial_values["free_margin"],
                    leverage=int(account.leverage),
                    trade_allowed=bool(account.trade_allowed),
                    trade_expert=bool(account.trade_expert),
                )

            except MT5AdapterError:
                raise
            except Exception as exc:
                raise MT5AdapterError(
                    "MT5 returned malformed account information."
                ) from exc

    def get_broker_time_sample(
        self,
        symbol: str,
    ) -> BrokerTimeSample:
        """Capture one broker quote timestamp and its local receipt clocks.

        The broker timestamp is preserved as a raw millisecond value.
        This is a quote-event sample, not a broker wall-clock response.
        """
        if not isinstance(symbol, str):
            raise ValueError("Symbol name must be a string.")

        normalized_symbol = symbol.strip()

        if not normalized_symbol:
            raise ValueError("Symbol name cannot be empty.")

        with self._lock:
            mt5 = self._require_connection()

            try:
                account = mt5.account_info()
            except Exception as exc:
                raise MT5AdapterError(
                    "MT5 account identity request raised "
                    f"{type(exc).__name__}."
                ) from exc

            if account is None:
                raise MT5AdapterError(
                    "MT5 account identity is unavailable. "
                    f"{self._last_error_message(mt5)}"
                )

            try:
                raw_login = account.login

                if isinstance(raw_login, bool):
                    raise ValueError(
                        "MT5 returned a boolean account login."
                    )

                account_login = index(raw_login)
                broker_server = str(account.server).strip()

                if account_login <= 0 or not broker_server:
                    raise ValueError(
                        "MT5 returned invalid account identity."
                    )

            except (AttributeError, TypeError, ValueError, OverflowError) as exc:
                raise MT5AdapterError(
                    "MT5 returned malformed account identity."
                ) from exc

            try:
                tick = mt5.symbol_info_tick(normalized_symbol)
            except Exception as exc:
                raise MT5AdapterError(
                    "MT5 tick request raised "
                    f"{type(exc).__name__}."
                ) from exc

            received_monotonic = monotonic_ns()
            received_at = datetime.now(timezone.utc)

            if tick is None:
                raise MT5AdapterError(
                    "MT5 returned no tick for "
                    f"{normalized_symbol!r}. "
                    f"{self._last_error_message(mt5)}"
                )

            try:
                raw_timestamp = tick.time_msc

                if isinstance(raw_timestamp, bool):
                    raise ValueError(
                        "MT5 returned an invalid tick timestamp."
                    )

                broker_time_msc = index(raw_timestamp)

                if broker_time_msc <= 0:
                    raise ValueError(
                        "MT5 returned a non-positive tick timestamp."
                    )

                return BrokerTimeSample(
                    symbol=normalized_symbol,
                    broker_time_msc=broker_time_msc,
                    received_at_utc=received_at,
                    received_monotonic_ns=received_monotonic,
                    bid=tick.bid,
                    ask=tick.ask,
                    account_login=account_login,
                    broker_server=broker_server,
                )

            except (AttributeError, TypeError, ValueError, OverflowError) as exc:
                raise MT5AdapterError(
                    f"MT5 tick data failed validation: {exc}"
                ) from exc

    def get_symbol_info(self, symbol: str) -> MT5SymbolInfo:
        if not isinstance(symbol, str):
            raise ValueError("Symbol name must be a string.")

        normalized_symbol = symbol.strip()

        if not normalized_symbol:
            raise ValueError("Symbol name cannot be empty.")

        with self._lock:
            mt5 = self._require_connection()
            try:
                info = mt5.symbol_info(normalized_symbol)
            except Exception as exc:
                raise MT5AdapterError(
                    "MT5 symbol information request raised "
                    f"{type(exc).__name__}."
                ) from exc

            if info is None:
                raise MT5AdapterError(
                    f"Symbol {normalized_symbol!r} is unavailable. "
                    f"{self._last_error_message(mt5)}"
                )

            try:
                name = str(info.name).strip()

                if not name:
                    raise MT5AdapterError(
                        "MT5 returned an empty symbol name."
                    )

                digits = info.digits

                if (
                    isinstance(digits, bool)
                    or not isinstance(digits, int)
                    or not 0 <= digits <= 12
                ):
                    raise MT5AdapterError(
                        "MT5 returned invalid symbol digits."
                    )

                raw_numeric_fields = {
                    "point": info.point,
                    "trade_contract_size": info.trade_contract_size,
                    "trade_tick_size": info.trade_tick_size,
                    "trade_tick_value": info.trade_tick_value,
                    "volume_min": info.volume_min,
                    "volume_max": info.volume_max,
                    "volume_step": info.volume_step,
                }

                numeric_fields = {}

                for field, raw_value in raw_numeric_fields.items():
                    if isinstance(raw_value, bool):
                        raise MT5AdapterError(
                            f"MT5 returned invalid symbol field: {field}."
                        )

                    try:
                        value = float(raw_value)
                    except (TypeError, ValueError, OverflowError) as exc:
                        raise MT5AdapterError(
                            f"MT5 returned invalid symbol field: {field}."
                        ) from exc

                    if not isfinite(value):
                        raise MT5AdapterError(
                            f"MT5 returned non-finite symbol field: {field}."
                        )

                    numeric_fields[field] = value

                if numeric_fields["volume_min"] > numeric_fields["volume_max"]:
                    raise MT5AdapterError(
                        "MT5 returned invalid volume limits."
                    )

                spread = info.spread

                if (
                    isinstance(spread, bool)
                    or not isinstance(spread, int)
                    or spread < 0
                ):
                    raise MT5AdapterError(
                        "MT5 returned invalid symbol spread."
                    )

                return MT5SymbolInfo(
                    name=name,
                    description=str(info.description or ""),
                    digits=digits,
                    point=numeric_fields["point"],
                    trade_contract_size=numeric_fields[
                        "trade_contract_size"
                    ],
                    trade_tick_size=numeric_fields[
                        "trade_tick_size"
                    ],
                    trade_tick_value=numeric_fields[
                        "trade_tick_value"
                    ],
                    volume_min=numeric_fields["volume_min"],
                    volume_max=numeric_fields["volume_max"],
                    volume_step=numeric_fields["volume_step"],
                    spread=spread,
                    visible=bool(info.visible),
                )

            except MT5AdapterError:
                raise
            except Exception as exc:
                raise MT5AdapterError(
                    "MT5 returned malformed symbol information."
                ) from exc

    def get_closed_candles(
        self,
        symbol: str,
        timeframe: str,
        count: int,
    ) -> CandleBatch:
        """Retrieve and validate closed candles from MT5.

        Position zero is deliberately excluded because it is the
        currently forming candle. MT5 rate timestamps are Unix
        timestamps and are converted explicitly to UTC.

        VERIFIED here means the returned candle passed the local
        structural and closed-bar checks. It does not prove broker
        clock alignment or that no historical bars are missing.
        """
        normalized_symbol = symbol.strip()

        if not normalized_symbol:
            raise ValueError("Symbol name cannot be empty.")

        if isinstance(count, bool) or not isinstance(count, int):
            raise TypeError("Candle count must be an integer.")

        if not 1 <= count <= 10000:
            raise ValueError("Candle count must be between 1 and 10000.")

        normalized_timeframe = normalize_timeframe(timeframe)

        with self._lock:
            mt5 = self._require_connection()

            # Verify the broker candle clock before requesting
            # any closed candle data from MT5.
            calibrator = self._candle_clock_calibrator

            if calibrator is None:
                raise MT5AdapterError(
                    "Candle clock calibration is not configured. "
                    "UTC conversion is blocked."
                )

            clock, live_ntp = calibrator.trusted_evidence()

            if (
                clock.status != "VERIFIED"
                or clock.offset_seconds is None
                or not isfinite(clock.offset_seconds)
                or clock.uncertainty_seconds is None
                or not isfinite(clock.uncertainty_seconds)
                or clock.uncertainty_seconds > 2.0
                or clock.sample_count < 3
                or clock.symbol != normalized_symbol
            ):
                raise MT5AdapterError(
                    "Candle clock is not verified for this symbol: "
                    f"{clock.reason}"
                )

            if (
                live_ntp is None
                or not live_ntp.verified
                or live_ntp.checked_at_unix is None
                or not isfinite(live_ntp.checked_at_unix)
                or live_ntp.checked_at_unix <= 0
                or live_ntp.offset_seconds is None
                or not isfinite(live_ntp.offset_seconds)
                or live_ntp.uncertainty_seconds is None
                or not isfinite(live_ntp.uncertainty_seconds)
                or live_ntp.uncertainty_seconds < 0
                or live_ntp.uncertainty_seconds > 1.0
                or live_ntp.sample_count < 3
                or len(live_ntp.servers) < 3
                or len(set(live_ntp.servers)) < 3
            ):
                raise MT5AdapterError(
                    "Live trusted NTP evidence is unavailable. "
                    "UTC candle conversion is blocked."
                )

            ntp_checked_at = datetime.fromtimestamp(
                live_ntp.checked_at_unix + live_ntp.offset_seconds,
                tz=timezone.utc,
            )

            # Verify that the currently connected account is the
            # same account and broker server used for calibration.
            try:
                account = mt5.account_info()

                if account is None:
                    raise ValueError("Account identity unavailable.")

                if (
                    index(account.login) != clock.account_login
                    or str(account.server).strip() != clock.broker_server
                ):
                    calibrator.reset()
                    raise ValueError("Broker identity changed.")

            except Exception as exc:
                raise MT5AdapterError(
                    f"Candle clock identity verification failed: {exc}"
                ) from exc

            # Resolve historical broker offsets per candle below.
            # The current live calibration is not proof for older bars.
            registry = self._broker_offset_registry

            if registry is None:
                raise MT5AdapterError(
                    "Broker offset confirmation registry is not "
                    "configured. UTC conversion is blocked."
                )

            try:
                terminal = mt5.terminal_info()

                if terminal is None:
                    raise ValueError(
                        "MT5 terminal information is unavailable."
                    )

                raw_build = terminal.build

                if isinstance(raw_build, bool):
                    raise ValueError(
                        "Invalid MT5 terminal build."
                    )

                terminal_build = index(raw_build)

                if terminal_build <= 0:
                    raise ValueError(
                        "Invalid MT5 terminal build."
                    )

                identity = (
                    index(account.login),
                    str(account.server).strip(),
                    normalized_symbol,
                    terminal_build,
                )

            except Exception as exc:
                raise MT5AdapterError(
                    "Broker offset identity lookup failed: "
                    f"{type(exc).__name__}: {exc}"
                ) from exc

            # Only retrieve closed candles after clock and identity
            # verification have both succeeded.
            mt5_timeframe = resolve_mt5_timeframe(
                mt5,
                normalized_timeframe,
            )

            try:
                rates = mt5.copy_rates_from_pos(
                    normalized_symbol,
                    mt5_timeframe,
                    1,
                    count,
                )
            except Exception as exc:
                raise MT5AdapterError(
                    "MT5 candle retrieval raised "
                    f"{type(exc).__name__}."
                ) from exc

            if rates is None:
                raise MT5AdapterError(
                    "MT5 returned no candle data. "
                    f"{self._last_error_message(mt5)}"
                )

            if len(rates) == 0:
                raise MT5AdapterError(
                    "MT5 returned an empty candle history."
                )

            received_at = datetime.fromtimestamp(
                datetime.now(timezone.utc).timestamp()
                + live_ntp.offset_seconds,
                tz=timezone.utc,
            )
            candles: list[MarketCandle] = []

            try:
                for rate in rates:
                    raw_timestamp = rate["time"]

                    if isinstance(raw_timestamp, bool):
                        raise ValueError(
                            "MT5 returned an invalid candle timestamp."
                        )

                    try:
                        timestamp_value = index(raw_timestamp)
                    except (TypeError, ValueError, OverflowError) as exc:
                        raise ValueError(
                            "MT5 returned an invalid candle timestamp."
                        ) from exc

                    if timestamp_value <= 0:
                        raise ValueError(
                            "MT5 returned an invalid candle timestamp."
                        )

                    historical_record = registry.for_broker_time(
                        identity,
                        timestamp_value,
                    )

                    if (
                        historical_record is None
                        or historical_record.event_type != "CONFIRMATION"
                    ):
                        raise ValueError(
                            "No confirmed historical broker-offset evidence "
                            "covers raw candle timestamp "
                            f"{timestamp_value}; UTC conversion is blocked."
                        )

                    historical_ntp = historical_record.ntp_evidence

                    if (
                        historical_record.evidence_sample_count != 3
                        or len(historical_record.evidence_samples) != 3
                        or not isinstance(historical_ntp, dict)
                        or historical_ntp.get("verified") is not True
                        or historical_ntp.get("sample_count", 0) < 3
                    ):
                        raise ValueError(
                            "Historical broker-offset confirmation evidence "
                            "is incomplete."
                        )

                    candle_time = datetime.fromtimestamp(
                        timestamp_value - historical_record.offset_seconds,
                        tz=timezone.utc,
                    )

                    if candle_time > received_at:
                        raise ValueError(
                            "MT5 returned a candle with a future timestamp."
                        )

                    open_price = float(rate["open"])
                    high_price = float(rate["high"])
                    low_price = float(rate["low"])
                    close_price = float(rate["close"])

                    prices = (
                        open_price,
                        high_price,
                        low_price,
                        close_price,
                    )

                    if not all(isfinite(price) for price in prices):
                        raise ValueError(
                            "MT5 returned a non-finite candle price."
                        )

                    tick_volume = int(rate["tick_volume"])
                    raw_real_volume = int(rate["real_volume"])

                    if tick_volume < 0 or raw_real_volume < 0:
                        raise ValueError(
                            "MT5 returned a negative volume value."
                        )

                    if raw_real_volume > 0:
                        real_volume: int | None = raw_real_volume
                        volume_source = VolumeSource.REAL
                    else:
                        real_volume = None
                        volume_source = VolumeSource.TICK

                    candle = MarketCandle(
                        symbol=normalized_symbol,
                        timeframe=normalized_timeframe,
                        timestamp=candle_time,
                        open=open_price,
                        high=high_price,
                        low=low_price,
                        close=close_price,
                        tick_volume=tick_volume,
                        real_volume=real_volume,
                        volume_source=volume_source,
                        is_closed=True,
                        quality=CandleQuality.VERIFIED,
                    )

                    candles.append(candle)

            except (KeyError, TypeError, ValueError, OverflowError) as exc:
                raise MT5AdapterError(
                    f"MT5 candle data failed validation: {exc}"
                ) from exc

            candles.sort(key=lambda candle: candle.timestamp)

            try:
                return CandleBatch(
                    symbol=normalized_symbol,
                    timeframe=normalized_timeframe,
                    candles=tuple(candles),
                    received_at=received_at,
                    source="MT5",
                    clock_evidence=MarketDataClockEvidence(
                        verified=True,
                        account_login=index(account.login),
                        broker_server=str(account.server).strip(),
                        symbol=normalized_symbol,
                        terminal_build=terminal_build,
                        checked_at=ntp_checked_at,
                        ntp_offset_seconds=live_ntp.offset_seconds,
                        ntp_uncertainty_seconds=live_ntp.uncertainty_seconds,
                        ntp_sample_count=live_ntp.sample_count,
                        ntp_servers=tuple(live_ntp.servers),
                        candle_offset_seconds=clock.offset_seconds,
                        candle_uncertainty_seconds=clock.uncertainty_seconds,
                        calibration_sample_count=clock.sample_count,
                    ),
                )
            except Exception as exc:
                raise MT5AdapterError(
                    f"MT5 candle batch validation failed: {exc}"
                ) from exc

    def shutdown(self) -> None:
        with self._lock:
            if not self._initialized_by_client or self._mt5 is None:
                return

            mt5 = self._mt5

            try:
                mt5.shutdown()
            finally:
                self._initialized_by_client = False

    def _require_connection(self) -> Any:
        """Return the active MT5 module without initiating reconnection."""
        status = self.connection_status()

        if status.state != MT5ConnectionState.CONNECTED:
            raise MT5AdapterError(
                f"MT5 connection is not ready: {status.message}"
            )

        return self._mt5

    @staticmethod
    def _last_error_message(mt5: Any) -> str:
        try:
            error = mt5.last_error()
            return f"MetaTrader 5 operation failed: {error}"
        except Exception:
            return "MetaTrader 5 operation failed; error details unavailable."

    @staticmethod
    def _status(
        state: MT5ConnectionState,
        message: str,
    ) -> MT5ConnectionStatus:
        return MT5ConnectionStatus(
            state=state,
            terminal_connected=False,
            trading_allowed=False,
            message=message,
        )

    def __enter__(self) -> MT5Adapter:
        status = self.connect()

        if status.state != MT5ConnectionState.CONNECTED:
            raise MT5AdapterError(status.message)

        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: object | None,
    ) -> None:
        self.shutdown()
