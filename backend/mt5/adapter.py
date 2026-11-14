
from __future__ import annotations

from datetime import datetime, timezone
from math import isfinite
from threading import RLock
from typing import Any

from backend.config import Settings, get_settings
from backend.market_data import (
    CandleBatch,
    CandleQuality,
    MarketCandle,
    VolumeSource,
)
from backend.mt5.models import (
    MT5AccountInfo,
    MT5ConnectionState,
    MT5ConnectionStatus,
    MT5SymbolInfo,
)
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
            account = mt5.account_info()

            if account is None:
                raise MT5AdapterError(
                    self._last_error_message(mt5)
                )

            return MT5AccountInfo(
                login=int(account.login),
                server=str(account.server),
                currency=str(account.currency),
                balance=float(account.balance),
                equity=float(account.equity),
                margin=float(account.margin),
                free_margin=float(account.margin_free),
                leverage=int(account.leverage),
                trade_allowed=bool(account.trade_allowed),
                trade_expert=bool(account.trade_expert),
            )

    def get_symbol_info(self, symbol: str) -> MT5SymbolInfo:
        normalized_symbol = symbol.strip()

        if not normalized_symbol:
            raise ValueError("Symbol name cannot be empty.")

        with self._lock:
            mt5 = self._require_connection()
            info = mt5.symbol_info(normalized_symbol)

            if info is None:
                raise MT5AdapterError(
                    f"Symbol {normalized_symbol!r} is unavailable. "
                    f"{self._last_error_message(mt5)}"
                )

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

            numeric_fields = {
                "point": float(info.point),
                "trade_contract_size": float(
                    info.trade_contract_size
                ),
                "trade_tick_size": float(info.trade_tick_size),
                "trade_tick_value": float(info.trade_tick_value),
                "volume_min": float(info.volume_min),
                "volume_max": float(info.volume_max),
                "volume_step": float(info.volume_step),
            }

            for field, value in numeric_fields.items():
                if not isfinite(value):
                    raise MT5AdapterError(
                        f"MT5 returned non-finite symbol field: {field}."
                    )

            if numeric_fields["volume_min"] > numeric_fields["volume_max"]:
                raise MT5AdapterError(
                    "MT5 returned invalid volume limits."
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
                spread=int(info.spread),
                visible=bool(info.visible),
            )

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

            received_at = datetime.now(timezone.utc)
            candles: list[MarketCandle] = []

            try:
                for rate in rates:
                    timestamp_value = int(rate["time"])

                    if timestamp_value <= 0:
                        raise ValueError(
                            "MT5 returned an invalid candle timestamp."
                        )

                    candle_time = datetime.fromtimestamp(
                        timestamp_value,
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
        status = self.connection_status()

        if status.state != MT5ConnectionState.CONNECTED:
            status = self.connect()

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
