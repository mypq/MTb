from __future__ import annotations

from typing import Any

from backend.config import Settings, get_settings
from backend.mt5.models import (
    MT5AccountInfo,
    MT5ConnectionState,
    MT5ConnectionStatus,
    MT5SymbolInfo,
)


class MT5AdapterError(RuntimeError):
    """Raised when the MetaTrader 5 adapter cannot complete an operation."""


class MT5Adapter:
    """Read-only MetaTrader 5 terminal adapter.

    This adapter deliberately does not expose order_send or any trade
    execution method. Trading must later pass through the central
    execution gateway and risk manager.
    """

    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or get_settings()
        self._mt5: Any | None = None
        self._initialized = False

    def connect(self) -> MT5ConnectionStatus:
        if not self._settings.MT5_ENABLED:
            return MT5ConnectionStatus(
                state=MT5ConnectionState.DISABLED,
                message="MetaTrader 5 integration is disabled by configuration.",
            )

        try:
            import MetaTrader5 as mt5
        except ImportError as exc:
            return MT5ConnectionStatus(
                state=MT5ConnectionState.ERROR,
                message="MetaTrader5 package is not installed.",
            )

        self._mt5 = mt5

        initialize_args: dict[str, object] = {
            "timeout": self._settings.MT5_TIMEOUT_MS,
        }

        if self._settings.MT5_TERMINAL_PATH:
            initialize_args["path"] = self._settings.MT5_TERMINAL_PATH

        if self._settings.MT5_LOGIN is not None:
            initialize_args["login"] = self._settings.MT5_LOGIN

        password = self._settings.MT5_PASSWORD.get_secret_value()
        if password:
            initialize_args["password"] = password

        if self._settings.MT5_SERVER:
            initialize_args["server"] = self._settings.MT5_SERVER

        try:
            initialized = mt5.initialize(**initialize_args)
        except Exception as exc:
            return MT5ConnectionStatus(
                state=MT5ConnectionState.ERROR,
                message=f"MT5 initialization raised {type(exc).__name__}.",
            )

        if not initialized:
            error = mt5.last_error()
            return MT5ConnectionStatus(
                state=MT5ConnectionState.DISCONNECTED,
                message=f"MT5 initialization failed: {error}",
            )

        self._initialized = True
        return self.connection_status()

    def connection_status(self) -> MT5ConnectionStatus:
        if not self._settings.MT5_ENABLED:
            return MT5ConnectionStatus(
                state=MT5ConnectionState.DISABLED,
                message="MetaTrader 5 integration is disabled.",
            )

        if not self._initialized or self._mt5 is None:
            return MT5ConnectionStatus(
                state=MT5ConnectionState.DISCONNECTED,
                message="MetaTrader 5 is not initialized.",
            )

        try:
            terminal = self._mt5.terminal_info()
            account = self._mt5.account_info()

            if terminal is None or account is None:
                return MT5ConnectionStatus(
                    state=MT5ConnectionState.DISCONNECTED,
                    message=f"MT5 status unavailable: {self._mt5.last_error()}",
                )

            return MT5ConnectionStatus(
                state=MT5ConnectionState.CONNECTED,
                terminal_connected=bool(terminal.connected),
                trading_allowed=bool(
                    terminal.trade_allowed and account.trade_allowed
                ),
                message="MetaTrader 5 connection status retrieved.",
            )
        except Exception as exc:
            return MT5ConnectionStatus(
                state=MT5ConnectionState.ERROR,
                message=f"MT5 status check raised {type(exc).__name__}.",
            )

    def get_account_info(self) -> MT5AccountInfo:
        mt5 = self._require_connection()
        account = mt5.account_info()

        if account is None:
            raise MT5AdapterError(
                f"Could not read MT5 account information: {mt5.last_error()}"
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

        mt5 = self._require_connection()

        info = mt5.symbol_info(normalized_symbol)
        if info is None:
            raise MT5AdapterError(
                f"Symbol {normalized_symbol!r} is unavailable: {mt5.last_error()}"
            )

        return MT5SymbolInfo(
            name=str(info.name),
            description=str(info.description or ""),
            digits=int(info.digits),
            point=float(info.point),
            trade_contract_size=float(info.trade_contract_size),
            trade_tick_size=float(info.trade_tick_size),
            trade_tick_value=float(info.trade_tick_value),
            volume_min=float(info.volume_min),
            volume_max=float(info.volume_max),
            volume_step=float(info.volume_step),
            spread=int(info.spread),
            visible=bool(info.visible),
        )

    def shutdown(self) -> None:
        if self._initialized and self._mt5 is not None:
            try:
                self._mt5.shutdown()
            finally:
                self._initialized = False
                self._mt5 = None

    def _require_connection(self) -> Any:
        status = self.connection_status()

        if status.state != MT5ConnectionState.CONNECTED:
            raise MT5AdapterError(
                f"MT5 connection is not ready: {status.message}"
            )

        return self._mt5

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
