"""MetaTrader 5 integration package.

This package currently provides read-only terminal connectivity
and normalized account and symbol information.
"""

from backend.mt5.adapter import MT5Adapter, MT5AdapterError
from backend.mt5.models import (
    MT5AccountInfo,
    MT5ConnectionState,
    MT5ConnectionStatus,
    MT5SymbolInfo,
)

__all__ = [
    "MT5Adapter",
    "MT5AdapterError",
    "MT5AccountInfo",
    "MT5ConnectionState",
    "MT5ConnectionStatus",
    "MT5SymbolInfo",
]
