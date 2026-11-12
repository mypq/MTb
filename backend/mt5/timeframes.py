"""MetaTrader 5 timeframe mapping and duration definitions."""

from types import MappingProxyType
from typing import Any


_TIMEFRAME_CONSTANTS = MappingProxyType(
    {
        "M1": "TIMEFRAME_M1",
        "M5": "TIMEFRAME_M5",
        "M15": "TIMEFRAME_M15",
        "M30": "TIMEFRAME_M30",
        "H1": "TIMEFRAME_H1",
        "H4": "TIMEFRAME_H4",
        "D1": "TIMEFRAME_D1",
    }
)

_TIMEFRAME_SECONDS = MappingProxyType(
    {
        "M1": 60,
        "M5": 300,
        "M15": 900,
        "M30": 1800,
        "H1": 3600,
        "H4": 14400,
        "D1": 86400,
    }
)


def normalize_timeframe(timeframe: str) -> str:
    """Return a supported timeframe in its canonical uppercase form."""
    if not isinstance(timeframe, str):
        raise TypeError("Timeframe must be a string.")

    normalized = timeframe.strip().upper()

    if normalized not in _TIMEFRAME_CONSTANTS:
        raise ValueError(
            f"Unsupported timeframe: {timeframe!r}."
        )

    return normalized


def timeframe_seconds(timeframe: str) -> int:
    """Return the nominal duration of a supported timeframe in seconds."""
    normalized = normalize_timeframe(timeframe)
    return _TIMEFRAME_SECONDS[normalized]


def resolve_mt5_timeframe(
    mt5_module: Any,
    timeframe: str,
) -> int:
    """Resolve a timeframe using the installed MT5 module constants."""
    normalized = normalize_timeframe(timeframe)
    constant_name = _TIMEFRAME_CONSTANTS[normalized]

    try:
        value = getattr(mt5_module, constant_name)
    except AttributeError as exc:
        raise RuntimeError(
            f"MT5 module does not provide {constant_name}."
        ) from exc

    if isinstance(value, bool) or not isinstance(value, int):
        raise RuntimeError(
            f"MT5 constant {constant_name} is not an integer."
        )

    return value
