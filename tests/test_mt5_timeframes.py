from types import SimpleNamespace

import pytest

from backend.mt5.timeframes import (
    normalize_timeframe,
    resolve_mt5_timeframe,
    timeframe_seconds,
)


@pytest.mark.parametrize(
    ("timeframe", "expected"),
    [
        ("M1", "M1"),
        ("M5", "M5"),
        ("M15", "M15"),
        ("M30", "M30"),
        ("H1", "H1"),
        ("H4", "H4"),
        ("D1", "D1"),
        (" m5 ", "M5"),
        ("h1", "H1"),
    ],
)
def test_normalize_supported_timeframes(timeframe, expected):
    assert normalize_timeframe(timeframe) == expected


@pytest.mark.parametrize(
    "timeframe",
    ["M2", "M10", "H2", "W1", "", "   ", "INVALID"],
)
def test_unsupported_timeframes_are_rejected(timeframe):
    with pytest.raises(ValueError):
        normalize_timeframe(timeframe)


@pytest.mark.parametrize(
    "timeframe",
    [None, 5, True, [], {}],
)
def test_non_string_timeframes_are_rejected(timeframe):
    with pytest.raises(TypeError):
        normalize_timeframe(timeframe)


@pytest.mark.parametrize(
    ("timeframe", "expected_seconds"),
    [
        ("M1", 60),
        ("M5", 300),
        ("M15", 900),
        ("M30", 1800),
        ("H1", 3600),
        ("H4", 14400),
        ("D1", 86400),
    ],
)
def test_timeframe_duration(timeframe, expected_seconds):
    assert timeframe_seconds(timeframe) == expected_seconds


@pytest.mark.parametrize(
    ("timeframe", "constant", "expected"),
    [
        ("M1", "TIMEFRAME_M1", 1),
        ("M5", "TIMEFRAME_M5", 5),
        ("M15", "TIMEFRAME_M15", 15),
        ("M30", "TIMEFRAME_M30", 30),
        ("H1", "TIMEFRAME_H1", 60),
        ("H4", "TIMEFRAME_H4", 240),
        ("D1", "TIMEFRAME_D1", 1440),
    ],
)
def test_resolve_mt5_timeframe(
    timeframe,
    constant,
    expected,
):
    mt5 = SimpleNamespace(**{constant: expected})

    assert resolve_mt5_timeframe(mt5, timeframe) == expected


def test_missing_mt5_timeframe_constant_raises_runtime_error():
    mt5 = SimpleNamespace()

    with pytest.raises(RuntimeError, match="does not provide"):
        resolve_mt5_timeframe(mt5, "M5")


@pytest.mark.parametrize(
    "invalid_value",
    [None, "5", 5.0, True],
)
def test_invalid_mt5_timeframe_constant_type_is_rejected(
    invalid_value,
):
    mt5 = SimpleNamespace(TIMEFRAME_M5=invalid_value)

    with pytest.raises(RuntimeError, match="not an integer"):
        resolve_mt5_timeframe(mt5, "M5")


def test_timeframe_seconds_accepts_normalized_input():
    assert timeframe_seconds(" h1 ") == 3600
