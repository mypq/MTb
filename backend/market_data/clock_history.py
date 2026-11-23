"""Validated reader for append-only MT5 SessionBridge clock evidence."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path


CLOCK_HISTORY_COLUMNS = (
    "schema_version",
    "run_id",
    "sample_sequence",
    "monotonic_ms",
    "time_gmt_epoch",
    "time_trade_server_epoch",
    "time_current_quote_epoch",
    "time_local_epoch",
    "server_utc_offset_seconds",
    "clock_values_valid",
    "account_login",
    "broker_server",
    "symbol",
    "terminal_build",
    "terminal_connected",
)


class ClockHistoryError(ValueError):
    """Invalid or inconsistent MT5 clock evidence."""


@dataclass(frozen=True, slots=True)
class MT5ClockHistorySample:
    schema_version: int
    run_id: str
    sample_sequence: int
    monotonic_ms: int
    time_gmt_epoch: int
    time_trade_server_epoch: int
    time_current_quote_epoch: int
    time_local_epoch: int
    server_utc_offset_seconds: int
    account_login: int
    broker_server: str
    symbol: str
    terminal_build: int
    terminal_connected: bool

    @property
    def identity(self) -> tuple[int, str, str, int]:
        return (
            self.account_login,
            self.broker_server,
            self.symbol,
            self.terminal_build,
        )


def _text(row: dict[str, str], name: str) -> str:
    raw = row.get(name)

    if not isinstance(raw, str) or not raw.strip():
        raise ClockHistoryError(f"{name} is missing.")

    return raw.strip()


def _integer(row: dict[str, str], name: str) -> int:
    raw = row.get(name)

    if not isinstance(raw, str) or not raw.strip():
        raise ClockHistoryError(f"{name} is missing.")

    try:
        return int(raw, 10)
    except ValueError as exc:
        raise ClockHistoryError(
            f"{name} must be an integer."
        ) from exc


def _boolean(row: dict[str, str], name: str) -> bool:
    raw = row.get(name)

    if not isinstance(raw, str):
        raise ClockHistoryError(
            f"{name} must be true or false."
        )

    raw = raw.strip().lower()

    if raw == "true":
        return True

    if raw == "false":
        return False

    raise ClockHistoryError(
        f"{name} must be true or false."
    )


def parse_clock_history_row(
    row: dict[str, str],
) -> MT5ClockHistorySample:
    if not isinstance(row, dict):
        raise ClockHistoryError(
            "Clock history row must be a mapping."
        )

    schema_version = _integer(row, "schema_version")

    if schema_version != 1:
        raise ClockHistoryError(
            "Unsupported clock history schema version."
        )

    run_id = _text(row, "run_id")
    broker_server = _text(row, "broker_server")
    symbol = _text(row, "symbol")

    sample_sequence = _integer(row, "sample_sequence")
    monotonic_ms = _integer(row, "monotonic_ms")
    time_gmt_epoch = _integer(row, "time_gmt_epoch")
    time_trade_server_epoch = _integer(
        row, "time_trade_server_epoch"
    )
    time_current_quote_epoch = _integer(
        row, "time_current_quote_epoch"
    )
    time_local_epoch = _integer(row, "time_local_epoch")
    server_utc_offset_seconds = _integer(
        row, "server_utc_offset_seconds"
    )
    account_login = _integer(row, "account_login")
    terminal_build = _integer(row, "terminal_build")

    if sample_sequence < 1:
        raise ClockHistoryError(
            "sample_sequence must be positive."
        )

    if not 0 <= monotonic_ms <= 0xFFFFFFFF:
        raise ClockHistoryError(
            "monotonic_ms is outside the GetTickCount range."
        )

    if min(
        time_gmt_epoch,
        time_trade_server_epoch,
        time_local_epoch,
        account_login,
        terminal_build,
    ) <= 0:
        raise ClockHistoryError(
            "Required clock epochs and identity values must be positive."
        )

    if time_current_quote_epoch < 0:
        raise ClockHistoryError(
            "time_current_quote_epoch cannot be negative."
        )

    if abs(server_utc_offset_seconds) > 14 * 60 * 60:
        raise ClockHistoryError(
            "Broker UTC offset is outside the supported range."
        )

    if not _boolean(row, "clock_values_valid"):
        raise ClockHistoryError(
            "MT5 marked this clock sample invalid."
        )

    terminal_connected = _boolean(
        row, "terminal_connected"
    )

    measured_offset = (
        time_trade_server_epoch - time_gmt_epoch
    )

    if measured_offset != server_utc_offset_seconds:
        raise ClockHistoryError(
            "Recorded broker offset does not match raw clock fields."
        )

    return MT5ClockHistorySample(
        schema_version=schema_version,
        run_id=run_id,
        sample_sequence=sample_sequence,
        monotonic_ms=monotonic_ms,
        time_gmt_epoch=time_gmt_epoch,
        time_trade_server_epoch=time_trade_server_epoch,
        time_current_quote_epoch=time_current_quote_epoch,
        time_local_epoch=time_local_epoch,
        server_utc_offset_seconds=server_utc_offset_seconds,
        account_login=account_login,
        broker_server=broker_server,
        symbol=symbol,
        terminal_build=terminal_build,
        terminal_connected=terminal_connected,
    )


def read_clock_history(
    path: str | Path,
) -> tuple[MT5ClockHistorySample, ...]:
    """Read the complete append-only file or reject it entirely."""

    source = Path(path)

    try:
        with source.open(
            "r",
            encoding="utf-8-sig",
            newline="",
        ) as handle:
            reader = csv.DictReader(
                handle,
                delimiter=";",
            )

            if tuple(reader.fieldnames or ()) != CLOCK_HISTORY_COLUMNS:
                raise ClockHistoryError(
                    "Clock history CSV header is invalid."
                )

            samples = []
            previous_by_run = {}

            for line_number, row in enumerate(reader, start=2):
                if row is None or None in row:
                    raise ClockHistoryError(
                        f"Invalid CSV row shape at line {line_number}."
                    )

                sample = parse_clock_history_row(row)
                previous = previous_by_run.get(sample.run_id)

                if previous is not None:
                    if (
                        sample.sample_sequence
                        != previous.sample_sequence + 1
                    ):
                        raise ClockHistoryError(
                            f"Sequence discontinuity in run "
                            f"{sample.run_id!r} at line {line_number}."
                        )

                    if sample.identity != previous.identity:
                        raise ClockHistoryError(
                            f"Identity changed inside run "
                            f"{sample.run_id!r}."
                        )

                    tick_delta = (
                        sample.monotonic_ms - previous.monotonic_ms
                    ) & 0xFFFFFFFF

                    if tick_delta == 0:
                        raise ClockHistoryError(
                            f"Monotonic counter did not advance in run "
                            f"{sample.run_id!r}."
                        )

                previous_by_run[sample.run_id] = sample
                samples.append(sample)

    except OSError as exc:
        raise ClockHistoryError(
            "Unable to read MT5 clock history."
        ) from exc

    if not samples:
        raise ClockHistoryError(
            "MT5 clock history contains no samples."
        )

    return tuple(samples)
