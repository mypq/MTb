
from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from time import monotonic_ns

from backend.config import get_settings
from backend.market_data.ntp_time import NtpTimeVerifier
from backend.mt5 import MT5Adapter, MT5ConnectionState


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read-only live MT5 clock evidence collector."
    )
    parser.add_argument("--symbol", default=None)
    parser.add_argument("--duration-seconds", type=int, default=900)
    parser.add_argument("--interval-seconds", type=float, default=1.0)
    parser.add_argument("--ntp-interval-seconds", type=int, default=60)
    parser.add_argument("--output", default=None)
    args = parser.parse_args()

    if args.duration_seconds < 60:
        parser.error("--duration-seconds must be at least 60.")
    if not 0.2 <= args.interval_seconds <= 30:
        parser.error("--interval-seconds must be between 0.2 and 30.")
    if args.ntp_interval_seconds < 30:
        parser.error("--ntp-interval-seconds must be at least 30.")

    return args


def json_epochs(rates) -> str:
    if rates is None:
        return "[]"
    return json.dumps(
        [int(rate["time"]) for rate in rates],
        separators=(",", ":"),
    )


def main() -> int:
    args = parse_args()
    settings = get_settings()
    symbol = (args.symbol or settings.MT5_SYMBOL or "GOLD").strip()

    if not symbol:
        print("ERROR: MT5 symbol is empty.", file=sys.stderr)
        return 2

    output = Path(
        args.output
        or (
            "diagnostics/mt5_clock_evidence_"
            + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            + ".csv"
        )
    )
    output.parent.mkdir(parents=True, exist_ok=True)

    adapter = MT5Adapter(settings=settings)
    status = adapter.connect()

    if status.state != MT5ConnectionState.CONNECTED:
        print(
            f"ERROR: MT5 connection failed: {status.message}",
            file=sys.stderr,
        )
        return 3

    try:
        import MetaTrader5 as mt5
    except ImportError:
        adapter.shutdown()
        print("ERROR: MetaTrader5 package is not installed.", file=sys.stderr)
        return 4

    if not mt5.symbol_select(symbol, True):
        adapter.shutdown()
        print(
            f"ERROR: Cannot select {symbol}: {mt5.last_error()}",
            file=sys.stderr,
        )
        return 5

    verifier = NtpTimeVerifier()
    last_ntp_check = 0.0
    ntp_result = None
    started = time.monotonic()

    fields = [
        "captured_at_utc",
        "capture_monotonic_ns",
        "ntp_verified",
        "ntp_checked_at_unix",
        "ntp_offset_seconds",
        "ntp_uncertainty_seconds",
        "account_login",
        "broker_server",
        "symbol",
        "tick_time_msc_raw",
        "tick_time_epoch_raw",
        "tick_minus_corrected_utc_seconds",
        "m1_closed_open_epochs_raw",
        "m5_closed_open_epochs_raw",
        "latest_m1_open_minus_corrected_utc_seconds",
        "latest_m5_open_minus_corrected_utc_seconds",
        "capture_elapsed_seconds",
        "read_duration_seconds",
        "read_error",
    ]

    print(f"Read-only collection started: symbol={symbol}")
    print(f"Duration: {args.duration_seconds}s; output: {output}")

    try:
        with output.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()

            while time.monotonic() - started < args.duration_seconds:
                loop_started = time.monotonic()
                read_start_ns = monotonic_ns()
                read_start_utc = datetime.now(timezone.utc)

                row = {field: None for field in fields}
                row.update({
                    "captured_at_utc": read_start_utc.isoformat(),
                    "capture_monotonic_ns": read_start_ns,
                    "symbol": symbol,
                    "ntp_verified": False,
                    "m1_closed_open_epochs_raw": "[]",
                    "m5_closed_open_epochs_raw": "[]",
                    "read_error": "",
                })

                if (
                    loop_started - last_ntp_check
                    >= args.ntp_interval_seconds
                ):
                    ntp_result = verifier.verify()
                    last_ntp_check = loop_started

                if ntp_result is not None:
                    row["ntp_verified"] = ntp_result.verified
                    row["ntp_checked_at_unix"] = (
                        ntp_result.checked_at_unix
                    )
                    row["ntp_offset_seconds"] = (
                        ntp_result.offset_seconds
                    )
                    row["ntp_uncertainty_seconds"] = (
                        ntp_result.uncertainty_seconds
                    )

                try:
                    account = mt5.account_info()
                    tick = mt5.symbol_info_tick(symbol)

                    rates_m1 = mt5.copy_rates_from_pos(
                        symbol, mt5.TIMEFRAME_M1, 1, 3
                    )
                    rates_m5 = mt5.copy_rates_from_pos(
                        symbol, mt5.TIMEFRAME_M5, 1, 3
                    )

                    if account is None or tick is None:
                        raise RuntimeError(
                            f"MT5 data unavailable: {mt5.last_error()}"
                        )

                    if rates_m1 is None or rates_m5 is None:
                        raise RuntimeError(
                            f"MT5 candle data unavailable: {mt5.last_error()}"
                        )

                    row["account_login"] = int(account.login)
                    row["broker_server"] = str(account.server)

                    row["tick_time_msc_raw"] = int(tick.time_msc)
                    row["tick_time_epoch_raw"] = (
                        int(tick.time_msc) / 1000.0
                    )

                    row["m1_closed_open_epochs_raw"] = json_epochs(
                        rates_m1
                    )
                    row["m5_closed_open_epochs_raw"] = json_epochs(
                        rates_m5
                    )

                    receipt_utc = datetime.now(
                        timezone.utc
                    ).timestamp()

                    if (
                        ntp_result is not None
                        and ntp_result.verified
                        and ntp_result.offset_seconds is not None
                    ):
                        corrected_utc = (
                            receipt_utc + ntp_result.offset_seconds
                        )

                        row["tick_minus_corrected_utc_seconds"] = (
                            row["tick_time_epoch_raw"] - corrected_utc
                        )

                        m1_epochs = [
                            int(rate["time"]) for rate in rates_m1
                        ]
                        m5_epochs = [
                            int(rate["time"]) for rate in rates_m5
                        ]

                        if m1_epochs:
                            row[
                                "latest_m1_open_minus_corrected_utc_seconds"
                            ] = max(m1_epochs) - corrected_utc

                        if m5_epochs:
                            row[
                                "latest_m5_open_minus_corrected_utc_seconds"
                            ] = max(m5_epochs) - corrected_utc

                except Exception as exc:
                    row["read_error"] = (
                        f"{type(exc).__name__}: {exc}"
                    )

                read_end_ns = monotonic_ns()

                row["read_duration_seconds"] = (
                    read_end_ns - read_start_ns
                ) / 1_000_000_000

                row["capture_elapsed_seconds"] = (
                    time.monotonic() - started
                )

                writer.writerow(row)
                handle.flush()

                remaining = args.interval_seconds - (
                    time.monotonic() - loop_started
                )

                if remaining > 0:
                    time.sleep(remaining)

    except KeyboardInterrupt:
        print("Collection interrupted by user.")

    finally:
        adapter.shutdown()

    print(
        f"Collection finished. Evidence saved to: {output.resolve()}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
