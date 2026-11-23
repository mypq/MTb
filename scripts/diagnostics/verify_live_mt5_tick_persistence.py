from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.config import get_settings
from backend.database import Database
from backend.database_migrations import upgrade_database
from backend.mt5.adapter import MT5Adapter
from backend.mt5.broker_tick_monitor import BrokerTickMonitor
from backend.mt5.broker_tick_worker import BrokerTickMonitorWorker
from backend.mt5.quote_repository import BrokerQuoteRepository


def main() -> int:
    settings = get_settings()
    if not settings.MT5_ENABLED:
        raise RuntimeError("MT5_ENABLED must be true in .env.")
    symbol = settings.MT5_SYMBOL.strip()
    if not symbol:
        raise RuntimeError("MT5_SYMBOL is empty in .env.")

    adapter = MT5Adapter(settings=settings)
    database = Database(settings.DATABASE_URL)
    try:
        connection = adapter.connect()
        state = getattr(connection.state, "value", connection.state)
        if state != "CONNECTED":
            raise RuntimeError(f"MT5 connection failed: {connection.message}")

        database.initialize()
        upgrade_database()
        repository = BrokerQuoteRepository(database)
        worker = BrokerTickMonitorWorker(
            adapter,
            BrokerTickMonitor(stale_after_seconds=15.0),
            symbol=symbol,
            interval_seconds=1.0,
            quote_repository=repository,
        )

        # This is the same worker -> repository path used by the live app.
        worker.check_once()
        worker_status = worker.status()
        account = adapter.get_account_info()
        stored = repository.get_latest_sample(
            symbol=symbol,
            account_login=account.login,
            broker_server=account.server,
        )
        if stored is None:
            raise RuntimeError("No readable persisted quote was found.")

        before = repository.count_for_identity(
            symbol=symbol,
            account_login=account.login,
            broker_server=account.server,
        )
        duplicate = repository.save_sample(stored)
        after = repository.count_for_identity(
            symbol=symbol,
            account_login=account.login,
            broker_server=account.server,
        )
        if not duplicate.duplicate or duplicate.inserted or before != after:
            raise RuntimeError("Exact duplicate quote was not safely rejected.")

        if stored.ask < stored.bid or stored.symbol != symbol:
            raise RuntimeError("Persisted quote integrity check failed.")

        report = {
            "status": "PASSED",
            "checked_at_utc": datetime.now(timezone.utc).isoformat(),
            "symbol": stored.symbol,
            "broker_server": stored.broker_server,
            "account_login": stored.account_login,
            "connection": state,
            "worker_persistence": worker_status["persistence"],
            "quote": {
                "broker_time_msc_raw": stored.broker_time_msc,
                "received_at_utc": stored.received_at_utc.isoformat(),
                "received_monotonic_ns": stored.received_monotonic_ns,
                "bid": stored.bid,
                "ask": stored.ask,
                "spread": stored.ask - stored.bid,
                "source": stored.source,
            },
            "readback": {
                "found": True,
                "duplicate_inserted": duplicate.inserted,
                "duplicate_detected": duplicate.duplicate,
                "rows_before_duplicate": before,
                "rows_after_duplicate": after,
            },
            "timestamp_policy": (
                "Broker timestamp remains raw milliseconds; receipt time is "
                "stored separately as UTC. No broker offset conversion is performed."
            ),
        }
        output_path = PROJECT_ROOT / "diagnostics" / "mt5_live_tick_persistence.json"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print("LIVE MT5 TICK PERSISTENCE RESULT")
        print(json.dumps(report, indent=2))
        print(f"Evidence file: {output_path}")
        return 0
    finally:
        database.dispose()
        adapter.shutdown()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(
            "LIVE MT5 TICK PERSISTENCE FAILED: "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        raise SystemExit(1)
