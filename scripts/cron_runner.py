#!/usr/bin/env python3
"""
Master cron runner — dispatches scheduled tasks.

Usage:
    python scripts/cron_runner.py scan       # Scan markets + paper trade
    python scripts/cron_runner.py collect     # Snapshot market prices
    python scripts/cron_runner.py resolve     # Resolve yesterday's trades + daily recap
    python scripts/cron_runner.py digest      # Weekly learning digest
"""

import sys
import traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.alerts import setup_logging, alert_error


def run_scan():
    """Scan markets and execute paper trades."""
    from core.execution.paper import PaperTrader
    trader = PaperTrader(initial_bankroll=1000.0)
    signals = trader.run_scan_cycle()
    print(f"Scan complete: {len(signals)} trades executed")


def run_collect():
    """Collect market price snapshots."""
    from backtest.data_collector import collect_snapshots
    count = collect_snapshots()
    print(f"Collected {count} snapshots")


def run_resolve():
    """Resolve trades and send daily recap."""
    from core.resolution import run_daily_recap
    result = run_daily_recap(mode="paper")
    print(f"Resolved {result['resolved']} trades: {result['wins']}W/{result['losses']}L")


def run_digest():
    """Send weekly learning digest."""
    from core.learning import send_weekly_digest
    send_weekly_digest(mode="paper")
    print("Weekly digest sent")


COMMANDS = {
    "scan": run_scan,
    "collect": run_collect,
    "resolve": run_resolve,
    "digest": run_digest,
}


def main():
    setup_logging()

    if len(sys.argv) < 2 or sys.argv[1] not in COMMANDS:
        print(f"Usage: {sys.argv[0]} <{'|'.join(COMMANDS.keys())}>")
        sys.exit(1)

    cmd = sys.argv[1]
    try:
        COMMANDS[cmd]()
    except Exception as e:
        error_msg = f"{cmd} failed: {e}\n{traceback.format_exc()}"
        print(error_msg, file=sys.stderr)
        try:
            alert_error(str(e), context=cmd)
        except Exception:
            pass
        sys.exit(1)


if __name__ == "__main__":
    main()
