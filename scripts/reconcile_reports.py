#!/usr/bin/env python3
"""
Backfill saved scan reports or authenticated Kalshi fills into the trade ledger.

Usage:
    python scripts/reconcile_reports.py
    python scripts/reconcile_reports.py --venue kalshi --resolve
    python scripts/reconcile_reports.py --live-fills
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.alerts import setup_logging
from core.reconciliation import backfill_kalshi_live_fills, backfill_scan_reports
from core.resolution import resolve_pending_trades


def main():
    parser = argparse.ArgumentParser(description="Backfill scan reports into the DB")
    parser.add_argument("--venue", default="kalshi", help="Venue to backfill (default: kalshi)")
    parser.add_argument("--mode", default="paper", choices=["paper", "live"], help="Trading mode")
    parser.add_argument("--live-fills", action="store_true", help="Backfill authenticated Kalshi live fills")
    parser.add_argument("--resolve", action="store_true", help="Resolve eligible backfilled trades after import")
    args = parser.parse_args()

    setup_logging()
    if args.live_fills:
        summary = backfill_kalshi_live_fills()
        print(
            f"Parsed {summary['parsed']} authenticated Kalshi fill orders | "
            f"inserted {summary['inserted']} | skipped {summary['skipped']}"
        )
        args.venue = "kalshi"
        args.mode = "live"
    else:
        summary = backfill_scan_reports(venue=args.venue, mode=args.mode)
        print(
            f"Parsed {summary['parsed']} {args.venue} trades from reports | "
            f"inserted {summary['inserted']} | skipped {summary['skipped']}"
        )

    if args.resolve:
        resolution = resolve_pending_trades(mode=args.mode, venue=args.venue)
        print(
            f"Resolved {resolution['resolved']} trades | "
            f"{resolution['wins']}W/{resolution['losses']}L | "
            f"${resolution['pnl']:+.2f}"
        )


if __name__ == "__main__":
    main()
