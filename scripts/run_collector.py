#!/usr/bin/env python3
"""
Run the data collector to build the backtest dataset.
Snapshots live market prices + ensemble data periodically.

Usage:
    python scripts/run_collector.py                # Run once
    python scripts/run_collector.py --loop          # Run continuously (every 60 min)
    python scripts/run_collector.py --loop --interval 30  # Every 30 min
"""

import sys
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.alerts import setup_logging
from backtest.data_collector import collect_snapshots, run_collection_loop


def main():
    parser = argparse.ArgumentParser(description="Collect market data for backtesting")
    parser.add_argument("--loop", action="store_true", help="Run continuously")
    parser.add_argument("--interval", type=int, default=60,
                        help="Minutes between collections (default: 60)")

    args = parser.parse_args()
    setup_logging()

    if args.loop:
        print(f"Starting data collection loop (every {args.interval} min)")
        print("Press Ctrl+C to stop\n")
        run_collection_loop(interval_minutes=args.interval)
    else:
        print("Running single data collection cycle...")
        count = collect_snapshots()
        print(f"Collected {count} market snapshots")


if __name__ == "__main__":
    main()
