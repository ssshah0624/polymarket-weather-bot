#!/usr/bin/env python3
"""
Run the paper trading simulator.
Scans live Polymarket weather markets, calculates edges using GFS ensemble data,
and simulates trades with a virtual bankroll. Sends alerts to Slack.

Usage:
    python scripts/run_paper.py
    python scripts/run_paper.py --bankroll 5000 --interval 300
    python scripts/run_paper.py --once   # Single scan, no loop
"""

import sys
import argparse
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.alerts import setup_logging
from core.execution.paper import PaperTrader


def main():
    parser = argparse.ArgumentParser(description="Run paper trading simulator")
    parser.add_argument("--bankroll", type=float, default=1000.0,
                        help="Starting virtual bankroll (default: $1000)")
    parser.add_argument("--interval", type=int, default=None,
                        help="Seconds between scans (default: from config)")
    parser.add_argument("--once", action="store_true",
                        help="Run a single scan cycle and exit")

    args = parser.parse_args()
    setup_logging()

    trader = PaperTrader(initial_bankroll=args.bankroll)

    if args.once:
        print("Running single paper trading scan...")
        signals = trader.run_scan_cycle()
        print(f"\nExecuted {len(signals)} paper trades")

        if signals:
            print("\nSignals:")
            for s in signals:
                print(f"  {s['side']:>4} ${s['trade_size']:.2f} | "
                      f"{s['event_title'][:40]} | "
                      f"Edge: {s['edge']*100:+.1f}%")

        status = trader.get_status()
        print(f"\nBankroll: ${status['bankroll']:.2f}")
    else:
        print(f"Starting paper trading bot (bankroll: ${args.bankroll:.2f})")
        print("Press Ctrl+C to stop\n")
        trader.run_loop(interval=args.interval)


if __name__ == "__main__":
    main()
