#!/usr/bin/env python3
"""Run the Kalshi live trading bot."""

import sys
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.alerts import setup_logging
from config.settings import KALSHI_LIVE_ENABLED, TRADING_MODE


def main():
    parser = argparse.ArgumentParser(description="Run Kalshi live trading")
    parser.add_argument("--interval", type=int, default=None, help="Seconds between scans")
    parser.add_argument("--once", action="store_true", help="Run one live scan cycle and exit")
    args = parser.parse_args()

    setup_logging()

    if TRADING_MODE != "live":
        print("ERROR: TRADING_MODE must be set to 'live' in .env")
        print("Current mode:", TRADING_MODE)
        print("\nTo enable Kalshi live trading:")
        print("  1. Set TRADING_MODE=live in .env")
        print("  2. Set KALSHI_LIVE_ENABLED=true in .env")
        sys.exit(1)
    if not KALSHI_LIVE_ENABLED:
        print("ERROR: KALSHI_LIVE_ENABLED must be true in .env")
        sys.exit(1)

    from core.execution.live import LiveTrader
    trader = LiveTrader()
    if args.once:
        print("Running single Kalshi live scan...")
        trades = trader.run_scan_cycle()
        print(f"Filled {len(trades)} live Kalshi trades")
    else:
        trader.run_loop(interval=args.interval)


if __name__ == "__main__":
    main()
