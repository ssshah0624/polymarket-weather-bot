#!/usr/bin/env python3
"""
Run the live trading bot.
REQUIRES: Polymarket credentials in .env

Usage:
    python scripts/run_live.py

NOTE: This is a stub. Live trading will be enabled in Phase 4.
For now, use run_paper.py to validate the strategy.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.alerts import setup_logging
from config.settings import TRADING_MODE


def main():
    setup_logging()

    if TRADING_MODE != "live":
        print("ERROR: TRADING_MODE must be set to 'live' in .env")
        print("Current mode:", TRADING_MODE)
        print("\nTo enable live trading:")
        print("  1. Set TRADING_MODE=live in .env")
        print("  2. Set POLYMARKET_PRIVATE_KEY in .env")
        print("  3. Set POLYMARKET_FUNDER_ADDRESS in .env")
        print("  4. Install py-clob-client: pip install py-clob-client")
        sys.exit(1)

    from core.execution.live import LiveTrader
    trader = LiveTrader()
    trader.run_loop()


if __name__ == "__main__":
    main()
