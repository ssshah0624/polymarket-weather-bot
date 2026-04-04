#!/usr/bin/env python3
"""
Run trade resolution and daily recap.
Checks all unresolved trades, fetches actual temperatures,
scores wins/losses, and sends a Slack daily recap.

Usage:
    python scripts/run_resolve.py
    python scripts/run_resolve.py --mode live
"""

import sys
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.alerts import setup_logging
from core.resolution import run_daily_recap


def main():
    parser = argparse.ArgumentParser(description="Resolve trades and send daily recap")
    parser.add_argument("--mode", type=str, default="paper", choices=["paper", "live"],
                        help="Trading mode (default: paper)")
    args = parser.parse_args()

    setup_logging()
    run_daily_recap(mode=args.mode)


if __name__ == "__main__":
    main()
