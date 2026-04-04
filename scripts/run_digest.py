#!/usr/bin/env python3
"""
Run weekly learning digest.
Analyzes the past week's resolved trades and sends insights to Slack.

Usage:
    python scripts/run_digest.py
    python scripts/run_digest.py --mode live --days 14
"""

import sys
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.alerts import setup_logging
from core.learning import send_weekly_digest


def main():
    parser = argparse.ArgumentParser(description="Send weekly learning digest")
    parser.add_argument("--mode", type=str, default="paper", choices=["paper", "live"],
                        help="Trading mode (default: paper)")
    args = parser.parse_args()

    setup_logging()
    send_weekly_digest(mode=args.mode)


if __name__ == "__main__":
    main()
