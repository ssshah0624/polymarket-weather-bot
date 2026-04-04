#!/usr/bin/env python3
"""
Quick scan of all active weather markets.
Prints a table of current edges without executing any trades.
Useful for manual review before enabling the bot.

Usage:
    python scripts/scan_markets.py
    python scripts/scan_markets.py --city miami
    python scripts/scan_markets.py --threshold 0.05
"""

import sys
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.alerts import setup_logging
from core.strategy.signals import scan_all_markets
from config.settings import EDGE_THRESHOLD


def main():
    parser = argparse.ArgumentParser(description="Scan weather markets for edges")
    parser.add_argument("--city", type=str, help="Filter to specific city")
    parser.add_argument("--threshold", type=float, default=EDGE_THRESHOLD,
                        help="Minimum edge threshold to display")
    parser.add_argument("--all", action="store_true",
                        help="Show all buckets, not just tradeable ones")

    args = parser.parse_args()
    setup_logging()

    print("Scanning active Polymarket weather markets...\n")

    signals = scan_all_markets(bankroll=1000.0)

    if args.city:
        signals = [s for s in signals if s.get("city") == args.city]

    if not args.all:
        signals = [s for s in signals if abs(s.get("edge", 0)) >= args.threshold]

    if not signals:
        print("No signals found matching criteria.")
        return

    # Group by event
    events = {}
    for s in signals:
        key = s.get("event_title", "Unknown")
        if key not in events:
            events[key] = []
        events[key].append(s)

    for event_title, event_signals in events.items():
        print(f"\n{'='*70}")
        print(f"  {event_title}")
        meta = event_signals[0].get("ensemble_meta", {})
        nws = event_signals[0].get("nws_forecast")
        if meta:
            print(f"  Ensemble: mean={meta.get('mean', 0):.1f}°F, "
                  f"range=[{meta.get('min', 0):.1f}, {meta.get('max', 0):.1f}], "
                  f"members={meta.get('member_count', 0)}")
        if nws:
            print(f"  NWS Forecast: {nws.get('temp', '?')}°{nws.get('unit', 'F')} "
                  f"({nws.get('short_forecast', '')})")
        print(f"{'='*70}")

        print(f"  {'Bucket':<25} {'Market':>8} {'Ensemble':>10} {'Edge':>8} {'Signal':>12} {'Size':>8}")
        print(f"  {'-'*25} {'-'*8} {'-'*10} {'-'*8} {'-'*12} {'-'*8}")

        for s in sorted(event_signals, key=lambda x: abs(x.get("edge", 0)), reverse=True):
            # Build a clean bucket label from temp range
            low = s.get('temp_low')
            high = s.get('temp_high')
            unit = "°F" if s.get('is_fahrenheit', True) else "°C"
            if low is not None and high is not None:
                if low == -999:
                    q = f"{high:.0f}{unit} or below"
                elif high == 999:
                    q = f"{low:.0f}{unit} or higher"
                else:
                    q = f"{low:.0f}-{high:.0f}{unit}"
            else:
                q = s.get("bucket_question", "")[:25]
            mkt = f"{s.get('market_prob', 0):.1%}"
            ens = f"{s.get('ensemble_prob', 0):.1%}"
            edge = f"{s.get('edge', 0)*100:+.1f}%"
            sig = s.get("signal", "hold").upper()
            size = f"${s.get('trade_size', 0):.2f}"
            print(f"  {q:<25} {mkt:>8} {ens:>10} {edge:>8} {sig:>12} {size:>8}")

    print(f"\nTotal signals: {len(signals)}")


if __name__ == "__main__":
    main()
