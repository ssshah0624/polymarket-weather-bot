#!/usr/bin/env python3
"""
Run a backtest of the weather trading strategy.

Usage:
    python scripts/run_backtest.py --city nyc --start 2026-01-01 --end 2026-03-28
    python scripts/run_backtest.py --city miami --start 2026-02-01 --end 2026-03-28 --bankroll 5000
    python scripts/run_backtest.py --all --start 2026-01-01 --end 2026-03-28
"""

import sys
import argparse
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.alerts import setup_logging
from backtest.engine import run_synthetic_backtest, save_backtest_results
from config.settings import CITIES


def main():
    parser = argparse.ArgumentParser(description="Run weather trading backtest")
    parser.add_argument("--city", type=str, help="City key (e.g., nyc, miami, chicago)")
    parser.add_argument("--all", action="store_true", help="Run backtest for all cities")
    parser.add_argument("--start", type=str, required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--bankroll", type=float, default=1000.0, help="Starting bankroll (default: $1000)")
    parser.add_argument("--edge-threshold", type=float, default=None, help="Edge threshold override")
    parser.add_argument("--kelly-fraction", type=float, default=None, help="Kelly fraction override")

    args = parser.parse_args()
    setup_logging()

    cities = list(CITIES.keys()) if args.all else [args.city]

    if not args.all and not args.city:
        parser.error("Must specify --city or --all")

    for city_key in cities:
        if city_key not in CITIES:
            print(f"Unknown city: {city_key}. Available: {', '.join(CITIES.keys())}")
            continue

        print(f"\n{'='*60}")
        print(f"Backtesting: {CITIES[city_key]['name']} ({args.start} to {args.end})")
        print(f"{'='*60}")

        results = run_synthetic_backtest(
            city_key=city_key,
            start_date=args.start,
            end_date=args.end,
            bankroll=args.bankroll,
            edge_threshold=args.edge_threshold,
            kelly_fraction=args.kelly_fraction,
        )

        if "error" in results:
            print(f"  Error: {results['error']}")
            continue

        # Print summary
        print(f"\n  Results:")
        print(f"  {'Total Trades:':<20} {results['total_trades']}")
        print(f"  {'Wins:':<20} {results['wins']}")
        print(f"  {'Losses:':<20} {results['losses']}")
        print(f"  {'Win Rate:':<20} {results['win_rate']:.1%}")
        print(f"  {'Total P&L:':<20} ${results['total_pnl']:+.2f}")
        print(f"  {'Final Bankroll:':<20} ${results['final_bankroll']:.2f}")
        print(f"  {'Max Drawdown:':<20} ${results['max_drawdown']:.2f}")
        print(f"  {'Sharpe Ratio:':<20} {results['sharpe_ratio']:.2f}")
        print(f"  {'Avg Edge:':<20} {results['avg_edge']:.1%}")

        # Save results
        save_backtest_results(results)
        print(f"\n  Results saved to backtest/results/")


if __name__ == "__main__":
    main()
