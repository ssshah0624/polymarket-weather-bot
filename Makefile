.PHONY: install scan paper backtest collect resolve digest live test clean

# ============================================================
# Setup
# ============================================================

install:
	pip install -r requirements.txt

install-live:
	pip install -r requirements.txt
	pip install py-clob-client

# ============================================================
# Running
# ============================================================

## Scan all active markets and print edges (no trades)
scan:
	python scripts/scan_markets.py

## Scan with lower threshold to see more signals
scan-all:
	python scripts/scan_markets.py --all --threshold 0.01

## Run paper trading bot (continuous loop)
paper:
	python scripts/run_paper.py

## Run a single paper trading scan
paper-once:
	python scripts/run_paper.py --once

## Run backtest for a specific city
## Usage: make backtest CITY=nyc START=2026-01-01 END=2026-03-28
backtest:
	python scripts/run_backtest.py --city $(CITY) --start $(START) --end $(END)

## Run backtest for all cities
backtest-all:
	python scripts/run_backtest.py --all --start $(START) --end $(END)

## Run data collector (single snapshot)
collect:
	python scripts/run_collector.py

## Run data collector (continuous loop)
collect-loop:
	python scripts/run_collector.py --loop

## Resolve yesterday's trades and send daily Slack recap
resolve:
	python scripts/run_resolve.py

## Send weekly learning digest to Slack
digest:
	python scripts/run_digest.py

## Historical P&L analysis
historical:
	python scripts/quick_historical.py --days 3

## Run live trading bot (requires credentials)
live:
	python scripts/run_live.py

# ============================================================
# Development
# ============================================================

test:
	python -m pytest tests/ -v

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	rm -f data/trades.db
	rm -f backtest/results/*.csv backtest/results/*.json
	rm -rf logs/
