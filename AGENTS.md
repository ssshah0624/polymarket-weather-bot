# Repository Guidelines

## Project Structure & Module Organization
Core trading logic lives in `core/`: `core/data/` fetches Polymarket and weather inputs, `core/strategy/` turns forecasts into signals and sizing, and `core/execution/` handles paper and live execution. Supporting modules such as `core/database.py`, `core/resolution.py`, and `core/alerts.py` manage persistence, settlement, and notifications. Use `backtest/` for historical simulation code, `scripts/` for runnable entry points like `run_paper.py` and `run_backtest.py`, and `config/settings.py` for environment-driven settings. Runtime state is stored in `data/` and `logs/`. Tests belong in `tests/`.

## Build, Test, and Development Commands
Set up a local environment before running anything:

```bash
python3 -m venv .venv
source .venv/bin/activate
make install
```

Use `make scan` to inspect current market edges without placing trades, `make paper-once` for a single paper-trading cycle, and `make paper` for the continuous simulator. Run backtests with `make backtest CITY=nyc START=2026-01-01 END=2026-03-28` or `make backtest-all START=... END=...`. Use `make test` to run `pytest`. Avoid `make clean` unless you intend to remove `data/trades.db`, logs, and generated backtest outputs.

## Coding Style & Naming Conventions
Follow the existing Python style: 4-space indentation, `snake_case` for modules/functions, `PascalCase` for classes, and `UPPER_CASE` for configuration constants. Keep imports explicit, prefer small focused functions, and preserve the current docstring-heavy style for entry points and non-trivial modules. No formatter or linter is configured in this repo, so write code that is already PEP 8 compliant.

## Testing Guidelines
This repository uses `pytest`, but the `tests/` package is currently minimal. Add new tests as `tests/test_<feature>.py`, and focus first on strategy calculations, market parsing, and database side effects. For script changes, pair unit coverage with a manual smoke check such as `make paper-once` or a narrow `make backtest ...`.

## Commit & Pull Request Guidelines
Git history is minimal, but the existing commit uses a short imperative subject with context. Keep commits focused and descriptive, for example `Add guardrails for low-volume markets`. Pull requests should state the trading or data-path impact, list commands run (`make test`, backtest, or smoke checks), and include screenshots or sample output when changing dashboard or reporting behavior.

## Security & Configuration Tips
Do not commit `.env`, API keys, or live trading credentials. Default to paper mode unless live trading is intentionally being tested. On deployed systems, use `./scripts/deploy.sh` so the SQLite database in `data/trades.db` is backed up before code is updated.
