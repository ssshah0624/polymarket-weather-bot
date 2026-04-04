# Polymarket Weather Trading Bot

An automated trading system that exploits mispricings in Polymarket's daily "highest temperature" weather markets. The bot compares Polymarket's implied probabilities against the NOAA GFS 31-member ensemble model to find statistical edges, sizes positions using the Kelly Criterion, and executes trades.

## The Strategy

The core strategy relies on the difference between deterministic forecasts (e.g., "it will be 72°F") and probabilistic distributions. 

While retail traders often anchor to a single forecast number, this bot uses the **GFS 31-member ensemble model** to calculate the true statistical probability of a temperature falling into a specific bucket. When the ensemble probability is significantly different from the market price, the bot identifies an edge.

### Key Features
- **US Cities Only**: Focuses on 11 major US cities with reliable NWS data validation.
- **Forecast Horizon Filter**: Only trades markets 1-2 days out, avoiding the high uncertainty of long-range forecasts.
- **Contrarian Discount**: Automatically reduces position sizing when betting against the market consensus to manage variance.
- **Fee-Adjusted Edge**: Accounts for Polymarket's taker fees before calculating the true edge.
- **Continuous Learning Loop**: Automatically tracks placed bets, fetches actual observed temperatures upon market resolution, and scores wins/losses to evaluate strategy performance over time.

## Architecture

The system is built in Python and designed to run continuously on a cloud server (e.g., Digital Ocean droplet) via cron jobs.

- `core/data/`: Fetchers for Polymarket Gamma API, NWS point forecasts, and Open-Meteo GFS ensemble data.
- `core/strategy/`: Edge calculation, Kelly position sizing, and trade signal generation.
- `core/execution/`: Paper trading simulator (live trading module to be added).
- `core/database.py`: SQLite database for logging trades, market snapshots, and resolution outcomes.
- `core/resolution.py`: Tracker that fetches actual temperatures and scores resolved markets.
- `core/alerts.py`: Slack integration for daily pulses, trade summaries, and weekly learning digests.

## Deployment

The bot is designed to be deployed to a Linux server. A safe deployment script is included to ensure the database is never overwritten during updates.

### Prerequisites
- Python 3.11+
- SQLite3
- A Slack Incoming Webhook URL (for alerts)

### Setup Instructions

1. Clone the repository to your server.
2. Create a virtual environment and install dependencies:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```
3. Copy `.env.example` to `.env` and configure your settings (do not commit `.env` to version control).
4. Set up the cron jobs to run the various components (see `scripts/cron_runner.py` or the provided crontab configuration).

### Safe Deployment

When pushing updates to the server, always use the provided deploy script to protect the database:

```bash
./scripts/deploy.sh
```

This script will:
1. Create a timestamped backup of the SQLite database.
2. Sync the code files (excluding `data/`, `logs/`, and `.env`).
3. Verify the database integrity after the update.

## Disclaimer

This software is for educational and research purposes only. Trading prediction markets involves significant risk. The paper trading mode should be used extensively to validate the strategy before committing real capital.
