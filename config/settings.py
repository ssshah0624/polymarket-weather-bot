"""
Central configuration for the Polymarket Weather Trading Bot.
Values can be overridden via environment variables.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

# ============================================================
# Trading Mode
# ============================================================
TRADING_MODE = os.getenv("TRADING_MODE", "paper")  # backtest | paper | live

# ============================================================
# Slack Alerts
# ============================================================
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")

# ============================================================
# Polymarket Credentials (live trading only)
# ============================================================
POLYMARKET_PRIVATE_KEY = os.getenv("POLYMARKET_PRIVATE_KEY", "")
POLYMARKET_FUNDER_ADDRESS = os.getenv("POLYMARKET_FUNDER_ADDRESS", "")
POLYMARKET_CHAIN_ID = 137  # Polygon mainnet

# ============================================================
# Kalshi Credentials (live trading only)
# ============================================================
KALSHI_API_KEY_ID = os.getenv("KALSHI_API_KEY_ID", "")
KALSHI_PRIVATE_KEY_PATH = os.getenv("KALSHI_PRIVATE_KEY_PATH", "")
KALSHI_USE_DEMO = os.getenv("KALSHI_USE_DEMO", "true").lower() == "true"

# ============================================================
# API Endpoints
# ============================================================
GAMMA_API_BASE = "https://gamma-api.polymarket.com"
CLOB_API_BASE = "https://clob.polymarket.com"
DATA_API_BASE = "https://data-api.polymarket.com"
NWS_API_BASE = "https://api.weather.gov"
ENSEMBLE_API_BASE = "https://ensemble-api.open-meteo.com/v1/ensemble"
KALSHI_API_BASE = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_DEMO_API_BASE = "https://demo-api.kalshi.co/trade-api/v2"

# ============================================================
# Strategy Parameters
# ============================================================
EDGE_THRESHOLD = float(os.getenv("EDGE_THRESHOLD", "0.08"))       # 8% minimum edge to trade
KELLY_FRACTION = float(os.getenv("KELLY_FRACTION", "0.15"))       # 15% fractional Kelly
MAX_TRADE_SIZE = float(os.getenv("MAX_TRADE_SIZE", "50"))         # Max $50 per trade
MAX_DAILY_LOSS = float(os.getenv("MAX_DAILY_LOSS", "200"))        # Stop trading after $200 daily loss
MIN_VOLUME = float(os.getenv("MIN_VOLUME", "1000"))               # Skip markets with < $1k volume
SCAN_INTERVAL_SECONDS = int(os.getenv("SCAN_INTERVAL_SECONDS", "300"))  # 5 minutes
MAX_FORECAST_DAYS = int(os.getenv("MAX_FORECAST_DAYS", "2"))      # Only trade 1-2 days ahead
CONTRARIAN_DISCOUNT = float(os.getenv("CONTRARIAN_DISCOUNT", "0.6"))  # Scale down contrarian bets to 60% of normal size
KALSHI_FEE_BUFFER_PCT = float(os.getenv("KALSHI_FEE_BUFFER_PCT", "0.0"))
ENABLE_POLYMARKET = os.getenv("ENABLE_POLYMARKET", "true").lower() == "true"
ENABLE_KALSHI = os.getenv("ENABLE_KALSHI", "true").lower() == "true"
POLYMARKET_PAPER_BANKROLL = float(os.getenv("POLYMARKET_PAPER_BANKROLL", "1000"))
KALSHI_PAPER_BANKROLL = float(os.getenv("KALSHI_PAPER_BANKROLL", "1000"))

# ============================================================
# Cities Configuration — US Only
# ============================================================
CITIES = {
    "nyc": {
        "name": "New York City",
        "lat": 40.7128,
        "lon": -74.0060,
        "polymarket_names": ["new york city", "nyc", "new york"],
        "kalshi_names": ["new york city", "nyc", "new york"],
        "kalshi_series_ticker": "KXHIGHNY",
        "nws_available": True,
    },
    "chicago": {
        "name": "Chicago",
        "lat": 41.8781,
        "lon": -87.6298,
        "polymarket_names": ["chicago"],
        "kalshi_names": ["chicago"],
        "kalshi_series_ticker": "KXHIGHCHI",
        "nws_available": True,
    },
    "miami": {
        "name": "Miami",
        "lat": 25.7617,
        "lon": -80.1918,
        "polymarket_names": ["miami"],
        "kalshi_names": ["miami"],
        "kalshi_series_ticker": "KXHIGHMIA",
        "nws_available": True,
    },
    "dallas": {
        "name": "Dallas",
        "lat": 32.7767,
        "lon": -96.7970,
        "polymarket_names": ["dallas"],
        "kalshi_names": ["dallas"],
        "kalshi_series_ticker": "KXHIGHTDAL",
        "nws_available": True,
    },
    "seattle": {
        "name": "Seattle",
        "lat": 47.6062,
        "lon": -122.3321,
        "polymarket_names": ["seattle"],
        "kalshi_names": ["seattle"],
        "kalshi_series_ticker": "KXHIGHTSEA",
        "nws_available": True,
    },
    "atlanta": {
        "name": "Atlanta",
        "lat": 33.7490,
        "lon": -84.3880,
        "polymarket_names": ["atlanta"],
        "kalshi_names": ["atlanta"],
        "kalshi_series_ticker": "KXHIGHTATL",
        "nws_available": True,
    },
    "austin": {
        "name": "Austin",
        "lat": 30.2672,
        "lon": -97.7431,
        "polymarket_names": ["austin"],
        "kalshi_names": ["austin"],
        "kalshi_series_ticker": "KXHIGHAUS",
        "nws_available": True,
    },
    "denver": {
        "name": "Denver",
        "lat": 39.7392,
        "lon": -104.9903,
        "polymarket_names": ["denver"],
        "kalshi_names": ["denver"],
        "kalshi_series_ticker": "KXHIGHDEN",
        "nws_available": True,
    },
    "houston": {
        "name": "Houston",
        "lat": 29.7604,
        "lon": -95.3698,
        "polymarket_names": ["houston"],
        "kalshi_names": ["houston"],
        "kalshi_series_ticker": "KXHIGHTHOU",
        "nws_available": True,
    },
    "los_angeles": {
        "name": "Los Angeles",
        "lat": 34.0522,
        "lon": -118.2437,
        "polymarket_names": ["los angeles"],
        "kalshi_names": ["los angeles", "la"],
        "kalshi_series_ticker": "KXHIGHLAX",
        "nws_available": True,
    },
    "san_francisco": {
        "name": "San Francisco",
        "lat": 37.7749,
        "lon": -122.4194,
        "polymarket_names": ["san francisco"],
        "kalshi_names": ["san francisco"],
        "kalshi_series_ticker": "KXHIGHTSFO",
        "nws_available": True,
    },
}

# ============================================================
# Database
# ============================================================
DB_PATH = PROJECT_ROOT / "data" / "trades.db"

# ============================================================
# Logging
# ============================================================
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_DIR = PROJECT_ROOT / "logs"
