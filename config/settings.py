"""
Central configuration for the Polymarket Weather Trading Bot.
Values can be overridden via environment variables.
"""

import math
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
PRIMARY_VISIBLE_VENUE = os.getenv("PRIMARY_VISIBLE_VENUE", "kalshi").lower()
SLACK_INCLUDE_REPORT_LINKS = os.getenv("SLACK_INCLUDE_REPORT_LINKS", "false").lower() == "true"

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
_KALSHI_USE_DEMO_DEFAULT = os.getenv("KALSHI_USE_DEMO", "true").lower() == "true"
KALSHI_ENV = os.getenv("KALSHI_ENV", "demo" if _KALSHI_USE_DEMO_DEFAULT else "prod").lower()
KALSHI_USE_DEMO = KALSHI_ENV != "prod"

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
KALSHI_NWS_BLEND_WEIGHT = float(os.getenv("KALSHI_NWS_BLEND_WEIGHT", "0.65"))
KALSHI_NWS_DISAGREEMENT_THRESHOLD_F = float(os.getenv("KALSHI_NWS_DISAGREEMENT_THRESHOLD_F", "3.0"))
KALSHI_ADJACENT_SPLIT_MIN_COMBINED_PROB = float(
    os.getenv("KALSHI_ADJACENT_SPLIT_MIN_COMBINED_PROB", "0.55")
)
KALSHI_ADJACENT_SPLIT_MIN_SECOND_BUCKET_PROB = float(
    os.getenv("KALSHI_ADJACENT_SPLIT_MIN_SECOND_BUCKET_PROB", "0.20")
)
KALSHI_ADJACENT_SPLIT_BOUNDARY_DISTANCE_F = float(
    os.getenv("KALSHI_ADJACENT_SPLIT_BOUNDARY_DISTANCE_F", "0.75")
)
KALSHI_LOCAL_LADDER_MAX_BUCKETS = int(
    os.getenv("KALSHI_LOCAL_LADDER_MAX_BUCKETS", "3")
)
KALSHI_LOCAL_LADDER_MAX_DISTANCE_F = float(
    os.getenv("KALSHI_LOCAL_LADDER_MAX_DISTANCE_F", "2.0")
)
KALSHI_HEDGED_PRIMARY_WEIGHT = float(
    os.getenv("KALSHI_HEDGED_PRIMARY_WEIGHT", "0.7")
)
KALSHI_BUCKET_CENTER_TOLERANCE_F = float(
    os.getenv("KALSHI_BUCKET_CENTER_TOLERANCE_F", "0.5")
)
KALSHI_SELL_OUTSIDE_BUCKET_MARGIN_F = float(
    os.getenv("KALSHI_SELL_OUTSIDE_BUCKET_MARGIN_F", "1.0")
)
KALSHI_SKIP_MARKET_DIVERGENCE_F = float(
    os.getenv("KALSHI_SKIP_MARKET_DIVERGENCE_F", "2.0")
)
KALSHI_MAX_FORECAST_LEAD_HOURS = float(
    os.getenv("KALSHI_MAX_FORECAST_LEAD_HOURS", "24")
)
KALSHI_TRADE_NEXT_DAY_ONLY = os.getenv("KALSHI_TRADE_NEXT_DAY_ONLY", "true").lower() == "true"
KALSHI_SHADOW_INCLUDE_SAME_DAY = os.getenv("KALSHI_SHADOW_INCLUDE_SAME_DAY", "true").lower() == "true"
KALSHI_ALLOW_SAME_DAY_TRADING = os.getenv("KALSHI_ALLOW_SAME_DAY_TRADING", "false").lower() == "true"
ENABLE_POLYMARKET = os.getenv("ENABLE_POLYMARKET", "true").lower() == "true"
ENABLE_KALSHI = os.getenv("ENABLE_KALSHI", "true").lower() == "true"
POLYMARKET_PAPER_BANKROLL = float(os.getenv("POLYMARKET_PAPER_BANKROLL", "1000"))
KALSHI_PAPER_BANKROLL = float(os.getenv("KALSHI_PAPER_BANKROLL", "1000"))
WEATHER_STRATEGY_VERSION = os.getenv("WEATHER_STRATEGY_VERSION", "weather_v2")

# ============================================================
# Kalshi Live Trading Controls
# ============================================================
def _env_float(name: str, default: float) -> float:
    return float(os.getenv(name, str(default)))


def _budget_scaled_float(name: str, multiplier: float, *, minimum: float = 0.0) -> float:
    if name in os.environ:
        return float(os.getenv(name, "0"))
    return round(max(KALSHI_LIVE_BUDGET_ALLOCATION_USD * multiplier, minimum), 2)


KALSHI_LIVE_BUDGET_ALLOCATION_USD = _env_float(
    "KALSHI_LIVE_BUDGET_ALLOCATION_USD",
    float(os.getenv("KALSHI_LIVE_BANKROLL_SLICE_USD", "250")),
)
KALSHI_LIVE_ENABLED = os.getenv("KALSHI_LIVE_ENABLED", "false").lower() == "true"
KALSHI_LIVE_BANKROLL_SLICE_USD = KALSHI_LIVE_BUDGET_ALLOCATION_USD
KALSHI_LIVE_MAX_TRADE_SIZE_USD = _budget_scaled_float("KALSHI_LIVE_MAX_TRADE_SIZE_USD", 0.30, minimum=10.0)
KALSHI_LIVE_MAX_BUY_TRADE_SIZE_USD = float(
    os.getenv(
        "KALSHI_LIVE_MAX_BUY_TRADE_SIZE_USD",
        str(_budget_scaled_float("KALSHI_LIVE_MAX_BUY_TRADE_SIZE_USD", 0.10, minimum=5.0)),
    )
)
KALSHI_LIVE_MAX_SELL_TRADE_SIZE_USD = float(
    os.getenv(
        "KALSHI_LIVE_MAX_SELL_TRADE_SIZE_USD",
        str(_budget_scaled_float("KALSHI_LIVE_MAX_SELL_TRADE_SIZE_USD", 0.30, minimum=10.0)),
    )
)
KALSHI_LIVE_MAX_DAILY_LOSS_USD = _budget_scaled_float("KALSHI_LIVE_MAX_DAILY_LOSS_USD", 0.25, minimum=10.0)
KALSHI_LIVE_MAX_OPEN_EXPOSURE_USD = _budget_scaled_float("KALSHI_LIVE_MAX_OPEN_EXPOSURE_USD", 2.0, minimum=25.0)
KALSHI_LIVE_MIN_CASH_BUFFER_USD = float(os.getenv("KALSHI_LIVE_MIN_CASH_BUFFER_USD", "5"))
KALSHI_LIVE_SCALE_TO_AVAILABLE_CASH = (
    os.getenv("KALSHI_LIVE_SCALE_TO_AVAILABLE_CASH", "false").lower() == "true"
)
KALSHI_LIVE_NEXT_DAY_CAPITAL_PCT = float(os.getenv("KALSHI_LIVE_NEXT_DAY_CAPITAL_PCT", "0.65"))
KALSHI_LIVE_EMPIRICAL_RANKING_ENABLED = (
    os.getenv("KALSHI_LIVE_EMPIRICAL_RANKING_ENABLED", "true").lower() == "true"
)
KALSHI_LIVE_EMPIRICAL_LOOKBACK_DAYS = int(os.getenv("KALSHI_LIVE_EMPIRICAL_LOOKBACK_DAYS", "30"))
KALSHI_LIVE_TARGET_TOLERANCE_USD = _budget_scaled_float("KALSHI_LIVE_TARGET_TOLERANCE_USD", 0.10, minimum=5.0)
KALSHI_LIVE_SAME_DAY_TOP_UP_ENABLED = (
    os.getenv("KALSHI_LIVE_SAME_DAY_TOP_UP_ENABLED", "true").lower() == "true"
)
KALSHI_LIVE_MAX_POSITIONS = int(
    os.getenv(
        "KALSHI_LIVE_MAX_POSITIONS",
        str(max(10, min(24, math.ceil(KALSHI_LIVE_BUDGET_ALLOCATION_USD / 20.0) + 8))),
    )
)
KALSHI_LIVE_MAX_EVENT_PACKAGES = int(os.getenv("KALSHI_LIVE_MAX_EVENT_PACKAGES", "5"))
KALSHI_LIVE_CONTRARIAN_MAX_TRADE_SIZE_USD = _budget_scaled_float(
    "KALSHI_LIVE_CONTRARIAN_MAX_TRADE_SIZE_USD",
    0.06,
    minimum=3.0,
)
KALSHI_LIVE_ALLOWED_DRIFT_CENTS = int(os.getenv("KALSHI_LIVE_ALLOWED_DRIFT_CENTS", "2"))
KALSHI_LIVE_ORDER_SLIPPAGE_CENTS = int(os.getenv("KALSHI_LIVE_ORDER_SLIPPAGE_CENTS", "2"))
KALSHI_LIVE_SETTLEMENT_BUFFER_MINUTES = int(
    os.getenv("KALSHI_LIVE_SETTLEMENT_BUFFER_MINUTES", "60")
)

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
        "kalshi_cli_code": "NYC",
        "nws_available": True,
    },
    "chicago": {
        "name": "Chicago",
        "lat": 41.8781,
        "lon": -87.6298,
        "polymarket_names": ["chicago"],
        "kalshi_names": ["chicago"],
        "kalshi_series_ticker": "KXHIGHCHI",
        "kalshi_cli_code": "ORD",
        "kalshi_cli_codes": ["ORD", "CHI"],
        "nws_available": True,
    },
    "miami": {
        "name": "Miami",
        "lat": 25.7617,
        "lon": -80.1918,
        "polymarket_names": ["miami"],
        "kalshi_names": ["miami"],
        "kalshi_series_ticker": "KXHIGHMIA",
        "kalshi_cli_code": "MIA",
        "nws_available": True,
    },
    "dallas": {
        "name": "Dallas",
        "lat": 32.7767,
        "lon": -96.7970,
        "polymarket_names": ["dallas"],
        "kalshi_names": ["dallas"],
        "kalshi_series_ticker": "KXHIGHTDAL",
        "kalshi_cli_code": "DFW",
        "kalshi_cli_codes": ["DFW", "DAL"],
        "nws_available": True,
    },
    "seattle": {
        "name": "Seattle",
        "lat": 47.6062,
        "lon": -122.3321,
        "polymarket_names": ["seattle"],
        "kalshi_names": ["seattle"],
        "kalshi_series_ticker": "KXHIGHTSEA",
        "kalshi_cli_code": "SEA",
        "nws_available": True,
    },
    "atlanta": {
        "name": "Atlanta",
        "lat": 33.7490,
        "lon": -84.3880,
        "polymarket_names": ["atlanta"],
        "kalshi_names": ["atlanta"],
        "kalshi_series_ticker": "KXHIGHTATL",
        "kalshi_cli_code": "ATL",
        "nws_available": True,
    },
    "austin": {
        "name": "Austin",
        "lat": 30.2672,
        "lon": -97.7431,
        "polymarket_names": ["austin"],
        "kalshi_names": ["austin"],
        "kalshi_series_ticker": "KXHIGHAUS",
        "kalshi_cli_code": "AUS",
        "nws_available": True,
    },
    "denver": {
        "name": "Denver",
        "lat": 39.7392,
        "lon": -104.9903,
        "polymarket_names": ["denver"],
        "kalshi_names": ["denver"],
        "kalshi_series_ticker": "KXHIGHDEN",
        "kalshi_cli_code": "DEN",
        "nws_available": True,
    },
    "houston": {
        "name": "Houston",
        "lat": 29.7604,
        "lon": -95.3698,
        "polymarket_names": ["houston"],
        "kalshi_names": ["houston"],
        "kalshi_series_ticker": "KXHIGHTHOU",
        "kalshi_cli_code": "HOU",
        "nws_available": True,
    },
    "los_angeles": {
        "name": "Los Angeles",
        "lat": 34.0522,
        "lon": -118.2437,
        "polymarket_names": ["los angeles"],
        "kalshi_names": ["los angeles", "la"],
        "kalshi_series_ticker": "KXHIGHLAX",
        "kalshi_cli_code": "LAX",
        "nws_available": True,
    },
    "san_francisco": {
        "name": "San Francisco",
        "lat": 37.7749,
        "lon": -122.4194,
        "polymarket_names": ["san francisco"],
        "kalshi_names": ["san francisco"],
        "kalshi_series_ticker": "KXHIGHTSFO",
        "kalshi_cli_code": "SFO",
        "nws_available": True,
    },
}

# ============================================================
# Database
# ============================================================
DB_PATH = Path(os.getenv("DB_PATH", str(PROJECT_ROOT / "data" / "trades.db")))
KALSHI_TUNING_OVERRIDES_PATH = PROJECT_ROOT / "data" / "kalshi_tuning.json"
KALSHI_TUNING_HISTORY_PATH = PROJECT_ROOT / "data" / "kalshi_tuning_history.jsonl"

# ============================================================
# Logging
# ============================================================
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_DIR = PROJECT_ROOT / "logs"
