"""
Data collector for building the backtest dataset.
Runs periodically to snapshot live market prices + ensemble data.
Over time this builds a rich historical dataset for replay backtesting.
"""

import logging
from datetime import datetime, timezone

from core.data.polymarket import (
    get_active_temperature_events,
    parse_market_buckets,
    extract_event_city,
    extract_event_date,
)
from core.data.nws import get_forecast_high
from core.data.ensemble import get_full_distribution
from core.strategy.edge import analyze_event_buckets
from core.database import log_snapshot

logger = logging.getLogger(__name__)


def collect_snapshots() -> int:
    """
    Snapshot all active temperature markets with their current
    ensemble probabilities and market prices.
    
    Returns the number of snapshots collected.
    """
    logger.info("Starting data collection cycle...")

    events = get_active_temperature_events()
    count = 0

    for event in events:
        city_key = extract_event_city(event)
        target_date = extract_event_date(event)

        if not city_key or not target_date:
            continue

        buckets = parse_market_buckets(event)
        if not buckets:
            continue

        # Get ensemble data
        enriched = get_full_distribution(city_key, target_date, buckets)
        if not enriched:
            continue

        # Analyze edges
        analyzed = analyze_event_buckets(enriched)

        # Get NWS forecast
        from config.settings import CITIES
        nws = None
        if CITIES.get(city_key, {}).get("nws_available"):
            nws = get_forecast_high(city_key, target_date)

        # Log each bucket as a snapshot
        for bucket in analyzed:
            signal = {
                "event_title": event.get("title", ""),
                "city": city_key,
                "target_date": target_date,
                "bucket_question": bucket.get("question", ""),
                "market_prob": bucket.get("market_prob", 0),
                "ensemble_prob": bucket.get("ensemble_prob"),
                "edge": bucket.get("edge"),
                "signal": bucket.get("signal", ""),
                "nws_forecast": nws,
                "ensemble_meta": bucket.get("ensemble_meta", {}),
            }
            log_snapshot(signal)
            count += 1

    logger.info(f"Collected {count} market snapshots")
    return count


def run_collection_loop(interval_minutes: int = 60):
    """Run the data collector on a loop."""
    import time
    logger.info(f"Starting collection loop (every {interval_minutes} min)")

    while True:
        try:
            collect_snapshots()
        except Exception as e:
            logger.error(f"Collection error: {e}", exc_info=True)

        time.sleep(interval_minutes * 60)
