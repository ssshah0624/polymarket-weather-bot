"""
Signal generation pipeline.
Orchestrates the full flow: fetch data → calculate edge → size position → emit signal.
"""

import logging
from datetime import datetime, timezone
from typing import Optional

from core.data.polymarket import (
    get_active_temperature_events,
    parse_market_buckets,
    extract_event_city,
    extract_event_date,
    fetch_event_by_slug,
    build_event_slug,
)
from core.data.nws import get_forecast_high
from core.data.ensemble import get_full_distribution
from core.strategy.edge import analyze_event_buckets, rank_opportunities
from core.strategy.kelly import calculate_trade_size
from config.settings import CITIES, MIN_VOLUME, MAX_FORECAST_DAYS, CONTRARIAN_DISCOUNT
from datetime import date as date_type, timedelta

logger = logging.getLogger(__name__)


# ============================================================
# Signal Data Structure
# ============================================================

def make_signal(bucket: dict, event: dict, nws_forecast: Optional[dict],
                trade_size: dict, ensemble_meta: dict) -> dict:
    """Construct a standardized signal dict."""
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event_title": event.get("title", ""),
        "event_id": event.get("id"),
        "city": extract_event_city(event),
        "target_date": extract_event_date(event),
        "bucket_question": bucket.get("question", ""),
        "market_id": bucket.get("market_id"),
        "yes_token_id": bucket.get("yes_token_id"),
        "no_token_id": bucket.get("no_token_id"),
        "condition_id": bucket.get("condition_id"),
        "neg_risk": bucket.get("neg_risk", False),
        "market_prob": bucket.get("market_prob", 0),
        "ensemble_prob": bucket.get("ensemble_prob", 0),
        "edge": bucket.get("edge", 0),
        "signal": bucket.get("signal", "hold"),
        "side": "BUY" if bucket.get("edge", 0) > 0 else "SELL",
        "temp_low": bucket.get("temp_low"),
        "temp_high": bucket.get("temp_high"),
        "is_fahrenheit": bucket.get("is_fahrenheit", True),
        "trade_size": trade_size.get("size", 0),
        "kelly_pct": trade_size.get("kelly_pct", 0),
        "capped_by": trade_size.get("capped_by"),
        "skip_reason": trade_size.get("skip_reason"),
        "nws_forecast": nws_forecast,
        "ensemble_meta": ensemble_meta,
    }


# ============================================================
# Full Scan Pipeline
# ============================================================

def scan_all_markets(bankroll: float = 1000.0,
                     daily_pnl: float = 0.0) -> list[dict]:
    """
    Full market scan pipeline:
    1. Fetch all active temperature events from Polymarket
    2. For each event, fetch GFS ensemble distribution
    3. Calculate edge for each bucket
    4. Size positions using Kelly criterion
    5. Return list of actionable signals
    
    Args:
        bankroll: Current available bankroll
        daily_pnl: Today's running P&L
    
    Returns:
        List of signal dicts, sorted by absolute edge (highest first)
    """
    logger.info("=" * 60)
    logger.info("Starting full market scan...")
    logger.info("=" * 60)

    # Step 1: Fetch active temperature events
    events = get_active_temperature_events()
    logger.info(f"Found {len(events)} active temperature events")

    all_signals = []

    for event in events:
        title = event.get("title", "")
        city_key = extract_event_city(event)
        target_date = extract_event_date(event)

        if not city_key or not target_date:
            logger.debug(f"Skipping event (no city/date): {title}")
            continue

        # Skip events outside the forecast horizon
        # Day 1-2 ensemble forecasts are much more accurate than Day 5+
        try:
            event_date = datetime.strptime(target_date, "%Y-%m-%d").date()
            today = datetime.now(timezone.utc).date()
            days_ahead = (event_date - today).days
            if days_ahead <= 0:
                logger.debug(f"Skipping past/today event: {title} (date={target_date})")
                continue
            if days_ahead > MAX_FORECAST_DAYS:
                logger.debug(f"Skipping distant event: {title} ({days_ahead} days ahead, max={MAX_FORECAST_DAYS})")
                continue
            logger.info(f"  Forecast horizon: {days_ahead} day(s) ahead")
        except ValueError:
            pass

        # Skip low-volume markets
        volume = event.get("volume", 0) or 0
        if volume < MIN_VOLUME:
            logger.debug(f"Skipping low-volume event: {title} (${volume:,.0f})")
            continue

        logger.info(f"\nAnalyzing: {title}")
        logger.info(f"  City: {city_key} | Date: {target_date} | Volume: ${volume:,.0f}")

        # Step 2: Parse market buckets
        buckets = parse_market_buckets(event)
        if not buckets:
            logger.warning(f"  No buckets parsed for {title}")
            continue

        # Step 3: Get NWS forecast (US cities only)
        nws_forecast = None
        city_cfg = CITIES.get(city_key, {})
        if city_cfg.get("nws_available"):
            nws_forecast = get_forecast_high(city_key, target_date)
            if nws_forecast:
                logger.info(f"  NWS Forecast: {nws_forecast['temp']}°{nws_forecast['unit']} "
                           f"({nws_forecast.get('short_forecast', '')})")

        # Step 4: Get ensemble distribution and calculate probabilities
        enriched = get_full_distribution(city_key, target_date, buckets)
        if not enriched:
            logger.warning(f"  Ensemble data unavailable for {city_key} on {target_date}")
            continue

        # Step 5: Calculate edges
        analyzed = analyze_event_buckets(enriched)

        # Step 6: Get tradeable opportunities
        opportunities = rank_opportunities(analyzed)

        if not opportunities:
            logger.info(f"  No tradeable edges found")
            continue

        logger.info(f"  Found {len(opportunities)} tradeable buckets")

        # Step 7: Size positions and create signals
        ensemble_meta = enriched[0].get("ensemble_meta", {}) if enriched else {}

        for bucket in opportunities:
            edge = bucket["edge"]
            ens_prob = bucket["ensemble_prob"]
            mkt_prob = bucket.get("market_prob", 0)

            # Determine if this is a consensus or contrarian trade
            # Consensus: both model and market agree on direction (both >50% or both <50%)
            # Contrarian: model disagrees with market majority
            is_consensus = (ens_prob >= 0.5 and mkt_prob >= 0.5) or (ens_prob < 0.5 and mkt_prob < 0.5)
            is_contrarian = not is_consensus

            # For BUY signals, use ensemble_prob; for SELL, use 1 - ensemble_prob
            if edge > 0:
                trade_size = calculate_trade_size(edge, ens_prob, bankroll, daily_pnl)
            else:
                trade_size = calculate_trade_size(abs(edge), 1 - ens_prob, bankroll, daily_pnl)

            # Apply contrarian discount: reduce size when betting against market consensus
            if is_contrarian and trade_size.get("size", 0) > 0:
                original_size = trade_size["size"]
                trade_size["size"] = round(original_size * CONTRARIAN_DISCOUNT, 2)
                trade_size["capped_by"] = f"contrarian_discount ({CONTRARIAN_DISCOUNT:.0%})"
                logger.info(f"  Contrarian trade: ${original_size:.2f} -> ${trade_size['size']:.2f} "
                           f"(model={ens_prob:.0%} vs market={mkt_prob:.0%})")

            signal = make_signal(bucket, event, nws_forecast, trade_size, ensemble_meta)
            signal["is_contrarian"] = is_contrarian
            all_signals.append(signal)

    # Sort by absolute edge
    all_signals.sort(key=lambda s: abs(s.get("edge", 0)), reverse=True)

    logger.info(f"\nScan complete: {len(all_signals)} actionable signals across {len(events)} events")
    return all_signals


def scan_specific_event(event_slug: str, bankroll: float = 1000.0,
                        daily_pnl: float = 0.0) -> list[dict]:
    """Scan a single event by slug and return signals."""
    event = fetch_event_by_slug(event_slug)
    if not event:
        logger.warning(f"Event not found: {event_slug}")
        return []

    city_key = extract_event_city(event)
    target_date = extract_event_date(event)

    if not city_key or not target_date:
        return []

    buckets = parse_market_buckets(event)
    nws_forecast = get_forecast_high(city_key, target_date) if CITIES.get(city_key, {}).get("nws_available") else None
    enriched = get_full_distribution(city_key, target_date, buckets)

    if not enriched:
        return []

    analyzed = analyze_event_buckets(enriched)
    opportunities = rank_opportunities(analyzed)
    ensemble_meta = enriched[0].get("ensemble_meta", {}) if enriched else {}

    signals = []
    for bucket in opportunities:
        edge = bucket["edge"]
        ens_prob = bucket["ensemble_prob"]
        mkt_prob = bucket.get("market_prob", 0)
        is_consensus = (ens_prob >= 0.5 and mkt_prob >= 0.5) or (ens_prob < 0.5 and mkt_prob < 0.5)
        is_contrarian = not is_consensus

        if edge > 0:
            trade_size = calculate_trade_size(edge, ens_prob, bankroll, daily_pnl)
        else:
            trade_size = calculate_trade_size(abs(edge), 1 - ens_prob, bankroll, daily_pnl)

        if is_contrarian and trade_size.get("size", 0) > 0:
            trade_size["size"] = round(trade_size["size"] * CONTRARIAN_DISCOUNT, 2)
            trade_size["capped_by"] = f"contrarian_discount ({CONTRARIAN_DISCOUNT:.0%})"

        sig = make_signal(bucket, event, nws_forecast, trade_size, ensemble_meta)
        sig["is_contrarian"] = is_contrarian
        signals.append(sig)

    return signals
