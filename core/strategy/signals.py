"""
Signal generation pipeline.
Orchestrates the full flow: fetch venue markets -> compare against forecast ->
size per-venue positions -> emit normalized signals.
"""

import logging
from datetime import datetime, timezone
from typing import Optional

from config.settings import (
    CITIES,
    CONTRARIAN_DISCOUNT,
    ENABLE_KALSHI,
    ENABLE_POLYMARKET,
    KALSHI_PAPER_BANKROLL,
    MAX_FORECAST_DAYS,
    MIN_VOLUME,
    POLYMARKET_PAPER_BANKROLL,
)
from core.data.ensemble import get_full_distribution
from core.data.kalshi import get_active_temperature_events as get_active_kalshi_temperature_events
from core.data.nws import get_forecast_high
from core.data.polymarket import (
    fetch_event_by_slug,
    get_normalized_temperature_events as get_active_polymarket_temperature_events,
    parse_market_buckets,
    extract_event_city,
    extract_event_date,
)
from core.strategy.edge import analyze_event_buckets, rank_opportunities
from core.strategy.kelly import calculate_trade_size

logger = logging.getLogger(__name__)

DEFAULT_VENUE_BANKROLLS = {
    "polymarket": POLYMARKET_PAPER_BANKROLL,
    "kalshi": KALSHI_PAPER_BANKROLL,
}


def _coerce_venue_value(value, venue: str, default: float) -> float:
    if isinstance(value, dict):
        return float(value.get(venue, default))
    if value is None:
        return default
    return float(value)


def _get_enabled_events() -> list[dict]:
    events = []
    if ENABLE_POLYMARKET:
        try:
            poly_events = get_active_polymarket_temperature_events()
            logger.info(f"Loaded {len(poly_events)} active Polymarket temperature events")
            events.extend(poly_events)
        except Exception as exc:
            logger.error(f"Failed to load Polymarket events: {exc}", exc_info=True)
    if ENABLE_KALSHI:
        try:
            kalshi_events = get_active_kalshi_temperature_events()
            logger.info(f"Loaded {len(kalshi_events)} active Kalshi temperature events")
            events.extend(kalshi_events)
        except Exception as exc:
            logger.error(f"Failed to load Kalshi events: {exc}", exc_info=True)
    return events


def make_signal(bucket: dict, event: dict, nws_forecast: Optional[dict],
                trade_size: dict, ensemble_meta: dict) -> dict:
    """Construct a standardized venue-aware signal dict."""
    side = bucket.get("preferred_side") or ("BUY" if bucket.get("edge", 0) > 0 else "SELL")
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "venue": event.get("venue", "polymarket"),
        "event_title": event.get("event_title", ""),
        "event_id": event.get("event_id"),
        "venue_event_id": event.get("venue_event_id", event.get("event_id")),
        "city": event.get("city"),
        "target_date": event.get("target_date"),
        "bucket_question": bucket.get("question", ""),
        "market_id": bucket.get("market_id"),
        "venue_market_id": bucket.get("venue_market_id", bucket.get("market_id")),
        "yes_token_id": bucket.get("yes_token_id"),
        "no_token_id": bucket.get("no_token_id"),
        "condition_id": bucket.get("condition_id"),
        "neg_risk": bucket.get("neg_risk", False),
        "market_prob": bucket.get("market_prob", 0),
        "yes_price": bucket.get("yes_price", bucket.get("market_prob", 0)),
        "no_price": bucket.get("no_price"),
        "entry_price": bucket.get("selected_price", bucket.get("market_prob", 0)),
        "fee_pct": bucket.get("selected_fee_pct", 0.0),
        "ensemble_prob": bucket.get("ensemble_prob", 0),
        "selected_prob": bucket.get("selected_prob", 0),
        "edge": bucket.get("edge", 0),
        "yes_edge": bucket.get("yes_edge"),
        "no_edge": bucket.get("no_edge"),
        "signal": bucket.get("signal", "hold"),
        "side": side,
        "temp_low": bucket.get("temp_low"),
        "temp_high": bucket.get("temp_high"),
        "is_fahrenheit": bucket.get("is_fahrenheit", True),
        "trade_size": trade_size.get("size", 0),
        "kelly_pct": trade_size.get("kelly_pct", 0),
        "capped_by": trade_size.get("capped_by"),
        "skip_reason": trade_size.get("skip_reason"),
        "nws_forecast": nws_forecast,
        "ensemble_meta": ensemble_meta,
        "yes_bid": bucket.get("yes_bid"),
        "no_bid": bucket.get("no_bid"),
    }


def _is_consensus_trade(bucket: dict) -> bool:
    model_side_prob = bucket.get("selected_prob", 0)
    market_side_prob = bucket.get("selected_price", 0)
    return (
        (model_side_prob >= 0.5 and market_side_prob >= 0.5)
        or (model_side_prob < 0.5 and market_side_prob < 0.5)
    )


def _scan_event(event: dict, bankrolls, daily_pnls) -> list[dict]:
    title = event.get("event_title", "")
    venue = event.get("venue", "polymarket")
    city_key = event.get("city")
    target_date = event.get("target_date")

    if not city_key or not target_date:
        logger.debug(f"Skipping event (no city/date): {title}")
        return []

    try:
        event_date = datetime.strptime(target_date, "%Y-%m-%d").date()
        today = datetime.now(timezone.utc).date()
        days_ahead = (event_date - today).days
        if days_ahead <= 0:
            logger.debug(f"Skipping past/today event: {title} (date={target_date})")
            return []
        if days_ahead > MAX_FORECAST_DAYS:
            logger.debug(f"Skipping distant event: {title} ({days_ahead} days ahead, max={MAX_FORECAST_DAYS})")
            return []
        logger.info(f"  Forecast horizon: {days_ahead} day(s) ahead")
    except ValueError:
        pass

    volume = event.get("volume", 0) or 0
    if volume < MIN_VOLUME:
        logger.debug(f"Skipping low-volume event: {title} (${volume:,.0f})")
        return []

    logger.info(f"\nAnalyzing [{venue}]: {title}")
    logger.info(f"  City: {city_key} | Date: {target_date} | Volume: ${volume:,.0f}")

    buckets = event.get("buckets", [])
    if not buckets:
        logger.warning(f"  No buckets parsed for {title}")
        return []

    nws_forecast = None
    city_cfg = CITIES.get(city_key, {})
    if city_cfg.get("nws_available"):
        nws_forecast = get_forecast_high(city_key, target_date)
        if nws_forecast:
            logger.info(
                f"  NWS Forecast: {nws_forecast['temp']}°{nws_forecast['unit']} "
                f"({nws_forecast.get('short_forecast', '')})"
            )

    enriched = get_full_distribution(city_key, target_date, buckets)
    if not enriched:
        logger.warning(f"  Ensemble data unavailable for {city_key} on {target_date}")
        return []

    analyzed = analyze_event_buckets(enriched, venue=venue)
    opportunities = rank_opportunities(analyzed)
    if not opportunities:
        logger.info("  No tradeable edges found")
        return []

    logger.info(f"  Found {len(opportunities)} tradeable buckets")

    venue_bankroll = _coerce_venue_value(bankrolls, venue, DEFAULT_VENUE_BANKROLLS.get(venue, 1000.0))
    venue_daily_pnl = _coerce_venue_value(daily_pnls, venue, 0.0)
    ensemble_meta = enriched[0].get("ensemble_meta", {}) if enriched else {}

    signals = []
    for bucket in opportunities:
        edge = abs(bucket["edge"])
        selected_prob = bucket.get("selected_prob", 0)
        trade_size = calculate_trade_size(edge, selected_prob, venue_bankroll, venue_daily_pnl)

        is_consensus = _is_consensus_trade(bucket)
        is_contrarian = not is_consensus
        if is_contrarian and trade_size.get("size", 0) > 0:
            original_size = trade_size["size"]
            trade_size["size"] = round(original_size * CONTRARIAN_DISCOUNT, 2)
            trade_size["capped_by"] = f"contrarian_discount ({CONTRARIAN_DISCOUNT:.0%})"
            logger.info(
                f"  Contrarian trade: ${original_size:.2f} -> ${trade_size['size']:.2f} "
                f"(model={selected_prob:.0%} vs price={bucket.get('selected_price', 0):.0%})"
            )

        signal = make_signal(bucket, event, nws_forecast, trade_size, ensemble_meta)
        signal["is_contrarian"] = is_contrarian
        signals.append(signal)

    return signals


def scan_all_markets(bankroll=None,
                     daily_pnl=0.0) -> list[dict]:
    """
    Full multi-venue market scan pipeline.

    Accepts either a single bankroll float or a dict keyed by venue.
    """
    logger.info("=" * 60)
    logger.info("Starting full market scan...")
    logger.info("=" * 60)

    bankroll = bankroll if bankroll is not None else DEFAULT_VENUE_BANKROLLS

    events = _get_enabled_events()
    logger.info(f"Found {len(events)} active temperature events across enabled venues")

    all_signals = []
    for event in events:
        all_signals.extend(_scan_event(event, bankroll, daily_pnl))

    all_signals.sort(key=lambda s: abs(s.get("edge", 0)), reverse=True)
    logger.info(f"\nScan complete: {len(all_signals)} actionable signals across {len(events)} events")
    return all_signals


def scan_specific_event(event_slug: str, bankroll=None,
                        daily_pnl: float = 0.0,
                        venue: str = "polymarket") -> list[dict]:
    """Scan a single event. Polymarket slugs are supported directly."""
    if venue != "polymarket":
        logger.warning("scan_specific_event currently supports only Polymarket slugs")
        return []
    bankroll = bankroll if bankroll is not None else DEFAULT_VENUE_BANKROLLS

    event = fetch_event_by_slug(event_slug)
    if not event:
        logger.warning(f"Event not found: {event_slug}")
        return []

    city_key = extract_event_city(event)
    target_date = extract_event_date(event)
    if not city_key or not target_date:
        return []

    normalized_event = {
        "venue": "polymarket",
        "event_id": event.get("id"),
        "venue_event_id": event.get("id"),
        "event_title": event.get("title", ""),
        "city": city_key,
        "target_date": target_date,
        "volume": event.get("volume", 0) or 0,
        "buckets": parse_market_buckets(event),
    }
    return _scan_event(normalized_event, bankroll, daily_pnl)
