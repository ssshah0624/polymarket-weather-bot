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
    WEATHER_STRATEGY_VERSION,
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
from core.tuning import get_effective_strategy_params

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


def _infer_bucket_width(buckets: list[dict], reference_temp: Optional[float]) -> float:
    widths = []
    for bucket in buckets:
        low = bucket.get("temp_low")
        high = bucket.get("temp_high")
        if low is None or high is None:
            continue
        width = abs(float(high) - float(low)) or 1.0
        midpoint = (float(low) + float(high)) / 2
        distance = abs(midpoint - reference_temp) if reference_temp is not None else 0.0
        widths.append((distance, width))

    if not widths:
        return 1.0
    widths.sort(key=lambda item: item[0])
    return widths[0][1]


def representative_bucket_temp(bucket: dict, buckets: list[dict]) -> Optional[float]:
    """Return the representative temperature used for venue-implied highs."""
    low = bucket.get("temp_low")
    high = bucket.get("temp_high")

    if low is not None and high is not None:
        return (float(low) + float(high)) / 2

    if low is not None:
        width = _infer_bucket_width(buckets, float(low))
        return float(low) + width / 2

    if high is not None:
        width = _infer_bucket_width(buckets, float(high))
        return float(high) - width / 2

    return None


def implied_event_temperature(buckets: list[dict]) -> Optional[float]:
    """Compute a probability-weighted implied high temperature for an event strip."""
    weighted_sum = 0.0
    total_prob = 0.0

    for bucket in buckets:
        rep_temp = representative_bucket_temp(bucket, buckets)
        market_prob = bucket.get("yes_price", bucket.get("market_prob"))
        if rep_temp is None or market_prob is None:
            continue
        prob = float(market_prob)
        if prob <= 0:
            continue
        weighted_sum += rep_temp * prob
        total_prob += prob

    if total_prob <= 0:
        return None
    return weighted_sum / total_prob


def _candidate_summary(signal: dict) -> dict:
    stance = "against" if signal.get("is_contrarian") else "with"
    model_prob = signal.get("selected_prob", 0.0)
    price = signal.get("entry_price", signal.get("market_prob", 0.0))
    return {
        "venue": signal.get("venue", "polymarket"),
        "bucket_question": signal.get("bucket_question", ""),
        "side": signal.get("side", "BUY"),
        "edge": signal.get("edge", 0.0),
        "entry_price": price,
        "model_probability": model_prob,
        "trade_size": signal.get("trade_size", 0.0),
        "rationale": (
            f"{signal.get('side', 'BUY')} because model side probability "
            f"{model_prob:.0%} is {abs(signal.get('edge', 0.0)):.0%} away from "
            f"the venue entry price {price:.0%} ({stance} crowd)"
        ),
    }


def _selected_bet_summary(signal: dict) -> dict:
    return {
        "venue": signal.get("venue", "polymarket"),
        "bucket_question": signal.get("bucket_question", ""),
        "side": signal.get("side", "BUY"),
        "trade_size": signal.get("trade_size", 0.0),
        "entry_price": signal.get("entry_price", signal.get("market_prob", 0.0)),
        "model_probability": signal.get("selected_prob", 0.0),
        "edge": signal.get("edge", 0.0),
    }


def _merge_event_summaries(event_summaries: list[dict]) -> list[dict]:
    grouped: dict[tuple[str, str], dict] = {}

    for summary in event_summaries:
        if not summary:
            continue

        key = (summary.get("city", ""), summary.get("target_date", ""))
        row = grouped.setdefault(
            key,
            {
                "strategy_version": WEATHER_STRATEGY_VERSION,
                "city": summary.get("city", ""),
                "target_date": summary.get("target_date", ""),
                "model_expected_high": summary.get("model_expected_high"),
                "model_spread": summary.get("model_spread"),
                "model_summary": summary.get("model_summary") or {},
                "polymarket_implied_high": None,
                "kalshi_implied_high": None,
                "venue_availability": {"polymarket": False, "kalshi": False},
                "candidate_bets": [],
                "selected_bets": [],
                "skip_reasons": [],
            },
        )

        venue = summary.get("venue", "polymarket")
        row["venue_availability"][venue] = summary.get("available", True)
        row[f"{venue}_implied_high"] = summary.get("venue_implied_high")
        row["candidate_bets"].extend(summary.get("candidate_bets") or [])
        row["skip_reasons"].extend(summary.get("skip_reasons") or [])
        if row.get("model_expected_high") is None and summary.get("model_expected_high") is not None:
            row["model_expected_high"] = summary.get("model_expected_high")
        if row.get("model_spread") is None and summary.get("model_spread") is not None:
            row["model_spread"] = summary.get("model_spread")
        if not row.get("model_summary") and summary.get("model_summary"):
            row["model_summary"] = summary.get("model_summary")

    for row in grouped.values():
        deduped_reasons = []
        for reason in row["skip_reasons"]:
            if reason and reason not in deduped_reasons:
                deduped_reasons.append(reason)
        row["skip_reasons"] = deduped_reasons
        row["candidate_bets"].sort(key=lambda item: abs(item.get("edge", 0.0)), reverse=True)

    return sorted(grouped.values(), key=lambda row: (row["target_date"], row["city"]))


def finalize_scan_comparisons(comparison_rows: list[dict], executed: list[dict]) -> list[dict]:
    """Attach executed bets to comparison rows for Slack/reporting and learning."""
    executed_by_key: dict[tuple[str, str], list[dict]] = {}
    for signal in executed:
        key = (signal.get("city", ""), signal.get("target_date", ""))
        executed_by_key.setdefault(key, []).append(_selected_bet_summary(signal))

    finalized = []
    for row in comparison_rows:
        enriched = dict(row)
        key = (row.get("city", ""), row.get("target_date", ""))
        selected = executed_by_key.get(key, [])
        enriched["selected_bets"] = selected
        if enriched.get("candidate_bets") and not selected and not enriched.get("skip_reasons"):
            enriched["skip_reasons"] = ["No trade executed this cycle after sizing or dedup filters"]
        finalized.append(enriched)
    return finalized


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
        "strategy_version": WEATHER_STRATEGY_VERSION,
        "model_expected_high": ensemble_meta.get("mean"),
        "model_spread": ensemble_meta.get("spread"),
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
        return {"signals": [], "event_summary": None}

    summary = {
        "strategy_version": WEATHER_STRATEGY_VERSION,
        "venue": venue,
        "city": city_key,
        "target_date": target_date,
        "available": True,
        "model_expected_high": None,
        "model_spread": None,
        "venue_implied_high": None,
        "model_summary": {},
        "candidate_bets": [],
        "skip_reasons": [],
    }
    strategy_params = get_effective_strategy_params(venue)

    try:
        event_date = datetime.strptime(target_date, "%Y-%m-%d").date()
        today = datetime.now(timezone.utc).date()
        days_ahead = (event_date - today).days
        if days_ahead <= 0:
            logger.debug(f"Skipping past/today event: {title} (date={target_date})")
            summary["skip_reasons"].append("Skipped by forecast horizon: same-day or past event")
            return {"signals": [], "event_summary": summary}
        if days_ahead > MAX_FORECAST_DAYS:
            logger.debug(f"Skipping distant event: {title} ({days_ahead} days ahead, max={MAX_FORECAST_DAYS})")
            summary["skip_reasons"].append(
                f"Skipped by forecast horizon: {days_ahead} days ahead exceeds max {MAX_FORECAST_DAYS}"
            )
            return {"signals": [], "event_summary": summary}
        logger.info(f"  Forecast horizon: {days_ahead} day(s) ahead")
        summary["model_summary"]["forecast_horizon_days"] = days_ahead
    except ValueError:
        pass

    volume = event.get("volume", 0) or 0
    if volume < MIN_VOLUME:
        logger.debug(f"Skipping low-volume event: {title} (${volume:,.0f})")
        summary["skip_reasons"].append(f"Skipped for low volume (${volume:,.0f} < ${MIN_VOLUME:,.0f})")
        return {"signals": [], "event_summary": summary}

    logger.info(f"\nAnalyzing [{venue}]: {title}")
    logger.info(f"  City: {city_key} | Date: {target_date} | Volume: ${volume:,.0f}")

    buckets = event.get("buckets", [])
    if not buckets:
        logger.warning(f"  No buckets parsed for {title}")
        summary["skip_reasons"].append("No normalized buckets parsed for this venue event")
        return {"signals": [], "event_summary": summary}

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
        summary["skip_reasons"].append("Ensemble distribution unavailable")
        return {"signals": [], "event_summary": summary}

    analyzed = analyze_event_buckets(
        enriched,
        venue=venue,
        threshold=strategy_params.get("edge_threshold"),
        strategy_params=strategy_params,
    )
    ensemble_meta = enriched[0].get("ensemble_meta", {}) if enriched else {}
    summary["model_expected_high"] = ensemble_meta.get("mean")
    summary["model_spread"] = ensemble_meta.get("spread")
    summary["venue_implied_high"] = implied_event_temperature(analyzed)
    summary["model_summary"] = {
        **summary["model_summary"],
        "ensemble_members": ensemble_meta.get("member_count"),
        "nws_temp": (nws_forecast or {}).get("temp"),
    }

    opportunities = rank_opportunities(analyzed)
    if not opportunities:
        logger.info("  No tradeable edges found")
        summary["skip_reasons"].append("No tradeable edges found after fees")
        return {"signals": [], "event_summary": summary}

    logger.info(f"  Found {len(opportunities)} tradeable buckets")

    venue_bankroll = _coerce_venue_value(bankrolls, venue, DEFAULT_VENUE_BANKROLLS.get(venue, 1000.0))
    venue_daily_pnl = _coerce_venue_value(daily_pnls, venue, 0.0)

    signals = []
    for bucket in opportunities:
        edge = abs(bucket["edge"])
        selected_prob = bucket.get("selected_prob", 0)
        trade_size = calculate_trade_size(
            edge,
            selected_prob,
            venue_bankroll,
            venue_daily_pnl,
            fraction=strategy_params.get("kelly_fraction"),
            max_size=strategy_params.get("max_trade_size"),
            max_loss=strategy_params.get("max_daily_loss"),
        )

        is_consensus = _is_consensus_trade(bucket)
        is_contrarian = not is_consensus
        if is_contrarian and trade_size.get("size", 0) > 0:
            original_size = trade_size["size"]
            contrarian_discount = strategy_params.get("contrarian_discount", CONTRARIAN_DISCOUNT)
            trade_size["size"] = round(original_size * contrarian_discount, 2)
            trade_size["capped_by"] = f"contrarian_discount ({contrarian_discount:.0%})"
            logger.info(
                f"  Contrarian trade: ${original_size:.2f} -> ${trade_size['size']:.2f} "
                f"(model={selected_prob:.0%} vs price={bucket.get('selected_price', 0):.0%})"
            )

        signal = make_signal(bucket, event, nws_forecast, trade_size, ensemble_meta)
        signal["venue_implied_high"] = summary["venue_implied_high"]
        signal["is_contrarian"] = is_contrarian
        signals.append(signal)
        summary["candidate_bets"].append(_candidate_summary(signal))

    if signals and all(s.get("trade_size", 0) <= 0 for s in signals):
        sizing_reasons = [
            s.get("skip_reason") for s in signals if s.get("skip_reason")
        ]
        summary["skip_reasons"].extend(sizing_reasons or ["Risk sizing reduced all candidate bets to $0"])

    return {"signals": signals, "event_summary": summary}


def scan_all_markets(bankroll=None,
                     daily_pnl=0.0,
                     return_context: bool = False):
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
    event_summaries = []
    for event in events:
        result = _scan_event(event, bankroll, daily_pnl)
        all_signals.extend(result["signals"])
        if result["event_summary"]:
            event_summaries.append(result["event_summary"])

    all_signals.sort(key=lambda s: abs(s.get("edge", 0)), reverse=True)
    logger.info(f"\nScan complete: {len(all_signals)} actionable signals across {len(events)} events")
    if not return_context:
        return all_signals

    return {
        "signals": all_signals,
        "comparisons": _merge_event_summaries(event_summaries),
    }


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
    return _scan_event(normalized_event, bankroll, daily_pnl)["signals"]
