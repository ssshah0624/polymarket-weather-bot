"""
Signal generation pipeline.
Orchestrates the full flow: fetch venue markets -> compare against forecast ->
size per-venue positions -> emit normalized signals.
"""

import logging
import datetime as dt
from datetime import datetime, timezone
from typing import Optional
from zoneinfo import ZoneInfo

from config.settings import (
    CITIES,
    CONTRARIAN_DISCOUNT,
    ENABLE_KALSHI,
    ENABLE_POLYMARKET,
    KALSHI_ADJACENT_SPLIT_BOUNDARY_DISTANCE_F,
    KALSHI_ALLOW_SAME_DAY_TRADING,
    KALSHI_ADJACENT_SPLIT_MIN_COMBINED_PROB,
    KALSHI_ADJACENT_SPLIT_MIN_SECOND_BUCKET_PROB,
    KALSHI_BUCKET_CENTER_TOLERANCE_F,
    KALSHI_HEDGED_PRIMARY_WEIGHT,
    KALSHI_LOCAL_LADDER_MAX_BUCKETS,
    KALSHI_LOCAL_LADDER_MAX_DISTANCE_F,
    KALSHI_MAX_FORECAST_LEAD_HOURS,
    KALSHI_SHADOW_INCLUDE_SAME_DAY,
    KALSHI_PAPER_BANKROLL,
    KALSHI_SELL_OUTSIDE_BUCKET_MARGIN_F,
    KALSHI_SKIP_MARKET_DIVERGENCE_F,
    KALSHI_TRADE_NEXT_DAY_ONLY,
    MAX_FORECAST_DAYS,
    MIN_VOLUME,
    POLYMARKET_PAPER_BANKROLL,
    WEATHER_STRATEGY_VERSION,
)
from core.data.ensemble import get_full_distribution
from core.data.kalshi import get_active_temperature_events as get_active_kalshi_temperature_events
from core.data.nws import get_forecast_high, get_hourly_forecast_high
from core.data.nws_climate import get_climate_station_metadata
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
MARKET_TIMEZONE = ZoneInfo("America/New_York")

DEFAULT_VENUE_BANKROLLS = {
    "polymarket": POLYMARKET_PAPER_BANKROLL,
    "kalshi": KALSHI_PAPER_BANKROLL,
}


def _mark_summary(summary: dict, status: str, reason_code: str | None = None, detail: str | None = None) -> dict:
    """Attach normalized status metadata to an event summary."""
    summary["status"] = status
    summary["reason_code"] = reason_code
    summary["reason_detail"] = detail
    return summary


def _target_start_dt(target_date: str) -> datetime:
    event_date = datetime.strptime(target_date, "%Y-%m-%d").date()
    return dt.datetime(event_date.year, event_date.month, event_date.day, tzinfo=MARKET_TIMEZONE)


def _target_settlement_dt(target_date: str) -> datetime:
    event_date = datetime.strptime(target_date, "%Y-%m-%d").date() + dt.timedelta(days=1)
    return dt.datetime(event_date.year, event_date.month, event_date.day, tzinfo=MARKET_TIMEZONE)


def _trade_window_lead_hours(target_date: str, now: datetime) -> float:
    return round((_target_start_dt(target_date) - now).total_seconds() / 3600.0, 2)


def _lead_time_hours(target_date: str, now: datetime) -> float:
    return round((_target_settlement_dt(target_date) - now).total_seconds() / 3600.0, 2)


def _lead_time_bucket(lead_hours: float | None) -> str | None:
    if lead_hours is None:
        return None
    if lead_hours <= 6:
        return "0-6h"
    if lead_hours <= 12:
        return "6-12h"
    if lead_hours <= 24:
        return "12-24h"
    if lead_hours <= 36:
        return "24-36h"
    if lead_hours <= 48:
        return "36-48h"
    return "48h+"


def _log_venue_diagnostics(event_summaries: list[dict], all_signals: list[dict]):
    """Emit compact per-venue scan diagnostics so skips are attributable."""
    for venue in ("kalshi", "polymarket"):
        venue_summaries = [row for row in event_summaries if row.get("venue") == venue]
        if not venue_summaries:
            continue

        reason_counts: dict[str, int] = {}
        reason_examples: dict[str, list[str]] = {}
        analyzed = 0
        actionable_events = 0
        candidate_bets = 0
        realized_signals = 0

        for row in venue_summaries:
            status = row.get("status", "unknown")
            if status in {"analyzed", "actionable", "sized_to_zero"}:
                analyzed += 1
            if status == "actionable":
                actionable_events += 1
            candidate_bets += len(row.get("candidate_bets") or [])
            if row.get("status") != "actionable":
                code = row.get("reason_code") or "unknown"
                reason_counts[code] = reason_counts.get(code, 0) + 1
                examples = reason_examples.setdefault(code, [])
                if len(examples) < 3:
                    examples.append(
                        f"{row.get('city', '?')} {row.get('target_date', '?')} | "
                        f"{row.get('reason_detail') or row.get('skip_reasons', [''])[0]}"
                    )

        realized_signals = sum(1 for signal in all_signals if signal.get("venue") == venue and signal.get("trade_size", 0) > 0)

        headline = (
            f"Venue diagnostics [{venue}]: events={len(venue_summaries)} | analyzed={analyzed} | "
            f"actionable_events={actionable_events} | candidate_bets={candidate_bets} | signals={realized_signals}"
        )
        if reason_counts:
            headline += " | " + " ".join(
                f"{code}={count}" for code, count in sorted(reason_counts.items())
            )
        logger.info(headline)
        for code, examples in sorted(reason_examples.items()):
            for example in examples:
                logger.info("  %s: %s", code, example)


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

    if low == -999.0 and high is not None:
        width = _infer_bucket_width(buckets, float(high))
        return float(high) - width / 2

    if high == 999.0 and low is not None:
        width = _infer_bucket_width(buckets, float(low))
        return float(low) + width / 2

    if low is not None and high is not None:
        return (float(low) + float(high)) / 2

    return None


def implied_event_temperature(buckets: list[dict]) -> Optional[float]:
    """Compute a probability-weighted implied high temperature for an event strip."""
    probabilities: list[tuple[float, float]] = []
    total_prob = 0.0

    for bucket in buckets:
        rep_temp = representative_bucket_temp(bucket, buckets)
        market_prob = bucket.get("yes_price", bucket.get("market_prob"))
        if rep_temp is None or market_prob is None:
            continue
        prob = float(market_prob)
        if prob <= 0:
            continue
        probabilities.append((rep_temp, prob))
        total_prob += prob

    if total_prob <= 0:
        return None

    weighted_sum = 0.0
    for rep_temp, prob in probabilities:
        weighted_sum += rep_temp * (prob / total_prob)
    return weighted_sum


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
                "proposed_bets": [],
                "selected_bets": [],
                "skip_reasons": [],
            },
        )

        venue = summary.get("venue", "polymarket")
        row["venue_availability"][venue] = summary.get("available", True)
        row[f"{venue}_implied_high"] = summary.get("venue_implied_high")
        row["candidate_bets"].extend(summary.get("candidate_bets") or [])
        row["proposed_bets"].extend(summary.get("proposed_bets") or [])
        row["skip_reasons"].extend(summary.get("skip_reasons") or [])
        if row.get("model_expected_high") is None and summary.get("model_expected_high") is not None:
            row["model_expected_high"] = summary.get("model_expected_high")
        if row.get("model_spread") is None and summary.get("model_spread") is not None:
            row["model_spread"] = summary.get("model_spread")
        if summary.get("model_summary"):
            row["model_summary"] = {
                **(row.get("model_summary") or {}),
                **(summary.get("model_summary") or {}),
            }

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
        proposed = list(row.get("proposed_bets") or [])
        model_summary = dict(enriched.get("model_summary") or {})
        if proposed:
            model_summary["proposed_selected_bets"] = proposed
        if selected:
            enriched["selected_bets"] = selected
            model_summary["selected_bets_source"] = "executed"
        elif model_summary.get("shadow_only") and proposed:
            enriched["selected_bets"] = proposed
            model_summary["selected_bets_source"] = "proposed_shadow"
        elif model_summary.get("shadow_only"):
            enriched["selected_bets"] = []
            model_summary["proposed_selected_bets"] = []
            model_summary["selected_bets_source"] = "no_shadow_package"
        else:
            enriched["selected_bets"] = []
            if proposed:
                model_summary["selected_bets_source"] = "proposed_unexecuted"
        enriched["model_summary"] = model_summary
        if enriched.get("candidate_bets") and not selected and not enriched.get("skip_reasons"):
            enriched["skip_reasons"] = ["No trade executed this cycle after sizing or dedup filters"]
        finalized.append(enriched)
    return finalized


def make_signal(bucket: dict, event: dict, nws_forecast: Optional[dict],
                trade_size: dict, ensemble_meta: dict,
                station_meta: Optional[dict] = None) -> dict:
    """Construct a standardized venue-aware signal dict."""
    side = bucket.get("preferred_side") or ("BUY" if bucket.get("edge", 0) > 0 else "SELL")
    station_meta = station_meta or {}
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
        "settlement_station": station_meta.get("station_name"),
        "settlement_station_code": station_meta.get("issuedby"),
        "settlement_station_source": station_meta.get("source"),
        "forecast_lat": station_meta.get("lat"),
        "forecast_lon": station_meta.get("lon"),
    }


def _is_consensus_trade(bucket: dict) -> bool:
    model_side_prob = bucket.get("selected_prob", 0)
    market_side_prob = bucket.get("selected_price", 0)
    return (
        (model_side_prob >= 0.5 and market_side_prob >= 0.5)
        or (model_side_prob < 0.5 and market_side_prob < 0.5)
    )


def _kalshi_nws_anchor_temp(nws_forecast: Optional[dict]) -> float | None:
    if not nws_forecast:
        return None
    hourly_max = nws_forecast.get("hourly_max_temp")
    if hourly_max is not None:
        return float(hourly_max)
    daily_temp = nws_forecast.get("temp")
    if daily_temp is not None:
        return float(daily_temp)
    return None


def _bucket_boundary(signal_a: dict, signal_b: dict) -> Optional[float]:
    high_a = signal_a.get("temp_high")
    low_a = signal_a.get("temp_low")
    high_b = signal_b.get("temp_high")
    low_b = signal_b.get("temp_low")

    if high_a is not None and low_b is not None and abs(float(high_a) - float(low_b)) < 1e-9:
        return float(high_a)
    if high_b is not None and low_a is not None and abs(float(high_b) - float(low_a)) < 1e-9:
        return float(high_b)
    return None


def _bucket_distance_to_temp(signal: dict, temp: float | None) -> float:
    if temp is None:
        return float("inf")
    low_bound, high_bound = _bucket_outer_bounds(signal)
    if low_bound <= float(temp) <= high_bound:
        return 0.0
    if float(temp) < low_bound:
        return low_bound - float(temp)
    return float(temp) - high_bound


def _signal_midpoint(signal: dict) -> float | None:
    low = signal.get("temp_low")
    high = signal.get("temp_high")
    if low is None and high is None:
        return None
    if low == -999.0 and high is not None:
        return float(high) - 1.0
    if high == 999.0 and low is not None:
        return float(low) + 1.0
    if low is not None and high is not None:
        return (float(low) + float(high) - 1.0) / 2.0
    return float(low if low is not None else high)


def _forecast_anchor_temp(candidate_signals: list[dict]) -> float | None:
    for signal in candidate_signals:
        context = signal.get("forecast_context") or {}
        anchor = context.get("forecast_anchor_temp")
        if anchor is not None:
            return float(anchor)
    for signal in candidate_signals:
        context = signal.get("forecast_context") or {}
        anchor = context.get("ensemble_mean") or signal.get("model_expected_high")
        if anchor is not None:
            return float(anchor)
    return None


def _signal_model_prob(signal: dict) -> float:
    context = signal.get("forecast_context") or {}
    return float(context.get("selected_prob") or signal.get("selected_prob") or 0.0)


def _signal_sort_key(signal: dict, anchor_temp: float | None) -> tuple[float, float, float]:
    distance = _bucket_distance_to_temp(signal, anchor_temp)
    return (
        distance,
        -_signal_model_prob(signal),
        -abs(float(signal.get("edge", 0.0) or 0.0)),
    )


def _bucket_contains_temp(signal: dict, temp: float | None) -> bool:
    if temp is None:
        return False
    return _bucket_distance_to_temp(signal, temp) == 0.0


def _bucket_adjacent_to_temp(signal: dict, temp: float | None) -> bool:
    if temp is None:
        return False
    return abs(_bucket_distance_to_temp(signal, temp) - 1.0) < 1e-9


def _bucket_outer_bounds(signal: dict) -> tuple[float, float]:
    low = signal.get("temp_low")
    high = signal.get("temp_high")
    low_bound = float("-inf") if low == -999.0 or low is None else float(low)
    high_bound = float("inf") if high == 999.0 or high is None else float(high) - 1.0
    return low_bound, high_bound


def _same_side_margin_from_bucket(signal: dict, temp: float | None) -> float | None:
    if temp is None:
        return None
    low_bound, high_bound = _bucket_outer_bounds(signal)
    temp = float(temp)
    if temp < low_bound:
        return low_bound - temp
    if temp > high_bound:
        return temp - high_bound
    return None


def _kalshi_signal_passes_forecast_gate(signal: dict, strategy_params: dict, market_center: float | None) -> bool:
    context = signal.get("forecast_context") or {}
    side = signal.get("side")
    hourly = context.get("nws_hourly_max_temp")
    daily = context.get("nws_temp")
    anchor = context.get("forecast_anchor_temp")
    midpoint = _signal_midpoint(signal)

    if market_center is not None and anchor is not None:
        divergence_cap = strategy_params.get(
            "kalshi_skip_market_divergence_f",
            KALSHI_SKIP_MARKET_DIVERGENCE_F,
        )
        if abs(float(market_center) - float(anchor)) > divergence_cap:
            return False

    if side == "BUY":
        if not _bucket_contains_temp(signal, hourly):
            return False
        if not (_bucket_contains_temp(signal, daily) or _bucket_adjacent_to_temp(signal, daily)):
            return False
        if midpoint is None or anchor is None:
            return False
        center_tolerance = strategy_params.get(
            "kalshi_bucket_center_tolerance_f",
            KALSHI_BUCKET_CENTER_TOLERANCE_F,
        )
        near_bucket_boundary = False
        low = signal.get("temp_low")
        high = signal.get("temp_high")
        if low not in (None, -999.0) and abs(float(anchor) - float(low)) <= KALSHI_ADJACENT_SPLIT_BOUNDARY_DISTANCE_F:
            near_bucket_boundary = True
        upper_boundary = None if high in (None, 999.0) else float(high)
        if upper_boundary is not None and abs(float(anchor) - upper_boundary) <= KALSHI_ADJACENT_SPLIT_BOUNDARY_DISTANCE_F:
            near_bucket_boundary = True
        if abs(float(anchor) - midpoint) > center_tolerance and not near_bucket_boundary:
            return False
        return True

    if side == "SELL":
        margin_required = strategy_params.get(
            "kalshi_sell_outside_bucket_margin_f",
            KALSHI_SELL_OUTSIDE_BUCKET_MARGIN_F,
        )
        hourly_margin = _same_side_margin_from_bucket(signal, hourly)
        daily_margin = _same_side_margin_from_bucket(signal, daily)
        if hourly_margin is None or daily_margin is None:
            return False
        if hourly_margin < margin_required or daily_margin < margin_required:
            return False
        return True

    return False


def _kalshi_signal_valid_spill(signal: dict, primary: dict, anchor_temp: float | None) -> bool:
    if signal.get("side") != primary.get("side"):
        return False
    if _bucket_boundary(primary, signal) is None:
        return False
    if primary.get("side") != "BUY":
        return False

    context = signal.get("forecast_context") or {}
    hourly = context.get("nws_hourly_max_temp")
    daily = context.get("nws_temp")
    if not (
        _bucket_contains_temp(signal, daily)
        or _bucket_adjacent_to_temp(signal, daily)
        or _bucket_contains_temp(signal, hourly)
        or _bucket_adjacent_to_temp(signal, hourly)
    ):
        return False
    if anchor_temp is None:
        return True
    return _bucket_distance_to_temp(signal, anchor_temp) <= 1.0


def _is_same_day_shadow_buy_package(selected_signals: list[dict]) -> bool:
    if not selected_signals:
        return False
    context = selected_signals[0].get("forecast_context") or {}
    return bool(context.get("shadow_only")) and selected_signals[0].get("side") == "BUY"


def _is_same_day_shadow(signal: dict) -> bool:
    context = signal.get("forecast_context") or {}
    return bool(context.get("shadow_only"))


def _is_same_day_intraday(signal: dict) -> bool:
    context = signal.get("forecast_context") or {}
    return bool(context.get("shadow_only")) or bool(context.get("same_day_live"))


def _strategy_is_same_day_sell_ladder() -> bool:
    return "same_day_sell_ladder" in WEATHER_STRATEGY_VERSION


def _select_same_day_sell_ladder(
    candidate_signals: list[dict],
    strategy_params: dict,
    anchor_temp: float | None,
    market_center: float | None,
) -> list[dict]:
    sell_candidates = [
        signal for signal in candidate_signals
        if signal.get("side") == "SELL"
        and _kalshi_signal_passes_forecast_gate(signal, strategy_params, market_center)
    ]
    if not sell_candidates:
        return []

    max_buckets = max(
        1,
        int(
            strategy_params.get(
                "kalshi_local_ladder_max_buckets",
                KALSHI_LOCAL_LADDER_MAX_BUCKETS,
            )
        ),
    )
    max_distance = float(
        strategy_params.get(
            "kalshi_local_ladder_max_distance_f",
            KALSHI_LOCAL_LADDER_MAX_DISTANCE_F,
        )
    )

    ranked = sorted(sell_candidates, key=lambda signal: _signal_sort_key(signal, anchor_temp))
    selected = []
    for signal in ranked:
        distance = _bucket_distance_to_temp(signal, anchor_temp)
        if distance > max_distance:
            continue
        updated = dict(signal)
        forecast_context = dict(updated.get("forecast_context") or {})
        forecast_context["event_selection"] = "same_day_sell_ladder"
        forecast_context["event_role"] = f"ladder_{len(selected) + 1}"
        forecast_context["event_bucket_distance_f"] = round(distance, 2)
        updated["forecast_context"] = forecast_context
        selected.append(updated)
        if len(selected) >= max_buckets:
            break

    return selected


def _select_kalshi_event_signals(candidate_signals: list[dict], strategy_params: dict) -> list[dict]:
    """Build a hedged event thesis: primary bucket plus optional spill bucket."""
    if not candidate_signals:
        return []

    anchor_temp = _forecast_anchor_temp(candidate_signals)
    market_center = None
    for signal in candidate_signals:
        market_center = signal.get("venue_implied_high")
        if market_center is not None:
            break

    if _strategy_is_same_day_sell_ladder() and any(_is_same_day_intraday(signal) for signal in candidate_signals):
        return _select_same_day_sell_ladder(candidate_signals, strategy_params, anchor_temp, market_center)

    primary_candidates = [
        signal for signal in candidate_signals
        if _kalshi_signal_passes_forecast_gate(signal, strategy_params, market_center)
    ]
    if not primary_candidates:
        return []

    split_threshold = strategy_params.get(
        "kalshi_adjacent_split_min_combined_prob",
        KALSHI_ADJACENT_SPLIT_MIN_COMBINED_PROB,
    )
    second_bucket_threshold = strategy_params.get(
        "kalshi_adjacent_split_min_second_bucket_prob",
        KALSHI_ADJACENT_SPLIT_MIN_SECOND_BUCKET_PROB,
    )
    boundary_distance_threshold = strategy_params.get(
        "kalshi_adjacent_split_boundary_distance_f",
        KALSHI_ADJACENT_SPLIT_BOUNDARY_DISTANCE_F,
    )
    primary = min(primary_candidates, key=lambda signal: _signal_sort_key(signal, anchor_temp))
    primary_context = primary.setdefault("forecast_context", {})
    primary_context["event_selection"] = "hedged"
    primary_context["event_role"] = "primary"

    if anchor_temp is None:
        return [primary]

    primary_prob = _signal_model_prob(primary)
    secondary_options = []
    for signal in candidate_signals:
        if signal.get("bucket_question") == primary.get("bucket_question"):
            continue
        boundary = _bucket_boundary(primary, signal)
        if boundary is None:
            continue
        if not _kalshi_signal_valid_spill(signal, primary, anchor_temp):
            continue
        if abs(float(anchor_temp) - boundary) > boundary_distance_threshold:
            continue
        second_prob = _signal_model_prob(signal)
        if second_prob < second_bucket_threshold:
            continue
        if primary_prob + second_prob < split_threshold:
            continue

        primary_mid = _signal_midpoint(primary)
        signal_mid = _signal_midpoint(signal)
        if primary_mid is None or signal_mid is None:
            continue

        if abs(signal_mid - float(anchor_temp)) > abs(primary_mid - float(anchor_temp)) + 2.0:
            continue

        secondary_options.append((_signal_sort_key(signal, anchor_temp), signal, boundary))

    if not secondary_options:
        if _is_same_day_shadow_buy_package([primary]):
            return []
        return [primary]

    _, secondary, boundary = min(secondary_options, key=lambda item: item[0])
    secondary_context = secondary.setdefault("forecast_context", {})
    secondary_context["event_selection"] = "hedged"
    secondary_context["event_role"] = "spill"
    secondary_context["event_split_boundary_f"] = boundary
    secondary_context["event_split_partner"] = primary.get("bucket_question", "")
    primary_context["event_split_boundary_f"] = boundary
    primary_context["event_split_partner"] = secondary.get("bucket_question", "")

    return sorted(
        [primary, secondary],
        key=lambda signal: (
            0 if signal.get("forecast_context", {}).get("event_role") == "primary" else 1,
            signal.get("temp_low") if signal.get("temp_low") is not None else float("-inf"),
        ),
    )


def _rebalance_selected_event_signals(selected_signals: list[dict]) -> list[dict]:
    """Cap a multi-bucket event to one budget with convex weighting."""
    if len(selected_signals) <= 1:
        return selected_signals

    original_sizes = [float(signal.get("trade_size", 0.0) or 0.0) for signal in selected_signals]
    event_budget = max(original_sizes)
    if event_budget <= 0:
        return selected_signals

    if all(
        signal.get("side") == "SELL"
        and signal.get("forecast_context", {}).get("event_selection") == "same_day_sell_ladder"
        for signal in selected_signals
    ):
        total_original = sum(original_sizes)
        if total_original <= 0:
            return []

        rebalanced = []
        for signal, original_size in zip(selected_signals, original_sizes):
            allocated_size = round(event_budget * (original_size / total_original), 2)
            if allocated_size < 1.0:
                continue
            updated = dict(signal)
            updated["trade_size"] = allocated_size
            updated["capped_by"] = "event_sell_ladder"
            forecast_context = dict(updated.get("forecast_context") or {})
            forecast_context["event_bucket_hedge"] = True
            forecast_context["event_bucket_split_budget"] = round(event_budget, 2)
            forecast_context["event_bucket_split_original_size"] = original_size
            forecast_context["event_bucket_split_style"] = "proportional_sell_ladder"
            updated["forecast_context"] = forecast_context
            rebalanced.append(updated)

        if not rebalanced:
            return []

        delta = round(event_budget - sum(signal["trade_size"] for signal in rebalanced), 2)
        if abs(delta) >= 0.01:
            rebalanced[0]["trade_size"] = round(rebalanced[0]["trade_size"] + delta, 2)

        logger.info(
            "Kalshi same-day sell ladder split: %s",
            ", ".join(
                f"{signal.get('bucket_question', '')} -> ${signal.get('trade_size', 0.0):.2f}"
                for signal in rebalanced
            ),
        )
        return rebalanced

    primary_weight = max(0.5, min(0.95, KALSHI_HEDGED_PRIMARY_WEIGHT))
    primary_index = 0
    for idx, signal in enumerate(selected_signals):
        if signal.get("forecast_context", {}).get("event_role") == "primary":
            primary_index = idx
            break

    spill_count = max(len(selected_signals) - 1, 1)
    spill_budget = event_budget * (1 - primary_weight)
    rebalanced = []
    for signal, original_size in zip(selected_signals, original_sizes):
        if signal is selected_signals[primary_index]:
            allocated_size = round(event_budget * primary_weight, 2)
        else:
            allocated_size = round(spill_budget / spill_count, 2)
        if allocated_size < 1.0:
            if _is_same_day_shadow_buy_package(selected_signals):
                logger.info(
                    "Kalshi same-day hedged BUY collapsed below minimum trade size; dropping package entirely"
                )
                return []
            logger.info("Kalshi hedged split collapsed below minimum trade size; keeping only the primary bucket")
            primary_only = dict(selected_signals[primary_index])
            forecast_context = dict(primary_only.get("forecast_context") or {})
            forecast_context["event_role"] = "primary"
            forecast_context.pop("event_split_partner", None)
            forecast_context.pop("event_split_boundary_f", None)
            primary_only["forecast_context"] = forecast_context
            return [primary_only]

        updated = dict(signal)
        updated["trade_size"] = allocated_size
        updated["capped_by"] = "event_bucket_hedge"
        forecast_context = dict(updated.get("forecast_context") or {})
        forecast_context["event_bucket_hedge"] = True
        forecast_context["event_bucket_split_budget"] = round(event_budget, 2)
        forecast_context["event_bucket_split_original_size"] = original_size
        forecast_context["event_bucket_primary_weight"] = round(primary_weight, 2)
        updated["forecast_context"] = forecast_context
        rebalanced.append(updated)

    logger.info(
        "Kalshi hedged event split: %s",
        ", ".join(
            f"{signal.get('bucket_question', '')} -> ${signal.get('trade_size', 0.0):.2f}"
            for signal in rebalanced
        ),
    )
    return rebalanced


def _scan_event(
    event: dict,
    bankrolls,
    daily_pnls,
    *,
    allow_same_day_live: bool = False,
) -> list[dict]:
    title = event.get("event_title", "")
    venue = event.get("venue", "polymarket")
    city_key = event.get("city")
    target_date = event.get("target_date")

    if not city_key or not target_date:
        logger.debug(f"Skipping event (no city/date): {title}")
        summary["skip_reasons"].append("Missing city or target date on normalized event")
        return {"signals": [], "event_summary": _mark_summary(summary, "invalid_event", "missing_city_or_date", title)}

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
    same_day_shadow = False
    same_day_live = False

    try:
        event_date = datetime.strptime(target_date, "%Y-%m-%d").date()
        now_local = datetime.now(MARKET_TIMEZONE)
        today = now_local.date()
        days_ahead = (event_date - today).days
        trade_window_hours = _trade_window_lead_hours(target_date, now_local)
        lead_hours = _lead_time_hours(target_date, now_local)
        lead_bucket = _lead_time_bucket(lead_hours)
        summary["model_summary"]["forecast_horizon_days"] = days_ahead
        summary["model_summary"]["forecast_lead_hours"] = lead_hours
        summary["model_summary"]["forecast_lead_bucket"] = lead_bucket
        summary["model_summary"]["trade_window_lead_hours"] = trade_window_hours
        if venue == "kalshi" and days_ahead == 0 and (KALSHI_ALLOW_SAME_DAY_TRADING or allow_same_day_live):
            same_day_live = True
            summary["model_summary"]["same_day_live"] = True
            summary["model_summary"]["same_day_live_reason"] = (
                "kalshi_same_day_live_override"
                if KALSHI_ALLOW_SAME_DAY_TRADING
                else "kalshi_same_day_target_top_up"
            )
        elif venue == "kalshi" and days_ahead == 0 and KALSHI_SHADOW_INCLUDE_SAME_DAY:
            same_day_shadow = True
            summary["model_summary"]["shadow_only"] = True
            summary["model_summary"]["shadow_reason"] = "kalshi_same_day_shadow"
            summary["skip_reasons"].append("Same-day Kalshi event is shadow-analyzed only; no trades will be placed")
        elif days_ahead <= 0:
            logger.debug(f"Skipping past/today event: {title} (date={target_date})")
            summary["skip_reasons"].append("Skipped by forecast horizon: same-day or past event")
            return {
                "signals": [],
                "event_summary": _mark_summary(
                    summary,
                    "skipped_pre_analysis",
                    "horizon_same_or_past",
                    f"{title} ({target_date})",
                ),
            }
        if venue == "kalshi" and KALSHI_TRADE_NEXT_DAY_ONLY and not same_day_shadow and not same_day_live and days_ahead != 1:
            logger.debug(
                "Skipping Kalshi event outside tomorrow-only trading window: %s (%sd ahead)",
                title,
                days_ahead,
            )
            summary["skip_reasons"].append(
                f"Skipped by Kalshi schedule: only next-day events are tradeable (got {days_ahead}d ahead)"
            )
            return {
                "signals": [],
                "event_summary": _mark_summary(
                    summary,
                    "skipped_pre_analysis",
                    "kalshi_not_next_day",
                    f"{title} ({days_ahead}d ahead)",
                ),
            }
        if venue == "kalshi" and not same_day_shadow and trade_window_hours > KALSHI_MAX_FORECAST_LEAD_HOURS:
            logger.debug(
                "Skipping distant Kalshi event outside trade window: %s (%0.1fh ahead to event start, max=%0.1fh)",
                title,
                trade_window_hours,
                KALSHI_MAX_FORECAST_LEAD_HOURS,
            )
            summary["skip_reasons"].append(
                f"Skipped by Kalshi lead window: {trade_window_hours:.1f}h until event start exceeds max "
                f"{KALSHI_MAX_FORECAST_LEAD_HOURS:.1f}h"
            )
            return {
                "signals": [],
                "event_summary": _mark_summary(
                    summary,
                    "skipped_pre_analysis",
                    "horizon_outside_kalshi_lead_window",
                    f"{title} ({trade_window_hours:.1f}h until event start > max {KALSHI_MAX_FORECAST_LEAD_HOURS:.1f}h)",
                ),
            }
        if days_ahead > MAX_FORECAST_DAYS:
            logger.debug(f"Skipping distant event: {title} ({days_ahead} days ahead, max={MAX_FORECAST_DAYS})")
            summary["skip_reasons"].append(
                f"Skipped by forecast horizon: {days_ahead} days ahead exceeds max {MAX_FORECAST_DAYS}"
            )
            return {
                "signals": [],
                "event_summary": _mark_summary(
                    summary,
                    "skipped_pre_analysis",
                    "horizon_too_far",
                    f"{title} ({days_ahead}d ahead > max {MAX_FORECAST_DAYS})",
                ),
            }
        logger.info(
            "  Forecast horizon: %s day(s) ahead | lead=%0.1fh (%s)",
            days_ahead,
            lead_hours,
            lead_bucket,
        )
    except ValueError:
        pass

    volume = event.get("volume", 0) or 0
    if volume < MIN_VOLUME:
        logger.debug(f"Skipping low-volume event: {title} (${volume:,.0f})")
        summary["skip_reasons"].append(f"Skipped for low volume (${volume:,.0f} < ${MIN_VOLUME:,.0f})")
        return {
            "signals": [],
            "event_summary": _mark_summary(
                summary,
                "skipped_pre_analysis",
                "low_volume",
                f"{title} (${volume:,.0f})",
            ),
        }

    logger.info(f"\nAnalyzing [{venue}]: {title}")
    logger.info(f"  City: {city_key} | Date: {target_date} | Volume: ${volume:,.0f}")

    buckets = event.get("buckets", [])
    if not buckets:
        logger.warning(f"  No buckets parsed for {title}")
        summary["skip_reasons"].append("No normalized buckets parsed for this venue event")
        return {
            "signals": [],
            "event_summary": _mark_summary(
                summary,
                "skipped_pre_analysis",
                "no_buckets",
                title,
            ),
        }

    station_meta = None
    forecast_lat = None
    forecast_lon = None
    if venue == "kalshi":
        station_meta = get_climate_station_metadata(city_key)
        if station_meta:
            forecast_lat = station_meta.get("lat")
            forecast_lon = station_meta.get("lon")
            summary["model_summary"]["settlement_station"] = station_meta.get("station_name")
            summary["model_summary"]["settlement_station_source"] = station_meta.get("source")

    nws_forecast = None
    nws_hourly_high = None
    city_cfg = CITIES.get(city_key, {})
    if city_cfg.get("nws_available"):
        nws_forecast = get_forecast_high(city_key, target_date, lat=forecast_lat, lon=forecast_lon)
        nws_hourly_high = get_hourly_forecast_high(city_key, target_date, lat=forecast_lat, lon=forecast_lon)
        if nws_hourly_high:
            summary["model_summary"]["nws_hourly_high_temp"] = nws_hourly_high.get("temp")
            summary["model_summary"]["nws_hourly_high_hour"] = nws_hourly_high.get("hour")
        if nws_forecast:
            logger.info(
                f"  NWS Forecast: {nws_forecast['temp']}°{nws_forecast['unit']} "
                f"({nws_forecast.get('short_forecast', '')})"
            )
        if nws_hourly_high:
            logger.info(
                f"  NWS Hourly Max: {nws_hourly_high['temp']}°{nws_hourly_high['unit']} "
                f"at hour {nws_hourly_high['hour']:02d}"
            )

    if nws_forecast or nws_hourly_high:
        nws_forecast = {
            **(nws_forecast or {}),
            "hourly_max_temp": (nws_hourly_high or {}).get("temp"),
            "hourly_max_hour": (nws_hourly_high or {}).get("hour"),
            "hourly_source": (nws_hourly_high or {}).get("source"),
            "forecast_lat": forecast_lat,
            "forecast_lon": forecast_lon,
            "settlement_station": (station_meta or {}).get("station_name"),
            "settlement_station_code": (station_meta or {}).get("issuedby"),
        }

    kalshi_nws_anchor = _kalshi_nws_anchor_temp(nws_forecast) if venue == "kalshi" else None
    kalshi_blend_weight = strategy_params.get("kalshi_nws_blend_weight", 0.0) if venue == "kalshi" else 0.0

    enriched = get_full_distribution(
        city_key,
        target_date,
        buckets,
        lat=forecast_lat,
        lon=forecast_lon,
        anchor_temp=kalshi_nws_anchor,
        blend_weight=kalshi_blend_weight,
        blend_source="nws_station_forecast" if kalshi_nws_anchor is not None and kalshi_blend_weight > 0 else None,
    )
    if not enriched:
        logger.warning(f"  Ensemble data unavailable for {city_key} on {target_date}")
        summary["skip_reasons"].append("Ensemble distribution unavailable")
        return {
            "signals": [],
            "event_summary": _mark_summary(
                summary,
                "skipped_post_analysis",
                "ensemble_unavailable",
                f"{city_key} {target_date}",
            ),
        }

    analyzed = analyze_event_buckets(
        enriched,
        venue=venue,
        threshold=strategy_params.get("edge_threshold"),
        strategy_params=strategy_params,
    )
    ensemble_meta = enriched[0].get("ensemble_meta", {}) if enriched else {}
    raw_mean = ensemble_meta.get("raw_mean", ensemble_meta.get("mean"))
    blended_mean = ensemble_meta.get("mean")
    forecast_disagreement_f = None
    if kalshi_nws_anchor is not None and raw_mean is not None:
        forecast_disagreement_f = abs(float(kalshi_nws_anchor) - float(raw_mean))
    summary["model_expected_high"] = ensemble_meta.get("mean")
    summary["model_spread"] = ensemble_meta.get("spread")
    summary["venue_implied_high"] = implied_event_temperature(analyzed)
    summary["model_summary"] = {
        **summary["model_summary"],
        "ensemble_members": ensemble_meta.get("member_count"),
        "nws_temp": (nws_forecast or {}).get("temp"),
        "nws_hourly_max_temp": (nws_forecast or {}).get("hourly_max_temp"),
        "raw_ensemble_mean": raw_mean,
        "raw_ensemble_spread": ensemble_meta.get("raw_spread", ensemble_meta.get("spread")),
        "forecast_anchor_temp": kalshi_nws_anchor,
        "forecast_blend_weight": ensemble_meta.get("blend_weight"),
            "forecast_disagreement_f": forecast_disagreement_f,
            "forecast_lead_hours": summary["model_summary"].get("forecast_lead_hours"),
            "forecast_lead_bucket": summary["model_summary"].get("forecast_lead_bucket"),
        }

    opportunities = rank_opportunities(analyzed)
    if not opportunities:
        logger.info("  No tradeable edges found")
        summary["skip_reasons"].append("No tradeable edges found after fees")
        return {
            "signals": [],
            "event_summary": _mark_summary(
                summary,
                "analyzed",
                "no_tradeable_edges",
                f"{city_key} {target_date}",
            ),
        }

    logger.info(f"  Found {len(opportunities)} tradeable buckets")

    venue_bankroll = _coerce_venue_value(bankrolls, venue, DEFAULT_VENUE_BANKROLLS.get(venue, 1000.0))
    venue_daily_pnl = _coerce_venue_value(daily_pnls, venue, 0.0)

    candidate_signals = []
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

        signal = make_signal(bucket, event, nws_forecast, trade_size, ensemble_meta, station_meta=station_meta)
        signal["venue_implied_high"] = summary["venue_implied_high"]
        signal["is_contrarian"] = is_contrarian
        signal["forecast_context"] = {
            "selected_prob": bucket.get("selected_prob"),
            "market_prob": bucket.get("market_prob"),
            "entry_price": bucket.get("selected_price"),
            "yes_price": bucket.get("yes_price"),
            "no_price": bucket.get("no_price"),
            "yes_edge": bucket.get("yes_edge"),
            "no_edge": bucket.get("no_edge"),
            "ensemble_mean": blended_mean,
            "ensemble_spread": ensemble_meta.get("spread"),
            "ensemble_min": ensemble_meta.get("min"),
            "ensemble_max": ensemble_meta.get("max"),
            "ensemble_members": ensemble_meta.get("member_count"),
            "raw_ensemble_mean": raw_mean,
            "raw_ensemble_spread": ensemble_meta.get("raw_spread", ensemble_meta.get("spread")),
            "raw_ensemble_min": ensemble_meta.get("raw_min", ensemble_meta.get("min")),
            "raw_ensemble_max": ensemble_meta.get("raw_max", ensemble_meta.get("max")),
            "nws_temp": (nws_forecast or {}).get("temp"),
            "nws_unit": (nws_forecast or {}).get("unit"),
            "nws_short_forecast": (nws_forecast or {}).get("short_forecast"),
            "nws_hourly_max_temp": (nws_forecast or {}).get("hourly_max_temp"),
            "nws_hourly_max_hour": (nws_forecast or {}).get("hourly_max_hour"),
            "nws_hourly_source": (nws_forecast or {}).get("hourly_source"),
            "forecast_anchor_temp": kalshi_nws_anchor,
            "forecast_blend_weight": ensemble_meta.get("blend_weight"),
            "forecast_blend_source": ensemble_meta.get("blend_source"),
            "forecast_disagreement_f": forecast_disagreement_f,
            "forecast_lead_hours": summary["model_summary"].get("forecast_lead_hours"),
            "forecast_lead_bucket": summary["model_summary"].get("forecast_lead_bucket"),
            "trade_window_lead_hours": summary["model_summary"].get("trade_window_lead_hours"),
            "shadow_only": summary["model_summary"].get("shadow_only", False),
            "shadow_reason": summary["model_summary"].get("shadow_reason"),
            "same_day_live": summary["model_summary"].get("same_day_live", False),
            "same_day_live_reason": summary["model_summary"].get("same_day_live_reason"),
            "settlement_station": signal.get("settlement_station"),
            "settlement_station_code": signal.get("settlement_station_code"),
            "settlement_station_source": signal.get("settlement_station_source"),
            "forecast_lat": signal.get("forecast_lat"),
            "forecast_lon": signal.get("forecast_lon"),
        }

        if venue == "kalshi" and forecast_disagreement_f is not None:
            threshold = strategy_params.get("kalshi_nws_disagreement_threshold_f", 3.0)
            if forecast_disagreement_f >= threshold and trade_size.get("size", 0) > 0:
                original_size = trade_size["size"]
                trade_size["size"] = round(original_size * 0.5, 2)
                trade_size["capped_by"] = (
                    f"nws_disagreement ({forecast_disagreement_f:.1f}F >= {threshold:.1f}F)"
                )
                signal["trade_size"] = trade_size["size"]
                signal["capped_by"] = trade_size["capped_by"]
                signal["forecast_context"]["nws_disagreement_size_cut"] = True
                logger.info(
                    f"  NWS disagreement cut: ${original_size:.2f} -> ${trade_size['size']:.2f} "
                    f"(raw ensemble {raw_mean:.1f}F vs NWS anchor {kalshi_nws_anchor:.1f}F)"
                )
        candidate_signals.append(signal)
        summary["candidate_bets"].append(_candidate_summary(signal))

    if venue == "kalshi":
        selected_signals = _select_kalshi_event_signals(candidate_signals, strategy_params)
        if not selected_signals:
            if summary["model_summary"].get("shadow_only"):
                return {
                    "signals": [],
                    "event_summary": _mark_summary(
                        summary,
                        "shadow_only",
                        "kalshi_same_day_shadow",
                        f"{title} (same-day shadow only)",
                    ),
                }
            summary["skip_reasons"].append("All Kalshi candidate buckets failed the station-aware forecast gate")
            return {
                "signals": [],
                "event_summary": _mark_summary(
                    summary,
                    "skipped_post_analysis",
                    "kalshi_station_gate",
                    f"{city_key} {target_date}",
                ),
            }
        signals = _rebalance_selected_event_signals(selected_signals)
    else:
        signals = candidate_signals

    summary["proposed_bets"] = [_selected_bet_summary(signal) for signal in signals]
    if venue == "kalshi":
        summary["model_summary"]["shadow_experiment_name"] = "same_day_veto_replace"
        summary["model_summary"]["shadow_experiment_tracking"] = True

    if signals and all(s.get("trade_size", 0) <= 0 for s in signals):
        sizing_reasons = [
            s.get("skip_reason") for s in signals if s.get("skip_reason")
        ]
        summary["skip_reasons"].extend(sizing_reasons or ["Risk sizing reduced all candidate bets to $0"])
        return {
            "signals": signals,
            "event_summary": _mark_summary(
                summary,
                "sized_to_zero",
                "sized_to_zero",
                sizing_reasons[0] if sizing_reasons else "Risk sizing reduced all candidate bets to $0",
            ),
        }

    if venue == "kalshi" and summary["model_summary"].get("shadow_only"):
        return {
            "signals": [],
            "event_summary": _mark_summary(
                summary,
                "shadow_only",
                "kalshi_same_day_shadow",
                f"{title} (same-day shadow only)",
            ),
        }

    return {"signals": signals, "event_summary": _mark_summary(summary, "actionable", None, None)}


def scan_all_markets(bankroll=None,
                     daily_pnl=0.0,
                     return_context: bool = False,
                     allow_same_day_live: bool = False):
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
        result = _scan_event(
            event,
            bankroll,
            daily_pnl,
            allow_same_day_live=allow_same_day_live,
        )
        all_signals.extend(result["signals"])
        if result["event_summary"]:
            event_summaries.append(result["event_summary"])

    all_signals.sort(key=lambda s: abs(s.get("edge", 0)), reverse=True)
    _log_venue_diagnostics(event_summaries, all_signals)
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
