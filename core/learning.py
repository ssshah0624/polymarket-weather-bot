"""
Learning Digest — Weekly Pattern Analysis.

Analyzes resolved trades to find patterns in wins vs losses,
identifies which cities/edges/sides perform best, and sends
a Slack digest with actionable insights.

Designed to inform gradual strategy tuning — not hair-trigger changes.
"""

import logging
import json
from datetime import date, timedelta
from collections import defaultdict

from config.settings import PRIMARY_VISIBLE_VENUE
from core.database import session_scope, Trade, WeatherComparisonSnapshot
from core.alerts import send_slack_blocks, send_slack_message
from core.resolution import check_bucket_hit
from core.tuning import (
    apply_kalshi_tuning,
    evaluate_kalshi_tuning,
    format_param_value,
    get_base_strategy_params,
    get_effective_strategy_params,
    load_kalshi_tuning_state,
)

logger = logging.getLogger(__name__)


def _get_resolved_trades(days: int = 7, mode: str = "paper", venue: str | None = None) -> list[dict]:
    """Fetch resolved trades from the past N days."""
    cutoff = (date.today() - timedelta(days=days)).strftime("%Y-%m-%d")
    with session_scope() as session:
        query = (
            session.query(Trade)
            .filter(Trade.mode == mode, Trade.resolved == True,
                    Trade.target_date >= cutoff)
        )
        if venue:
            query = query.filter(Trade.venue == venue)
        trades = query.all()
        return [
            {
                "id": t.id,
                "venue": t.venue or "polymarket",
                "city": t.city,
                "target_date": t.target_date,
                "bucket_question": t.bucket_question,
                "side": t.side,
                "size_usd": t.size_usd,
                "price": t.price,
                "ensemble_prob": t.ensemble_prob,
                "edge": t.edge,
                "outcome": t.outcome,
                "pnl": t.pnl,
                "actual_temp": t.actual_temp,
                "settlement_station": t.settlement_station,
                "resolution_source": t.resolution_source,
                "model_expected_high": t.model_expected_high,
                "forecast_context": json.loads(t.forecast_context_json) if t.forecast_context_json else {},
                "is_contrarian": bool(t.is_contrarian),
                "strategy_version": t.strategy_version,
            }
            for t in trades
        ]


def _get_weather_comparison_snapshots(days: int = 7, mode: str = "live") -> list[dict]:
    """Fetch comparison snapshots from the past N target dates."""
    cutoff = (date.today() - timedelta(days=days)).strftime("%Y-%m-%d")
    with session_scope() as session:
        rows = (
            session.query(WeatherComparisonSnapshot)
            .filter(
                WeatherComparisonSnapshot.mode == mode,
                WeatherComparisonSnapshot.target_date >= cutoff,
            )
            .order_by(
                WeatherComparisonSnapshot.target_date.asc(),
                WeatherComparisonSnapshot.timestamp.asc(),
            )
            .all()
        )
        return [
            {
                "timestamp": row.timestamp,
                "mode": row.mode,
                "strategy_version": row.strategy_version,
                "city": row.city,
                "target_date": row.target_date,
                "model_expected_high": row.model_expected_high,
                "model_spread": row.model_spread,
                "model_summary": json.loads(row.model_summary_json or "{}"),
                "candidate_bets": json.loads(row.candidate_bets_json or "[]"),
                "selected_bets": json.loads(row.selected_bets_json or "[]"),
                "skip_reasons": json.loads(row.skip_reasons_json or "[]"),
            }
            for row in rows
        ]


def _bucket_signature(bet: dict) -> tuple[str, str]:
    return (bet.get("bucket_question", ""), bet.get("side", "BUY"))


def _estimate_selected_bets_pnl(selected_bets: list[dict], actual_temp: float | None, city: str) -> dict:
    """Estimate realized gross P&L for a hypothetical selected package."""
    if actual_temp is None or not selected_bets:
        return {"pnl": 0.0, "wins": 0, "losses": 0, "trades": 0}

    total_pnl = 0.0
    wins = 0
    losses = 0
    for bet in selected_bets:
        in_bucket = check_bucket_hit(actual_temp, bet.get("bucket_question", ""), city)
        if in_bucket is None:
            continue
        side = bet.get("side", "BUY")
        size = float(bet.get("trade_size", 0.0) or 0.0)
        entry_price = float(bet.get("entry_price", 0.0) or 0.0)
        if size <= 0 or entry_price <= 0:
            continue

        won = in_bucket if side == "BUY" else not in_bucket
        if won:
            shares = size / max(entry_price, 0.03)
            pnl = min(shares - size, size * 19)
            wins += 1
        else:
            pnl = -size
            losses += 1
        total_pnl += pnl

    return {
        "pnl": total_pnl,
        "wins": wins,
        "losses": losses,
        "trades": wins + losses,
    }


def analyze_shadow_layer_experiment(days: int = 5, mode: str = "live", venue: str = "kalshi") -> dict:
    """
    Compare held next-day live trades against the latest same-day shadow package.

    This evaluates whether a same-day veto or replace layer would have improved
    outcomes on settled event-days.
    """
    resolved = _get_resolved_trades(days=days, mode=mode, venue=venue)
    snapshots = _get_weather_comparison_snapshots(days=days + 2, mode=mode)
    if not resolved:
        return {"cases": [], "summary": {"sample_size": 0}}

    trades_by_key: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for trade in resolved:
        if trade.get("venue") != venue:
            continue
        if trade.get("resolution_source") == "manual_exit":
            continue
        trades_by_key[(trade.get("city", ""), trade.get("target_date", ""))].append(trade)

    snapshots_by_key: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for row in snapshots:
        key = (row.get("city", ""), row.get("target_date", ""))
        snapshots_by_key[key].append(row)

    cases = []
    for key, held_trades in sorted(trades_by_key.items(), key=lambda item: item[0]):
        actual_temp = next((t.get("actual_temp") for t in held_trades if t.get("actual_temp") is not None), None)
        if actual_temp is None:
            continue

        rows = snapshots_by_key.get(key, [])
        shadow_rows = [row for row in rows if (row.get("model_summary") or {}).get("shadow_only")]
        if not shadow_rows:
            continue

        latest_shadow = shadow_rows[-1]
        shadow_selected = list(latest_shadow.get("selected_bets") or [])
        if not shadow_selected:
            proposed = (latest_shadow.get("model_summary") or {}).get("proposed_selected_bets") or []
            shadow_selected = list(proposed)

        held_pnl = sum(float(t.get("pnl", 0.0) or 0.0) for t in held_trades)
        held_signatures = {_bucket_signature(t) for t in held_trades}
        shadow_signatures = {_bucket_signature(bet) for bet in shadow_selected}
        same_day_eval = _estimate_selected_bets_pnl(shadow_selected, actual_temp, key[0])
        no_shadow_package = (latest_shadow.get("model_summary") or {}).get("selected_bets_source") == "no_shadow_package"
        changed = no_shadow_package or shadow_signatures != held_signatures
        overlap = bool(held_signatures & shadow_signatures)
        veto_benefit = -held_pnl if changed else 0.0
        replace_benefit = same_day_eval["pnl"] - held_pnl if shadow_signatures else 0.0

        cases.append(
            {
                "city": key[0],
                "target_date": key[1],
                "actual_temp": actual_temp,
                "held_trades": held_trades,
                "held_pnl": held_pnl,
                "held_signatures": sorted(held_signatures),
                "shadow_selected": shadow_selected,
                "shadow_signatures": sorted(shadow_signatures),
                "shadow_hold_overlap": overlap,
                "shadow_changed": changed,
                "shadow_selected_source": (latest_shadow.get("model_summary") or {}).get("selected_bets_source"),
                "shadow_forecast_lead_hours": (latest_shadow.get("model_summary") or {}).get("forecast_lead_hours"),
                "shadow_forecast_lead_bucket": (latest_shadow.get("model_summary") or {}).get("forecast_lead_bucket"),
                "shadow_no_package": no_shadow_package,
                "shadow_pnl_estimate": same_day_eval["pnl"],
                "shadow_wins": same_day_eval["wins"],
                "shadow_losses": same_day_eval["losses"],
                "veto_benefit": veto_benefit,
                "replace_benefit": replace_benefit,
            }
        )

    sample_size = len(cases)
    hold_pnl = sum(case["held_pnl"] for case in cases)
    shadow_replace_pnl = sum(case["shadow_pnl_estimate"] for case in cases)
    veto_pnl = sum(0.0 if case["shadow_changed"] else case["held_pnl"] for case in cases)
    changed_cases = [case for case in cases if case["shadow_changed"]]
    replace_wins = sum(1 for case in cases if case["replace_benefit"] > 0)
    veto_wins = sum(1 for case in changed_cases if case["held_pnl"] < 0)

    recommendation = {
        "layer": "collect_more_data",
        "reason": "Not enough settled shadow cases yet.",
    }
    if sample_size >= 4:
        if shadow_replace_pnl > hold_pnl and replace_wins >= max(3, sample_size // 2):
            recommendation = {
                "layer": "same_day_replace",
                "reason": (
                    f"Latest same-day shadow package outperformed hold on {replace_wins}/{sample_size} settled cases "
                    f"and improved total P&L by ${shadow_replace_pnl - hold_pnl:+.2f}."
                ),
            }
        elif changed_cases and veto_pnl > hold_pnl and veto_wins >= max(2, len(changed_cases) // 2):
            recommendation = {
                "layer": "same_day_veto",
                "reason": (
                    f"Zeroing materially changed positions would have improved total P&L by ${veto_pnl - hold_pnl:+.2f} "
                    f"and avoided losses on {veto_wins}/{len(changed_cases)} changed cases."
                ),
            }
        else:
            recommendation = {
                "layer": "no_same_day_layer",
                "reason": "Neither veto nor replace beat hold strongly enough on the settled sample.",
            }

    return {
        "cases": cases,
        "summary": {
            "sample_size": sample_size,
            "changed_cases": len(changed_cases),
            "hold_pnl": hold_pnl,
            "shadow_replace_pnl": shadow_replace_pnl,
            "veto_pnl": veto_pnl,
            "replace_wins": replace_wins,
            "veto_wins": veto_wins,
            "recommendation": recommendation,
        },
    }


def _bucket_edge(edge: float) -> str:
    """Categorize edge into buckets for analysis."""
    ae = abs(edge)
    if ae < 0.10:
        return "5-10%"
    elif ae < 0.20:
        return "10-20%"
    elif ae < 0.30:
        return "20-30%"
    else:
        return "30%+"


def _calibration_midpoint(label: str) -> float | None:
    try:
        low, high = label.rstrip("%").split("-")
        return (float(low) + float(high)) / 200.0
    except ValueError:
        return None


def analyze_patterns(trades: list[dict]) -> dict:
    """
    Analyze resolved trades for patterns.
    Returns a dict of insights.
    """
    if not trades:
        return {"total": 0}

    total = len(trades)
    wins = [t for t in trades if t["outcome"] == "win"]
    losses = [t for t in trades if t["outcome"] == "loss"]
    total_pnl = sum(t["pnl"] for t in trades)

    # By city
    city_stats = defaultdict(lambda: {
        "wins": 0, "losses": 0, "pnl": 0.0, "trades": 0,
        "bias_sum": 0.0, "bias_count": 0,
    })
    for t in trades:
        city = t["city"]
        city_stats[city]["trades"] += 1
        city_stats[city]["pnl"] += t["pnl"]
        if t.get("actual_temp") is not None and t.get("model_expected_high") is not None:
            city_stats[city]["bias_sum"] += t["actual_temp"] - t["model_expected_high"]
            city_stats[city]["bias_count"] += 1
        if t["outcome"] == "win":
            city_stats[city]["wins"] += 1
        else:
            city_stats[city]["losses"] += 1

    # By side (BUY vs SELL)
    side_stats = defaultdict(lambda: {"wins": 0, "losses": 0, "pnl": 0.0, "trades": 0})
    for t in trades:
        side = t["side"]
        side_stats[side]["trades"] += 1
        side_stats[side]["pnl"] += t["pnl"]
        if t["outcome"] == "win":
            side_stats[side]["wins"] += 1
        else:
            side_stats[side]["losses"] += 1

    # By edge bucket
    edge_stats = defaultdict(lambda: {"wins": 0, "losses": 0, "pnl": 0.0, "trades": 0})
    for t in trades:
        bucket = _bucket_edge(t["edge"])
        edge_stats[bucket]["trades"] += 1
        edge_stats[bucket]["pnl"] += t["pnl"]
        if t["outcome"] == "win":
            edge_stats[bucket]["wins"] += 1
        else:
            edge_stats[bucket]["losses"] += 1

    venue_stats = defaultdict(lambda: {"wins": 0, "losses": 0, "pnl": 0.0, "trades": 0})
    for t in trades:
        venue = t.get("venue", "polymarket")
        venue_stats[venue]["trades"] += 1
        venue_stats[venue]["pnl"] += t["pnl"]
        if t["outcome"] == "win":
            venue_stats[venue]["wins"] += 1
        else:
            venue_stats[venue]["losses"] += 1

    stance_stats = defaultdict(lambda: {"wins": 0, "losses": 0, "pnl": 0.0, "trades": 0})
    for t in trades:
        stance = "contrarian" if t.get("is_contrarian") else "consensus"
        stance_stats[stance]["trades"] += 1
        stance_stats[stance]["pnl"] += t["pnl"]
        if t["outcome"] == "win":
            stance_stats[stance]["wins"] += 1
        else:
            stance_stats[stance]["losses"] += 1

    calibration_stats = defaultdict(lambda: {"wins": 0, "trades": 0})
    for t in trades:
        selected_prob = t["ensemble_prob"] if t["side"] == "BUY" else 1 - t["ensemble_prob"]
        bucket_floor = int(selected_prob * 10) * 10
        bucket_floor = min(max(bucket_floor, 0), 90)
        label = f"{bucket_floor}-{bucket_floor + 10}%"
        calibration_stats[label]["trades"] += 1
        if t["outcome"] == "win":
            calibration_stats[label]["wins"] += 1

    calibration_error_numerator = 0.0
    calibration_error_denominator = 0
    for label, stats in calibration_stats.items():
        midpoint = _calibration_midpoint(label)
        if midpoint is None or stats["trades"] == 0:
            continue
        realized = stats["wins"] / stats["trades"]
        calibration_error_numerator += abs(realized - midpoint) * stats["trades"]
        calibration_error_denominator += stats["trades"]

    # Best and worst cities
    cities_sorted = sorted(city_stats.items(), key=lambda x: x[1]["pnl"], reverse=True)
    best_cities = cities_sorted[:3]
    worst_cities = cities_sorted[-3:] if len(cities_sorted) > 3 else []

    return {
        "total": total,
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": len(wins) / total if total > 0 else 0,
        "total_pnl": total_pnl,
        "avg_pnl_per_trade": total_pnl / total if total > 0 else 0,
        "city_stats": dict(city_stats),
        "side_stats": dict(side_stats),
        "edge_stats": dict(edge_stats),
        "venue_stats": dict(venue_stats),
        "stance_stats": dict(stance_stats),
        "calibration_stats": dict(calibration_stats),
        "calibration_mean_abs_error": (
            calibration_error_numerator / calibration_error_denominator
            if calibration_error_denominator else None
        ),
        "best_cities": best_cities,
        "worst_cities": worst_cities,
    }


def _confidence_label(sample_size: int) -> str:
    if sample_size >= 20:
        return "high"
    if sample_size >= 10:
        return "medium"
    return "low"


def _win_rate(stats: dict) -> float:
    trades = stats.get("trades", 0)
    return stats.get("wins", 0) / trades if trades else 0.0


def generate_insights(analysis: dict) -> list[str]:
    """Generate observational, not self-modifying, insights."""
    insights = []

    if analysis["total"] == 0:
        return ["Not enough data yet to generate insights. Keep collecting!"]

    # Overall performance
    wr = analysis["win_rate"]
    if wr >= 0.60:
        insights.append(
            f"Win rate is strong at {wr:.0%}. The ensemble edge is holding up well."
        )
    elif wr >= 0.50:
        insights.append(
            f"Win rate is {wr:.0%} — slightly above breakeven. "
            f"Monitor whether this improves as we collect more data."
        )
    else:
        insights.append(
            f"Win rate is {wr:.0%} — below 50%. This could be a small sample issue, "
            f"or the edge threshold may need to be raised."
        )

    # BUY vs SELL
    side_stats = analysis.get("side_stats", {})
    buy = side_stats.get("BUY", {})
    sell = side_stats.get("SELL", {})
    if buy.get("trades", 0) >= 3 and sell.get("trades", 0) >= 3:
        buy_wr = buy["wins"] / buy["trades"] if buy["trades"] > 0 else 0
        sell_wr = sell["wins"] / sell["trades"] if sell["trades"] > 0 else 0
        if abs(buy_wr - sell_wr) > 0.15:
            better = "BUY" if buy_wr > sell_wr else "SELL"
            insights.append(
                f"{better} trades are outperforming "
                f"(BUY: {buy_wr:.0%} win rate, SELL: {sell_wr:.0%}). "
                f"Worth watching but don't change strategy yet."
            )

    # Edge size correlation
    edge_stats = analysis.get("edge_stats", {})
    for bucket in ["30%+", "20-30%", "10-20%", "5-10%"]:
        es = edge_stats.get(bucket, {})
        if es.get("trades", 0) >= 3:
            wr = es["wins"] / es["trades"]
            insights.append(
                f"Edge {bucket}: {wr:.0%} win rate across {es['trades']} trades "
                f"(P&L: ${es['pnl']:+.2f})"
            )

    # Venue view
    venue_stats = analysis.get("venue_stats", {})
    for venue, stats in sorted(venue_stats.items()):
        if stats.get("trades", 0) >= 3:
            insights.append(
                f"{venue.title()}: {_win_rate(stats):.0%} win rate across {stats['trades']} trades "
                f"(P&L: ${stats['pnl']:+.2f})"
            )

    # Best/worst cities
    best = analysis.get("best_cities", [])
    if best and best[0][1]["trades"] >= 2:
        city, stats = best[0]
        insights.append(
            f"Best city: {city.replace('_',' ').title()} — "
            f"${stats['pnl']:+.2f} P&L across {stats['trades']} trades"
        )

    worst = analysis.get("worst_cities", [])
    if worst and worst[-1][1]["pnl"] < 0 and worst[-1][1]["trades"] >= 2:
        city, stats = worst[-1]
        insights.append(
            f"Worst city: {city.replace('_',' ').title()} — "
            f"${stats['pnl']:+.2f} P&L across {stats['trades']} trades. "
            f"Consider whether ensemble data is less reliable here."
        )

    return insights


def generate_recommendations(analysis: dict) -> list[dict]:
    """Rules-based recommendations only. No automatic strategy changes."""
    recommendations = []

    total = analysis.get("total", 0)
    if total < 20:
        return recommendations

    low_edge = analysis.get("edge_stats", {}).get("5-10%", {})
    if low_edge.get("trades", 0) >= 10 and _win_rate(low_edge) < 0.45:
        recommendations.append({
            "rule": "low_edge_underperformance",
            "segment": "edge bucket 5-10%",
            "sample_size": low_edge["trades"],
            "recommendation": "Raise the minimum tradable edge above the 5-10% bucket.",
            "confidence": _confidence_label(low_edge["trades"]),
        })

    venue_stats = analysis.get("venue_stats", {})
    venue_names = sorted(venue_stats.keys())
    for venue in venue_names:
        stats = venue_stats[venue]
        if stats.get("trades", 0) < 8:
            continue
        for other in venue_names:
            if venue == other or venue_stats[other].get("trades", 0) < 8:
                continue
            wr_gap = _win_rate(venue_stats[other]) - _win_rate(stats)
            avg_pnl_gap = (venue_stats[other]["pnl"] / venue_stats[other]["trades"]) - (stats["pnl"] / stats["trades"])
            if wr_gap >= 0.15 and avg_pnl_gap > 0:
                recommendations.append({
                    "rule": "venue_underperformance",
                    "segment": venue,
                    "sample_size": stats["trades"],
                    "recommendation": (
                        f"Increase the {venue.title()} fee/risk buffer or temporarily disable {venue.title()} "
                        f"until its edge quality improves."
                    ),
                    "confidence": _confidence_label(stats["trades"]),
                })
                break

    for city, stats in sorted(analysis.get("city_stats", {}).items()):
        if stats.get("bias_count", 0) < 6:
            continue
        avg_bias = stats["bias_sum"] / stats["bias_count"]
        if abs(avg_bias) >= 1.5:
            direction = "too cold" if avg_bias > 0 else "too hot"
            recommendations.append({
                "rule": "city_bias_detected",
                "segment": city.replace("_", " ").title(),
                "sample_size": stats["bias_count"],
                "recommendation": (
                    f"Model runs are averaging {abs(avg_bias):.1f}F {direction} in "
                    f"{city.replace('_', ' ').title()}. Consider a city bias adjustment or "
                    f"a stricter edge threshold there."
                ),
                "confidence": _confidence_label(stats["bias_count"]),
            })

    consensus = analysis.get("stance_stats", {}).get("consensus", {})
    contrarian = analysis.get("stance_stats", {}).get("contrarian", {})
    if contrarian.get("trades", 0) >= 8 and consensus.get("trades", 0) >= 8:
        wr_gap = _win_rate(consensus) - _win_rate(contrarian)
        if wr_gap >= 0.15:
            recommendations.append({
                "rule": "contrarian_underperformance",
                "segment": "contrarian sizing",
                "sample_size": contrarian["trades"],
                "recommendation": "Lower contrarian sizing further until the win-rate gap narrows.",
                "confidence": _confidence_label(contrarian["trades"]),
            })

    return recommendations


def _format_recommendation_lines(recommendations: list[dict]) -> str:
    if not recommendations:
        return "No parameter change recommendation yet. Keep collecting evidence."

    lines = []
    for rec in recommendations:
        lines.append(
            f"• {rec['recommendation']} "
            f"(rule: {rec['rule']}, segment: {rec['segment']}, "
            f"n={rec['sample_size']}, confidence: {rec['confidence']})"
        )
    return "\n".join(lines)


def _format_active_param_line(params: dict) -> str:
    return (
        f"Min edge {format_param_value('edge_threshold', params['edge_threshold'])} | "
        f"Contrarian {format_param_value('contrarian_discount', params['contrarian_discount'])} | "
        f"Fee buffer {format_param_value('kalshi_fee_buffer_pct', params['kalshi_fee_buffer_pct'])} | "
        f"Max size {format_param_value('max_trade_size', params['max_trade_size'])}"
    )


def _format_tuning_change_lines(decision: dict) -> str:
    changes = decision.get("applied_changes", [])
    if not changes:
        held = decision.get("held_notes", [])
        if held:
            return "\n".join(f"• Holding: {note}" for note in held[:3])
        return "• No Kalshi parameter changes applied this week."

    lines = []
    for change in changes:
        lines.append(
            f"• {change['parameter']}: {format_param_value(change['parameter'], change['from'])} "
            f"→ {format_param_value(change['parameter'], change['to'])} "
            f"because {change['reason']}."
        )
    return "\n".join(lines)


def send_weekly_digest(mode: str = "paper"):
    """
    Generate and send the weekly learning digest to Slack.
    """
    logger.info("Generating weekly learning digest...")

    lookback_days = 14
    visible_venue = PRIMARY_VISIBLE_VENUE
    trades = _get_resolved_trades(days=lookback_days, mode=mode, venue=visible_venue)
    analysis = analyze_patterns(trades)
    insights = generate_insights(analysis)
    recommendations = generate_recommendations(analysis)
    base_params = get_base_strategy_params(visible_venue)
    current_params = get_effective_strategy_params(visible_venue)
    tuning_state = load_kalshi_tuning_state()
    tuning_decision = {
        "applied_changes": [],
        "held_notes": [],
        "effective_params": current_params,
    }
    if visible_venue == "kalshi":
        tuning_decision = evaluate_kalshi_tuning(
            analysis=analysis,
            base_params=base_params,
            current_params=current_params,
            state=tuning_state,
        )
        apply_kalshi_tuning(tuning_decision)

    mode_label = mode.upper()
    period = f"{(date.today() - timedelta(days=lookback_days)).strftime('%b %d')} – {date.today().strftime('%b %d')}"
    venue_label = visible_venue.title()

    if analysis["total"] == 0:
        send_slack_message(
            f"*Weekly Learning Digest ({mode_label}) — {venue_label}*\n"
            f"Lookback: {period}\n"
            f"• No resolved {venue_label} trades yet.\n"
            f"• Active params: {_format_active_param_line(tuning_decision['effective_params'])}\n"
            f"• No autonomous tuning change applied."
        )
        return

    city_lines = []
    for city, stats in sorted(
        analysis["city_stats"].items(),
        key=lambda x: x[1]["pnl"], reverse=True
    )[:3]:
        wr = stats["wins"] / stats["trades"] if stats["trades"] > 0 else 0
        city_name = city.replace("_", " ").title()
        city_lines.append(
            f"• {city_name}: {stats['trades']} trades | {wr:.0%} win rate | ${stats['pnl']:+.2f}"
        )

    calibration_lines = []
    for bucket, stats in sorted(analysis.get("calibration_stats", {}).items()):
        if stats.get("trades", 0) < 3:
            continue
        calibration_lines.append(
            f"• {bucket}: {_win_rate(stats):.0%} realized over {stats['trades']} trades"
        )

    insight_lines = "\n".join(f"• {item}" for item in insights[:3]) or "• Keep collecting evidence."
    recommendation_lines = _format_recommendation_lines(recommendations[:3]) if recommendations else "• No additional watchlist items this week."

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"Weekly Learning Digest ({mode_label}) — {venue_label}"}
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*Lookback:* {period}\n"
                    f"• {analysis['total']} resolved trades | {analysis['wins']}W / {analysis['losses']}L | "
                    f"{analysis['win_rate']:.0%} win rate\n"
                    f"• Total P&L: ${analysis['total_pnl']:+.2f} | Avg/trade: ${analysis['avg_pnl_per_trade']:+.2f}\n"
                    f"• Active params: {_format_active_param_line(tuning_decision['effective_params'])}"
                )
            }
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": "*Applied This Week:*\n" + _format_tuning_change_lines(tuning_decision)}
        },
    ]

    if city_lines:
        blocks.extend([
            {"type": "divider"},
            {"type": "section", "text": {"type": "mrkdwn", "text": "*Top Cities:*\n" + "\n".join(city_lines)}},
        ])

    if calibration_lines:
        calibration_header = "*Calibration:*\n"
        if analysis.get("calibration_mean_abs_error") is not None:
            calibration_header = (
                f"*Calibration:* mean abs error {analysis['calibration_mean_abs_error']:.0%}\n"
            )
        blocks.extend([
            {"type": "divider"},
            {"type": "section", "text": {"type": "mrkdwn", "text": calibration_header + "\n".join(calibration_lines[:4])}},
        ])

    blocks.extend([
        {"type": "divider"},
        {"type": "section", "text": {"type": "mrkdwn", "text": "*What We Learned:*\n" + insight_lines}},
        {"type": "divider"},
        {"type": "section", "text": {"type": "mrkdwn", "text": "*Still Watching:*\n" + recommendation_lines}},
    ])

    send_slack_blocks(
        blocks,
        text=f"Weekly {venue_label}: {analysis['wins']}W/{analysis['losses']}L, ${analysis['total_pnl']:+.2f}",
    )
    logger.info("Weekly digest sent to Slack")


def send_shadow_experiment_digest(days: int = 5, mode: str = "live", venue: str = "kalshi"):
    """Send the same-day shadow layer experiment conclusion to Slack."""
    logger.info("Generating same-day shadow experiment digest...")
    result = analyze_shadow_layer_experiment(days=days, mode=mode, venue=venue)
    summary = result["summary"]
    cases = result["cases"]

    mode_label = mode.upper()
    venue_label = venue.title()
    period = f"{(date.today() - timedelta(days=days)).strftime('%b %d')} – {date.today().strftime('%b %d')}"
    recommendation = summary.get("recommendation") or {}

    if summary.get("sample_size", 0) == 0:
        send_slack_message(
            f"*Shadow Layer Experiment ({mode_label}) — {venue_label}*\n"
            f"Lookback: {period}\n"
            f"• No settled same-day shadow cases yet.\n"
            f"• Recommendation: keep logging and re-check later."
        )
        return

    example_lines = []
    for case in sorted(cases, key=lambda item: item["replace_benefit"], reverse=True)[:3]:
        city = case["city"].replace("_", " ").title()
        example_lines.append(
            f"• {city} {case['target_date']}: hold ${case['held_pnl']:+.2f}, "
            f"replace ${case['shadow_pnl_estimate']:+.2f}, changed={case['shadow_changed']}"
        )

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"Shadow Layer Experiment ({mode_label}) — {venue_label}"}
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*Lookback:* {period}\n"
                    f"• Settled event-days with same-day shadow: {summary['sample_size']}\n"
                    f"• Material same-day thesis changes: {summary['changed_cases']}\n"
                    f"• Hold P&L: ${summary['hold_pnl']:+.2f}\n"
                    f"• Same-day replace P&L estimate: ${summary['shadow_replace_pnl']:+.2f}\n"
                    f"• Same-day veto P&L estimate: ${summary['veto_pnl']:+.2f}"
                )
            }
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*Recommendation:* `{recommendation.get('layer', 'collect_more_data')}`\n"
                    f"{recommendation.get('reason', 'No recommendation.')}"
                )
            }
        },
    ]

    if example_lines:
        blocks.extend([
            {"type": "divider"},
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "*Examples:*\n" + "\n".join(example_lines)}
            },
        ])

    send_slack_blocks(
        blocks,
        text=(
            f"Shadow layer experiment ({mode_label}) — {venue_label}: "
            f"hold ${summary['hold_pnl']:+.2f}, replace ${summary['shadow_replace_pnl']:+.2f}, "
            f"recommend {recommendation.get('layer', 'collect_more_data')}"
        ),
    )
    logger.info("Shadow experiment digest sent to Slack")
