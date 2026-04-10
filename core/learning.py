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
from core.database import session_scope, Trade
from core.alerts import send_slack_blocks, send_slack_message
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
                "side": t.side,
                "size_usd": t.size_usd,
                "price": t.price,
                "ensemble_prob": t.ensemble_prob,
                "edge": t.edge,
                "outcome": t.outcome,
                "pnl": t.pnl,
                "actual_temp": t.actual_temp,
                "model_expected_high": t.model_expected_high,
                "forecast_context": json.loads(t.forecast_context_json) if t.forecast_context_json else {},
                "is_contrarian": bool(t.is_contrarian),
                "strategy_version": t.strategy_version,
            }
            for t in trades
        ]


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
