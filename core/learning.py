"""
Learning Digest — Weekly Pattern Analysis.

Analyzes resolved trades to find patterns in wins vs losses,
identifies which cities/edges/sides perform best, and sends
a Slack digest with actionable insights.

Designed to inform gradual strategy tuning — not hair-trigger changes.
"""

import logging
from datetime import datetime, date, timedelta, timezone
from collections import defaultdict

from core.database import session_scope, Trade
from core.alerts import send_slack_blocks, send_slack_message

logger = logging.getLogger(__name__)


def _get_resolved_trades(days: int = 7, mode: str = "paper") -> list[dict]:
    """Fetch resolved trades from the past N days."""
    cutoff = (date.today() - timedelta(days=days)).strftime("%Y-%m-%d")
    with session_scope() as session:
        trades = (
            session.query(Trade)
            .filter(Trade.mode == mode, Trade.resolved == True,
                    Trade.target_date >= cutoff)
            .all()
        )
        return [
            {
                "id": t.id,
                "city": t.city,
                "target_date": t.target_date,
                "side": t.side,
                "size_usd": t.size_usd,
                "price": t.price,
                "ensemble_prob": t.ensemble_prob,
                "edge": t.edge,
                "outcome": t.outcome,
                "pnl": t.pnl,
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
    city_stats = defaultdict(lambda: {"wins": 0, "losses": 0, "pnl": 0.0, "trades": 0})
    for t in trades:
        city = t["city"]
        city_stats[city]["trades"] += 1
        city_stats[city]["pnl"] += t["pnl"]
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
        "best_cities": best_cities,
        "worst_cities": worst_cities,
    }


def generate_insights(analysis: dict) -> list[str]:
    """
    Generate plain-English insights from the analysis.
    These are observations, not automatic strategy changes.
    """
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


def send_weekly_digest(mode: str = "paper"):
    """
    Generate and send the weekly learning digest to Slack.
    """
    logger.info("Generating weekly learning digest...")

    trades = _get_resolved_trades(days=7, mode=mode)
    analysis = analyze_patterns(trades)
    insights = generate_insights(analysis)

    mode_label = mode.upper()
    period = f"{(date.today() - timedelta(days=7)).strftime('%b %d')} – {date.today().strftime('%b %d')}"

    if analysis["total"] == 0:
        send_slack_message(
            f":books: *Weekly Learning Digest ({mode_label})*\n"
            f"Period: {period}\n\n"
            f"No resolved trades this week yet. The bot is collecting data — "
            f"insights will appear once we have results to analyze."
        )
        return

    # Build the Slack message
    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"Weekly Learning Digest ({mode_label})"}
        },
        {
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": f"Period: {period}"}]
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*Overall Performance:*\n"
                    f"Trades: {analysis['total']} | "
                    f"Record: {analysis['wins']}W / {analysis['losses']}L | "
                    f"Win Rate: {analysis['win_rate']:.0%}\n"
                    f"Total P&L: ${analysis['total_pnl']:+.2f} | "
                    f"Avg per trade: ${analysis['avg_pnl_per_trade']:+.2f}"
                )
            }
        },
        {"type": "divider"},
    ]

    # City breakdown (top 5)
    city_lines = []
    for city, stats in sorted(
        analysis["city_stats"].items(),
        key=lambda x: x[1]["pnl"], reverse=True
    )[:5]:
        wr = stats["wins"] / stats["trades"] if stats["trades"] > 0 else 0
        city_name = city.replace("_", " ").title()
        city_lines.append(
            f"  {city_name}: {stats['trades']} trades, "
            f"{wr:.0%} win rate, ${stats['pnl']:+.2f}"
        )

    if city_lines:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*Top Cities:*\n" + "\n".join(city_lines)
            }
        })
        blocks.append({"type": "divider"})

    # Insights
    blocks.append({
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": "*Key Insights:*\n" + "\n".join(f"• {i}" for i in insights)
        }
    })

    blocks.append({
        "type": "context",
        "elements": [{
            "type": "mrkdwn",
            "text": (
                "_These insights are observational. Strategy changes should only be made "
                "after consistent patterns over 2+ weeks._"
            )
        }]
    })

    send_slack_blocks(blocks, text=f"Weekly digest: {analysis['wins']}W/{analysis['losses']}L, ${analysis['total_pnl']:+.2f}")
    logger.info("Weekly digest sent to Slack")
