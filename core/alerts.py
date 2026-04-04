"""
Slack alerting and structured logging.

Slack gets a COMPACT summary (3-5 lines).
Full trade-by-trade explanations are saved to dated markdown files on the droplet.
"""

import json
import logging
import requests
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from config.settings import SLACK_WEBHOOK_URL, LOG_DIR, LOG_LEVEL, PROJECT_ROOT

logger = logging.getLogger(__name__)

# Directory for detailed trade reports
REPORTS_DIR = PROJECT_ROOT / "data" / "reports"


# ============================================================
# Logging Setup
# ============================================================

def setup_logging():
    """Configure structured logging to console and file."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    log_file = LOG_DIR / f"bot_{datetime.now().strftime('%Y%m%d')}.log"

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console = logging.StreamHandler()
    console.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
    console.setFormatter(fmt)

    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(fmt)

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.addHandler(console)
    root.addHandler(file_handler)


# ============================================================
# Slack Messaging
# ============================================================

def _send_slack(payload: dict) -> bool:
    """Send a raw payload to the Slack webhook."""
    if not SLACK_WEBHOOK_URL:
        logger.debug("Slack webhook not configured, skipping alert")
        return False

    try:
        resp = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
        if resp.status_code == 200:
            return True
        else:
            logger.warning(f"Slack webhook returned {resp.status_code}: {resp.text}")
            return False
    except requests.RequestException as e:
        logger.warning(f"Slack webhook failed: {e}")
        return False


def send_slack_message(text: str) -> bool:
    """Send a simple text message to Slack."""
    return _send_slack({"text": text})


def send_slack_blocks(blocks: list[dict], text: str = "") -> bool:
    """Send a rich block-formatted message to Slack."""
    return _send_slack({"text": text, "blocks": blocks})


# ============================================================
# Fee & Payout Calculation
# ============================================================

WEATHER_FEE_RATE = 0.025
WEATHER_FEE_EXPONENT = 0.5


def calc_fee_pct(price: float) -> float:
    """Effective taker fee percentage for a weather market trade."""
    if price <= 0 or price >= 1:
        return 0.0
    return WEATHER_FEE_RATE * (price * (1 - price)) ** WEATHER_FEE_EXPONENT


def calc_fee_usd(size_usd: float, price: float) -> float:
    """Fee in USD for a given trade size and price."""
    return size_usd * calc_fee_pct(price)


def calc_payout(size_usd: float, price: float, side: str) -> dict:
    """
    Correct Polymarket payout math.
    YES shares cost $price each. NO shares cost $(1-price) each.
    If you win, each share pays $1.
    """
    fee = calc_fee_usd(size_usd, price)
    net_size = max(size_usd - fee, 0)

    if side == "BUY":  # YES
        share_price = price
    else:  # SELL = buying NO
        share_price = 1.0 - price

    if share_price < 0.03 or share_price > 0.97:
        return {"payout": 0, "profit": 0, "fee": round(fee, 2), "shares": 0}

    shares = net_size / share_price
    payout = shares
    profit = payout - size_usd

    return {
        "payout": round(payout, 2),
        "profit": round(profit, 2),
        "fee": round(fee, 2),
        "shares": round(shares, 1),
    }


# ============================================================
# Helpers
# ============================================================

def _c_to_f(c: float) -> float:
    return c * 9 / 5 + 32


def _bucket_label_f(sig: dict) -> str:
    """Build a clean bucket label, always in Fahrenheit."""
    low = sig.get("temp_low")
    high = sig.get("temp_high")
    is_f = sig.get("is_fahrenheit", True)
    if low is not None and high is not None:
        if not is_f:
            low = _c_to_f(low) if low != -999 else -999
            high = _c_to_f(high) if high != 999 else 999
        if low == -999:
            return f"{high:.0f}\u00b0F or below"
        elif high == 999:
            return f"{low:.0f}\u00b0F or higher"
        else:
            return f"{low:.0f}-{high:.0f}\u00b0F"
    return sig.get("bucket_question", "Unknown bucket")[:40]


def _yes_no(side: str) -> str:
    return "YES" if side == "BUY" else "NO"


def _friendly_date(date_str: str) -> str:
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return dt.strftime("%a %b %-d")
    except (ValueError, TypeError):
        return date_str or "?"


# ============================================================
# Detailed Markdown Report (saved to droplet filesystem)
# ============================================================

def _build_trade_narrative(sig: dict) -> str:
    """Build a full natural language explanation for one trade (for the markdown file)."""
    city = sig.get("city", "?").replace("_", " ").title()
    date = _friendly_date(sig.get("target_date"))
    bucket = _bucket_label_f(sig)
    side = sig.get("side", "BUY")
    yes_no = _yes_no(side)
    size = sig.get("trade_size", 0)
    market_prob = sig.get("market_prob", 0)
    ensemble_prob = sig.get("ensemble_prob", 0)
    edge = sig.get("edge", 0)
    is_contrarian = sig.get("is_contrarian", False)
    nws = sig.get("nws_forecast")
    meta = sig.get("ensemble_meta", {})

    payout = calc_payout(size, market_prob, side)

    # NWS line
    nws_line = ""
    if nws and nws.get("temp") is not None:
        nws_temp = nws["temp"]
        if nws.get("unit", "F") == "C":
            nws_temp = _c_to_f(nws_temp)
        short_fc = nws.get("short_forecast", "")
        conditions = f" with {short_fc.lower()}" if short_fc else ""
        nws_line = f"NWS forecasts a high of {nws_temp:.0f}\u00b0F{conditions}."
    else:
        mean_temp = meta.get("mean")
        if mean_temp:
            nws_line = f"Forecast models predict a high around {mean_temp:.0f}\u00b0F."

    # Ensemble line
    member_count = meta.get("member_count", 31)
    members_in_bucket = round(ensemble_prob * member_count)
    mean_temp = meta.get("mean", 0)
    temp_range_low = meta.get("min", 0)
    temp_range_high = meta.get("max", 0)

    ensemble_line = (
        f"Our {member_count} GFS simulations: "
        f"{members_in_bucket} of {member_count} runs land in the {bucket} range "
        f"({ensemble_prob:.0%} likely). "
        f"Simulations range from {temp_range_low:.0f}\u00b0F to {temp_range_high:.0f}\u00b0F, "
        f"averaging {mean_temp:.0f}\u00b0F."
    )

    # Mispricing line
    mkt_pct = market_prob * 100
    ens_pct = ensemble_prob * 100
    edge_pct = abs(edge) * 100

    if side == "BUY":
        mispricing_line = (
            f"Polymarket prices this at {mkt_pct:.0f}%, but our models say {ens_pct:.0f}%. "
            f"The market is undervaluing this outcome by {edge_pct:.0f}%."
        )
    else:
        mispricing_line = (
            f"Polymarket prices this at {mkt_pct:.0f}%, but our models say only {ens_pct:.0f}%. "
            f"The market is overpricing this outcome by {edge_pct:.0f}%."
        )

    # Trade line
    stance = "Against crowd" if is_contrarian else "With crowd"
    stance_note = " (60% size)" if is_contrarian else ""

    if payout["profit"] > 0:
        trade_line = (
            f"**{yes_no} ${size:.0f} to win ${payout['payout']:.0f} "
            f"(+${payout['profit']:.0f}) | "
            f"Edge {edge_pct:.0f}% | {stance}{stance_note}**"
        )
    else:
        trade_line = (
            f"**{yes_no} ${size:.0f} | "
            f"Edge {edge_pct:.0f}% | {stance}{stance_note}**"
        )

    return (
        f"### {city} ({date}) | {bucket}\n\n"
        f"{nws_line} {ensemble_line}\n\n"
        f"{mispricing_line}\n\n"
        f"{trade_line}\n"
    )


def save_scan_report(executed: list[dict], total_signals: int,
                     bankroll: float, mode: str = "paper") -> str:
    """
    Save a detailed markdown report of the scan to the droplet filesystem.
    Returns the file path.
    """
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    now = datetime.now(timezone.utc)
    filename = f"scan_{now.strftime('%Y-%m-%d_%H%M')}.md"
    filepath = REPORTS_DIR / filename

    mode_label = "PAPER" if mode == "paper" else "LIVE"
    total_invested = sum(s.get("trade_size", 0) for s in executed)
    total_fees = sum(calc_fee_usd(s.get("trade_size", 0), s.get("market_prob", 0)) for s in executed)
    consensus_count = sum(1 for s in executed if not s.get("is_contrarian"))
    contrarian_count = sum(1 for s in executed if s.get("is_contrarian"))
    yes_count = sum(1 for s in executed if s.get("side") == "BUY")
    no_count = sum(1 for s in executed if s.get("side") == "SELL")

    # Group by target date
    by_date = {}
    for s in executed:
        d = s.get("target_date", "unknown")
        by_date.setdefault(d, []).append(s)

    lines = [
        f"# {mode_label} Scan Report \u2014 {now.strftime('%a %b %-d, %Y at %H:%M UTC')}\n",
        f"**{len(executed)} trades placed** out of {total_signals} signals found\n",
        f"| Metric | Value |",
        f"|---|---|",
        f"| Total Invested | ${total_invested:.0f} |",
        f"| Estimated Fees | ${total_fees:.2f} |",
        f"| Remaining Bankroll | ${bankroll:.0f} |",
        f"| YES Trades | {yes_count} |",
        f"| NO Trades | {no_count} |",
        f"| With Crowd | {consensus_count} |",
        f"| Against Crowd | {contrarian_count} |",
        f"",
    ]

    for target_date in sorted(by_date.keys()):
        trades = by_date[target_date]
        lines.append(f"\n## {_friendly_date(target_date)} ({target_date})\n")
        for sig in trades:
            lines.append(_build_trade_narrative(sig))
            lines.append("---\n")

    content = "\n".join(lines)
    filepath.write_text(content)
    logger.info(f"Scan report saved to {filepath}")
    return str(filepath)


def save_resolution_report(details: list[dict], summary: dict) -> str:
    """
    Save a detailed markdown report of resolved trades.
    Returns the file path.
    """
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    now = datetime.now(timezone.utc)
    filename = f"resolution_{now.strftime('%Y-%m-%d')}.md"
    filepath = REPORTS_DIR / filename

    wins = summary.get("wins", 0)
    losses = summary.get("losses", 0)
    pnl = summary.get("pnl", 0)

    lines = [
        f"# Resolution Report \u2014 {now.strftime('%a %b %-d, %Y')}\n",
        f"**{len(details)} trades resolved** | {wins}W / {losses}L | P&L: ${pnl:+.2f}\n",
        f"",
    ]

    if details:
        lines.append("| City | Date | Bucket | Bet | Stake | Actual Temp | Result | P&L |")
        lines.append("|---|---|---|---|---|---|---|---|")
        for d in details:
            result_emoji = "WIN" if d["won"] else "LOSS"
            lines.append(
                f"| {d['city']} | {d['target_date']} | "
                f"{d['bucket'][:30]} | {d['side']} | "
                f"${d['size']:.0f} | {d['actual_temp_f']:.1f}\u00b0F | "
                f"{result_emoji} | ${d['pnl']:+.2f} |"
            )

        lines.append("")

        # Add narrative for each trade
        lines.append("## Trade-by-Trade Analysis\n")
        for d in details:
            result = "WON" if d["won"] else "LOST"
            in_bucket = "YES, it landed in this range" if d["in_bucket"] else "NO, it did not land in this range"
            mkt = d["market_price"] * 100
            ens = d["ensemble_prob"] * 100

            lines.append(f"### {d['city']} \u2014 {d['bucket'][:50]}\n")
            lines.append(
                f"The actual high was **{d['actual_temp_f']:.1f}\u00b0F**. "
                f"Did it land in the bucket? **{in_bucket}.**\n"
            )
            lines.append(
                f"We bet **{d['side']}** at ${d['size']:.0f}. "
                f"Polymarket had it at {mkt:.0f}%, our model said {ens:.0f}%. "
                f"**Result: {result} (${d['pnl']:+.2f})**\n"
            )
            lines.append("---\n")

    content = "\n".join(lines)
    filepath.write_text(content)
    logger.info(f"Resolution report saved to {filepath}")
    return str(filepath)


# ============================================================
# COMPACT Slack Alerts
# ============================================================

def alert_scan_summary(executed: list[dict], total_signals: int,
                       bankroll: float, mode: str = "paper"):
    """
    Send a COMPACT Slack summary (5-8 lines max).
    Full details are in the markdown report on the droplet.
    """
    if not executed:
        return

    # Save the detailed report first
    report_path = save_scan_report(executed, total_signals, bankroll, mode)

    mode_label = "PAPER" if mode == "paper" else "LIVE"
    now = datetime.now(timezone.utc).strftime("%H:%M UTC")
    total_invested = sum(s.get("trade_size", 0) for s in executed)
    total_potential = sum(
        calc_payout(s.get("trade_size", 0), s.get("market_prob", 0), s.get("side", "SELL")).get("profit", 0)
        for s in executed
    )
    yes_count = sum(1 for s in executed if s.get("side") == "BUY")
    no_count = sum(1 for s in executed if s.get("side") == "SELL")
    consensus_count = sum(1 for s in executed if not s.get("is_contrarian"))
    contrarian_count = sum(1 for s in executed if s.get("is_contrarian"))
    avg_edge = sum(abs(s.get("edge", 0)) for s in executed) / len(executed) * 100

    # Group by date for a quick summary
    dates = sorted(set(s.get("target_date", "") for s in executed))
    cities = sorted(set(s.get("city", "").replace("_", " ").title() for s in executed))

    # Build compact one-liner per trade
    trade_lines = []
    for s in executed:
        city = s.get("city", "?").replace("_", " ").title()
        bucket = _bucket_label_f(s)
        yes_no = _yes_no(s.get("side", "BUY"))
        size = s.get("trade_size", 0)
        payout = calc_payout(size, s.get("market_prob", 0), s.get("side", "SELL"))
        edge_pct = abs(s.get("edge", 0)) * 100
        stance = ":handshake:" if not s.get("is_contrarian") else ":eyes:"
        trade_lines.append(
            f"{stance} {city} | {bucket} | {yes_no} ${size:.0f}\u2192${payout['payout']:.0f} | Edge {edge_pct:.0f}%"
        )

    # Compact Slack message
    header_text = f"{mode_label} Scan \u2014 {len(executed)} trades placed"

    summary_text = (
        f"*{len(executed)} trades* | "
        f"{yes_count} YES / {no_count} NO | "
        f"{consensus_count} with crowd / {contrarian_count} against | "
        f"Avg edge {avg_edge:.0f}%\n"
        f"*${total_invested:.0f} invested* | "
        f"*${total_potential:+.0f} potential profit* | "
        f"${bankroll:.0f} remaining\n"
        f"Markets: {', '.join(_friendly_date(d) for d in dates)} | "
        f"Cities: {', '.join(cities[:6])}"
        f"{'...' if len(cities) > 6 else ''}"
    )

    # Compact trade list (one line each)
    trades_text = "\n".join(trade_lines)

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": header_text}
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": summary_text}
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": trades_text}
        },
        {
            "type": "context",
            "elements": [{
                "type": "mrkdwn",
                "text": f":page_facing_up: Full analysis: `{report_path}`"
            }]
        },
    ]

    send_slack_blocks(blocks, text=f"{mode_label}: {len(executed)} trades, ${total_invested:.0f} invested")


def alert_daily_summary(stats: dict):
    """
    Send a compact daily resolution pulse to Slack.
    Full details saved to markdown report.
    """
    date = stats.get("date", "Today")
    resolved = stats.get("trades_resolved", 0)
    daily_pnl = stats.get("daily_pnl", 0)
    wins = stats.get("wins", 0)
    losses = stats.get("losses", 0)
    details = stats.get("details", [])
    total_pnl = stats.get("total_pnl", 0)
    all_time_wr = stats.get("all_time_win_rate", 0)
    all_time_trades = stats.get("all_time_trades", 0)
    pending = stats.get("pending_trades", 0)

    # Save detailed resolution report
    report_path = ""
    if details:
        report_path = save_resolution_report(details, stats)

    if daily_pnl > 0:
        verdict = ":chart_with_upwards_trend: Good day"
    elif daily_pnl == 0 and resolved == 0:
        verdict = ":hourglass_flowing_sand: No trades resolved yet"
    elif daily_pnl == 0:
        verdict = ":neutral_face: Broke even"
    else:
        verdict = ":chart_with_downwards_trend: Rough day"

    # Build compact resolution lines
    result_lines = []
    for d in details:
        emoji = ":white_check_mark:" if d["won"] else ":x:"
        result_lines.append(
            f"{emoji} {d['city']} | {d['bucket'][:25]} | "
            f"Actual: {d['actual_temp_f']:.0f}\u00b0F | "
            f"${d['pnl']:+.2f}"
        )

    summary_text = (
        f"*{verdict}*\n\n"
        f"*Today:* {resolved} resolved ({wins}W / {losses}L) | "
        f"P&L: *${daily_pnl:+.2f}*\n"
        f"*All-time:* {all_time_trades} trades | "
        f"{all_time_wr:.0%} win rate | "
        f"P&L: *${total_pnl:+.2f}*"
    )

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"Daily Pulse \u2014 {date}"}
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": summary_text}
        },
    ]

    if result_lines:
        blocks.append({"type": "divider"})
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\n".join(result_lines)}
        })

    footer_parts = []
    if pending > 0:
        footer_parts.append(f":hourglass_flowing_sand: {pending} trades still pending")
    if report_path:
        footer_parts.append(f":page_facing_up: Full report: `{report_path}`")

    if footer_parts:
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": " | ".join(footer_parts)}]
        })

    send_slack_blocks(blocks, text=f"Daily pulse: ${daily_pnl:+.2f} | {verdict}")


# ============================================================
# Error & Startup
# ============================================================

def alert_error(error_msg: str, context: str = ""):
    """Send an error alert."""
    text = (
        f":rotating_light: *Bot Error*\n"
        f"Something went wrong{f' during {context}' if context else ''}:\n"
        f"```{error_msg[:500]}```"
    )
    send_slack_message(text)


def alert_bot_started(mode: str):
    """Send a startup notification."""
    mode_label = mode.upper()
    text = (
        f":rocket: *Weather Bot Started* ({mode_label})\n"
        f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')} | "
        f"US cities only | 1-2 day forecast horizon | "
        f"Min 8% edge after fees"
    )
    send_slack_message(text)
