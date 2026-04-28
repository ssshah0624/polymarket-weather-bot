"""
Slack alerting and structured logging.

Slack gets a COMPACT summary (3-5 lines).
Full trade-by-trade explanations are saved to dated markdown files on the droplet.
"""

import json
import logging
import math
import requests
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from config.settings import (
    KALSHI_API_KEY_ID,
    KALSHI_PRIVATE_KEY_PATH,
    KALSHI_USE_DEMO,
    LOG_DIR,
    LOG_LEVEL,
    PRIMARY_VISIBLE_VENUE,
    PROJECT_ROOT,
    SLACK_INCLUDE_REPORT_LINKS,
    SLACK_WEBHOOK_URL,
)
from core.tuning import get_effective_strategy_params

logger = logging.getLogger(__name__)

# Directory for detailed trade reports
REPORTS_DIR = PROJECT_ROOT / "data" / "reports"
SLACK_SECTION_TEXT_LIMIT = 3000
SLACK_DAILY_RESULTS_CHAR_BUDGET = 2800
SLACK_DAILY_RESULTS_MAX_LINES = 8
SLACK_SCAN_BETS_CHAR_BUDGET = 2400
SLACK_SCAN_BETS_MAX_LINES = 8


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


def calc_fee_pct(price: float, venue: str = "polymarket") -> float:
    """Effective fee percentage for a venue trade."""
    if venue == "kalshi":
        return max(get_effective_strategy_params(venue).get("kalshi_fee_buffer_pct", 0.0), 0.0)
    if price <= 0 or price >= 1:
        return 0.0
    return WEATHER_FEE_RATE * (price * (1 - price)) ** WEATHER_FEE_EXPONENT


def calc_fee_usd(size_usd: float, price: float, venue: str = "polymarket",
                 fee_pct: float | None = None) -> float:
    """Fee in USD for a given trade size and price."""
    effective_fee_pct = fee_pct if fee_pct is not None else calc_fee_pct(price, venue=venue)
    return size_usd * effective_fee_pct


def calc_payout(size_usd: float, price: float, side: str,
                entry_price: float | None = None,
                fee_pct: float | None = None,
                venue: str = "polymarket") -> dict:
    """
    Correct Polymarket payout math.
    YES shares cost $price each. NO shares cost $(1-price) each.
    If you win, each share pays $1.
    """
    fee = calc_fee_usd(size_usd, price, venue=venue, fee_pct=fee_pct)
    net_size = max(size_usd - fee, 0)

    if side == "BUY":  # YES
        share_price = entry_price if entry_price is not None else price
    else:  # SELL = buying NO
        share_price = entry_price if entry_price is not None else 1.0 - price

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
            display_high = high if (high - low) <= 1 else high - 1
            if display_high <= low:
                return f"{low:.0f}\u00b0F"
            return f"{low:.0f}-{display_high:.0f}\u00b0F"
    return sig.get("bucket_question", "Unknown bucket")[:40]


def _yes_no(side: str) -> str:
    return "YES" if side == "BUY" else "NO"


def _venue_label(value: str) -> str:
    return (value or "polymarket").replace("_", " ").title()


def _is_slack_visible_venue(value: str) -> bool:
    return (value or "polymarket") == PRIMARY_VISIBLE_VENUE


def _filter_slack_visible_items(items: list[dict]) -> list[dict]:
    return [item for item in items if _is_slack_visible_venue(item.get("venue"))]


def _display_city(value: str) -> str:
    city = (value or "").replace("_", " ").title()
    return "NYC" if city == "Nyc" else city


def _friendly_date(date_str: str) -> str:
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return dt.strftime("%a %b %-d")
    except (ValueError, TypeError):
        return date_str or "?"


def _format_temp_value(value: Optional[float]) -> str:
    if value is None:
        return "-"
    return f"{value:.1f}F"


def _truncate(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    return value[: max(limit - 1, 0)] + "…"


def _format_selected_bets(selected_bets: list[dict]) -> str:
    if not selected_bets:
        return "-"
    parts = []
    for bet in selected_bets:
        venue = "Poly" if bet.get("venue") == "polymarket" else "Kalshi"
        bucket = _truncate(bet.get("bucket_question", ""), 16)
        side = _yes_no(bet.get("side", "BUY"))
        parts.append(f"{venue} {side} {bucket}")
    return "; ".join(parts)


def _has_meaningful_market_view(row: dict) -> bool:
    return any(
        row.get(field) is not None
        for field in ("model_expected_high", "polymarket_implied_high", "kalshi_implied_high")
    ) or bool(row.get("selected_bets"))


def _select_alert_rows(comparison_rows: list[dict], max_rows: int = 8) -> list[dict]:
    meaningful = [row for row in comparison_rows if _has_meaningful_market_view(row)]
    selected = [row for row in meaningful if row.get("selected_bets")]
    rows = selected or meaningful
    rows.sort(
        key=lambda row: (
            0 if row.get("selected_bets") else 1,
            row.get("target_date", ""),
            row.get("city", ""),
        )
    )
    return rows[:max_rows]


def _build_comparison_table(comparison_rows: list[dict], max_rows: int = 8) -> str:
    rows = _select_alert_rows(comparison_rows, max_rows=max_rows)
    lines = [
        f"{'City':<12} {'Date':<11} {'Model High':>10} {'Poly':>7} {'Kalshi':>7}  Bets",
        f"{'-'*12} {'-'*11} {'-'*7} {'-'*7} {'-'*7}  {'-'*24}",
    ]
    for row in rows:
        city = _truncate(row.get("city", "").replace("_", " ").title(), 12)
        date = row.get("target_date", "")
        model = _format_temp_value(row.get("model_expected_high"))
        poly = _format_temp_value(row.get("polymarket_implied_high"))
        kalshi = _format_temp_value(row.get("kalshi_implied_high"))
        bets = _truncate(_format_selected_bets(row.get("selected_bets") or []), 48)
        lines.append(f"{city:<12} {date:<11} {model:>10} {poly:>7} {kalshi:>7}  {bets}")
    meaningful_count = len([row for row in comparison_rows if _has_meaningful_market_view(row)])
    if meaningful_count > len(rows):
        lines.append(f"...and {meaningful_count - len(rows)} more rows in report")
    return "\n".join(lines)


def _build_bets_table(executed: list[dict], max_rows: int = 10) -> str:
    rows = executed[:max_rows]
    lines = [
        f"{'Venue':<10} {'City':<12} {'Side':<4} {'Contract':<16} {'Size':>6} {'Edge':>6}",
        f"{'-'*10} {'-'*12} {'-'*4} {'-'*16} {'-'*6} {'-'*6}",
    ]
    for trade in rows:
        venue = "Poly" if trade.get("venue") == "polymarket" else "Kalshi"
        city = _truncate(trade.get("city", "").replace("_", " ").title(), 12)
        side = _yes_no(trade.get("side", "BUY"))
        contract = _truncate(_bucket_label_f(trade), 16)
        size = f"${trade.get('trade_size', 0):.0f}"
        edge = f"{abs(trade.get('edge', 0))*100:.0f}%"
        lines.append(f"{venue:<10} {city:<12} {side:<4} {contract:<16} {size:>6} {edge:>6}")
    if len(executed) > max_rows:
        lines.append(f"...and {len(executed) - max_rows} more trades in report")
    return "\n".join(lines)


def _sort_executed_for_alert(executed: list[dict]) -> list[dict]:
    """Show nearby markets together, with the strongest bets first within each date."""
    return sorted(
        executed,
        key=lambda trade: (
            trade.get("target_date", ""),
            -abs(trade.get("edge", 0)),
            trade.get("city", ""),
            trade.get("venue", ""),
        ),
    )


def _build_comparison_lookup(comparison_rows: list[dict]) -> dict[tuple[str, str], dict]:
    return {
        (row.get("city", ""), row.get("target_date", "")): row
        for row in comparison_rows
    }


def _selected_market_price(trade: dict) -> Optional[float]:
    entry_price = trade.get("entry_price")
    if entry_price is not None:
        return entry_price
    market_prob = trade.get("market_prob")
    if market_prob is None:
        return None
    if trade.get("side") == "SELL":
        return 1.0 - market_prob
    return market_prob


def _market_implied_temp_for_trade(trade: dict, comparison_lookup: dict[tuple[str, str], dict]) -> Optional[float]:
    row = comparison_lookup.get((trade.get("city", ""), trade.get("target_date", "")))
    if not row:
        return None
    if trade.get("venue") == "kalshi":
        return row.get("kalshi_implied_high")
    return row.get("polymarket_implied_high")


def _build_trade_reason(trade: dict, comparison_lookup: dict[tuple[str, str], dict]) -> str:
    model_temp = trade.get("model_expected_high")
    market_temp = _market_implied_temp_for_trade(trade, comparison_lookup)
    side = trade.get("side", "BUY")
    stance = " Against crowd." if trade.get("is_contrarian") else ""
    temp_low = trade.get("temp_low")
    temp_high = trade.get("temp_high")
    is_open_ended = temp_low in (-999, -999.0) or temp_high in (999, 999.0)

    if (not is_open_ended) and model_temp is not None and market_temp is not None:
        if side == "BUY" and model_temp > market_temp:
            return (
                f"Why: model {model_temp:.1f}F vs market-implied {market_temp:.1f}F, "
                f"so {PRIMARY_VISIBLE_VENUE.title()} looks too cheap on this bucket.{stance}"
            )
        if side == "SELL" and model_temp < market_temp:
            return (
                f"Why: model {model_temp:.1f}F vs market-implied {market_temp:.1f}F, "
                f"so {PRIMARY_VISIBLE_VENUE.title()} looks too hot on this bucket.{stance}"
            )

    model_prob = trade.get("selected_prob")
    market_price = _selected_market_price(trade)
    if model_prob is not None and market_price is not None:
        return (
            f"Why: model {model_prob:.0%} vs market {market_price:.0%}, "
            f"so this position looks underpriced.{stance}"
        )

    edge_pct = abs(trade.get("edge", 0)) * 100
    return f"Why: the model still shows a {edge_pct:.0f}% edge after fees.{stance}"


def _build_trade_context_line(trade: dict, comparison_lookup: dict[tuple[str, str], dict]) -> Optional[str]:
    if not (trade.get("is_contrarian") or abs(trade.get("edge", 0)) >= 0.20):
        return None

    parts = []
    nws_temp = (trade.get("nws_forecast") or {}).get("temp")
    if nws_temp is not None:
        parts.append(f"NWS {nws_temp:.0f}F")

    model_temp = trade.get("model_expected_high")
    if model_temp is not None:
        parts.append(f"Ensemble mean {model_temp:.1f}F")

    market_temp = trade.get("venue_implied_high")
    if market_temp is None:
        market_temp = _market_implied_temp_for_trade(trade, comparison_lookup)
    if market_temp is not None:
        parts.append(f"Market center {market_temp:.1f}F")

    member_count = (trade.get("ensemble_meta") or {}).get("member_count")
    ensemble_prob = trade.get("ensemble_prob")
    temp_low = trade.get("temp_low")
    temp_high = trade.get("temp_high")
    if member_count and ensemble_prob is not None and temp_low is not None and temp_high is not None:
        hits = int(round(float(ensemble_prob) * int(member_count)))
        if temp_low in (-999, -999.0):
            parts.append(f"{hits}/{member_count} members <= {temp_high:.0f}F")
        elif temp_high in (999, 999.0):
            parts.append(f"{hits}/{member_count} members >= {temp_low:.0f}F")
        else:
            display_high = temp_high - 1
            if display_high <= temp_low:
                parts.append(f"{hits}/{member_count} members at {temp_low:.0f}F")
            else:
                parts.append(f"{hits}/{member_count} members in {temp_low:.0f}-{display_high:.0f}F")

    if len(parts) < 2:
        return None
    return "Context: " + " | ".join(parts)


def _build_live_fill_line(trade: dict) -> Optional[str]:
    contracts = trade.get("filled_contracts")
    fill_price = trade.get("fill_price") or trade.get("entry_price")
    if not contracts or fill_price is None:
        return None

    line = f"Fill: {int(contracts)} contracts @ {fill_price * 100:.0f}¢"
    expected = trade.get("expected_entry_price")
    if expected is not None:
        drift_cents = (fill_price - expected) * 100.0
        if abs(drift_cents) >= 1:
            direction = "+" if drift_cents > 0 else ""
            line += f" ({direction}{drift_cents:.0f}¢ vs expected)"
    return line


def _build_trade_alert_entry(trade: dict, comparison_lookup: dict[tuple[str, str], dict]) -> str:
    city = _display_city(trade.get("city", ""))
    payout = calc_payout(
        trade.get("trade_size", 0),
        trade.get("market_prob", 0),
        trade.get("side", "SELL"),
        entry_price=trade.get("entry_price"),
        fee_pct=trade.get("fee_pct"),
        venue=trade.get("venue", "polymarket"),
    )
    stake_display = math.floor(trade.get("trade_size", 0))
    payout_display = math.floor(payout.get("payout", 0))
    lines = [
        f"• {_friendly_date(trade.get('target_date', ''))} | {city} | "
        f"${stake_display} -> ${payout_display} | Edge {abs(trade.get('edge', 0)) * 100:.0f}%",
        f"  Betting {_yes_no(trade.get('side', 'BUY'))} on {_bucket_label_f(trade)}",
    ]
    live_fill_line = _build_live_fill_line(trade)
    if live_fill_line:
        lines.append(f"  {live_fill_line}")
    lines.append(f"  {_build_trade_reason(trade, comparison_lookup)}")
    context_line = _build_trade_context_line(trade, comparison_lookup)
    if context_line:
        lines.append(f"  {context_line}")
    return "\n".join(lines)


def _build_scan_bets_text(executed: list[dict],
                          comparison_rows: list[dict],
                          max_lines: int = SLACK_SCAN_BETS_MAX_LINES,
                          char_budget: int = SLACK_SCAN_BETS_CHAR_BUDGET) -> tuple[str, int]:
    entries = []
    used = 0
    comparison_lookup = _build_comparison_lookup(comparison_rows)

    for trade in _sort_executed_for_alert(executed)[:max_lines]:
        entry = _build_trade_alert_entry(trade, comparison_lookup)
        candidate = "\n\n".join(entries + [entry])
        if len(candidate) > char_budget:
            break
        entries.append(entry)
        used += 1

    omitted = max(len(executed) - used, 0)
    if omitted > 0:
        omission_line = f"...and {omitted} more bets in report"
        while entries:
            candidate = "\n\n".join(entries + [omission_line])
            if len(candidate) <= char_budget:
                break
            entries.pop()
            used -= 1
            omitted = len(executed) - used
            omission_line = f"...and {omitted} more bets in report"
        entries.append(omission_line)

    return "\n\n".join(entries), omitted


def _build_comparison_markdown_table(comparison_rows: list[dict]) -> list[str]:
    meaningful_rows = [row for row in comparison_rows if _has_meaningful_market_view(row)]
    if not meaningful_rows:
        return []
    lines = [
        "## Market View by City/Date",
        "",
        "| City | Date | Model High | Polymarket Implied | Kalshi Implied | Bets Placed |",
        "|---|---|---:|---:|---:|---|",
    ]
    for row in meaningful_rows:
        city = row.get("city", "").replace("_", " ").title()
        lines.append(
            f"| {city} | {row.get('target_date', '')} | "
            f"{_format_temp_value(row.get('model_expected_high'))} | "
            f"{_format_temp_value(row.get('polymarket_implied_high'))} | "
            f"{_format_temp_value(row.get('kalshi_implied_high'))} | "
            f"{_format_selected_bets(row.get('selected_bets') or [])} |"
        )
    lines.append("")
    return lines


def _build_daily_result_lines(details: list[dict],
                              max_lines: int = SLACK_DAILY_RESULTS_MAX_LINES,
                              char_budget: int = SLACK_DAILY_RESULTS_CHAR_BUDGET) -> tuple[str, int]:
    """Fit resolved-trade rows into one Slack section while leaving room for an omitted note."""
    lines = []
    used = 0

    for detail in details[:max_lines]:
        outcome = "WON" if detail["won"] else "LOST"
        actual_temp = f"{detail['actual_temp_f']:.0f}F"
        payout_display = math.floor(max(detail["size"] + detail["pnl"], 0))
        line = (
            f"• {_display_city(detail['city'])} | ${detail['size']:.0f} -> ${payout_display} | "
            f"{detail['side']} {detail['bucket'][:20]} | {outcome} | ${detail['pnl']:+.0f}\n"
            f"  Why: model {detail['ensemble_prob']:.0%} vs market {detail['market_price']:.0%}; "
            f"actual high was {actual_temp}."
        )
        candidate = "\n\n".join(lines + [line])
        if len(candidate) > char_budget:
            break
        lines.append(line)
        used += 1

    omitted = max(len(details) - used, 0)
    if omitted > 0:
        omission_line = f"...and {omitted} more results in report"
        while lines:
            candidate = "\n\n".join(lines + [omission_line])
            if len(candidate) <= char_budget:
                break
            lines.pop()
            used -= 1
            omitted = len(details) - used
            omission_line = f"...and {omitted} more results in report"
        lines.append(omission_line)

    return "\n\n".join(lines), omitted


def _build_daily_fallback_text(date: str, verdict: str, resolved: int, wins: int,
                               losses: int, daily_pnl: float, total_pnl: float,
                               all_time_wr: float, all_time_trades: int,
                               pending: int, report_path: str = "",
                               mode: str = "paper",
                               venue_counts: dict[str, int] | None = None) -> str:
    """Build a plain-text fallback when Slack rejects block payloads."""
    mode_label = mode.upper()
    lines = [
        f"*Daily Pulse ({mode_label}) - {date}*",
        verdict,
        f"Today: {resolved} resolved ({wins}W / {losses}L) | P&L: ${daily_pnl:+.2f}",
        f"All-time: {all_time_trades} trades | {all_time_wr:.0%} win rate | P&L: ${total_pnl:+.2f}",
    ]
    if venue_counts:
        counts_text = ", ".join(f"{_venue_label(venue)}: {count}" for venue, count in sorted(venue_counts.items()))
        lines.append(f"Resolved by venue: {counts_text}")
    if pending > 0:
        lines.append(f"{pending} trades still pending")
    if report_path:
        lines.append(f"Full report: `{report_path}`")
    return "\n".join(lines)


def _get_live_portfolio_value() -> Optional[float]:
    """Return authenticated Kalshi portfolio value (cash + open market value)."""
    try:
        from core.execution.kalshi_client import KalshiClient

        client = KalshiClient(
            api_key_id=KALSHI_API_KEY_ID,
            private_key_path=KALSHI_PRIVATE_KEY_PATH,
            use_demo=KALSHI_USE_DEMO,
        )
        balance = client.get_balance()
        portfolio = client.get_portfolio_exposure()
        return round(balance.balance_usd + portfolio.market_value_usd, 2)
    except Exception as exc:
        logger.warning("Failed to load live portfolio value for Slack recap: %s", exc)
        return None


def _build_live_daily_summary_text(date: str, resolved_details: list[dict]) -> str:
    """Build the compact morning live W/L recap."""
    daily_pnl = sum(detail.get("pnl", 0.0) for detail in resolved_details)
    win_buys = sum(1 for detail in resolved_details if detail.get("won") and detail.get("side") == "YES")
    win_sells = sum(1 for detail in resolved_details if detail.get("won") and detail.get("side") == "NO")
    loss_buys = sum(1 for detail in resolved_details if (not detail.get("won")) and detail.get("side") == "YES")
    loss_sells = sum(1 for detail in resolved_details if (not detail.get("won")) and detail.get("side") == "NO")
    wins = win_buys + win_sells
    losses = loss_buys + loss_sells
    portfolio_value = _get_live_portfolio_value()
    portfolio_text = (
        f"${portfolio_value:,.2f}" if portfolio_value is not None else "Unavailable"
    )

    return "\n".join(
        [
            f"Previous day P&L: ${daily_pnl:+.2f}",
            f"Wins: {wins} ({win_buys} buys, {win_sells} sells)",
            f"Losses: {losses} ({loss_buys} buys, {loss_sells} sells)",
            f"Portfolio value: {portfolio_text}",
        ]
    )


# ============================================================
# Detailed Markdown Report (saved to droplet filesystem)
# ============================================================

def _build_trade_narrative(sig: dict) -> str:
    """Build a full natural language explanation for one trade (for the markdown file)."""
    venue = sig.get("venue", "polymarket")
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

    payout = calc_payout(
        size,
        market_prob,
        side,
        entry_price=sig.get("entry_price"),
        fee_pct=sig.get("fee_pct"),
        venue=venue,
    )

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
            f"{_venue_label(venue)} prices this at {mkt_pct:.0f}%, but our models say {ens_pct:.0f}%. "
            f"The market is undervaluing this outcome by {edge_pct:.0f}%."
        )
    else:
        mispricing_line = (
            f"{_venue_label(venue)} prices this at {mkt_pct:.0f}%, but our models say only {ens_pct:.0f}%. "
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
        f"### {_venue_label(venue)} | {city} ({date}) | {bucket}\n\n"
        f"{nws_line} {ensemble_line}\n\n"
        f"{mispricing_line}\n\n"
        f"{trade_line}\n"
    )


def save_scan_report(executed: list[dict], total_signals: int,
                     bankroll, mode: str = "paper",
                     comparison_rows: Optional[list[dict]] = None) -> str:
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
    total_fees = sum(
        calc_fee_usd(
            s.get("trade_size", 0),
            s.get("market_prob", 0),
            venue=s.get("venue", "polymarket"),
            fee_pct=s.get("fee_pct"),
        )
        for s in executed
    )
    consensus_count = sum(1 for s in executed if not s.get("is_contrarian"))
    contrarian_count = sum(1 for s in executed if s.get("is_contrarian"))
    yes_count = sum(1 for s in executed if s.get("side") == "BUY")
    no_count = sum(1 for s in executed if s.get("side") == "SELL")
    venue_counts = {}
    for trade in executed:
        venue = trade.get("venue", "polymarket")
        venue_counts[venue] = venue_counts.get(venue, 0) + 1

    if isinstance(bankroll, dict):
        bankroll_text = ", ".join(
            f"{_venue_label(venue)} ${amount:.0f}"
            for venue, amount in sorted(bankroll.items())
        )
    else:
        bankroll_text = f"${bankroll:.0f}"

    by_venue_date = {}
    for s in executed:
        key = (s.get("venue", "polymarket"), s.get("target_date", "unknown"))
        by_venue_date.setdefault(key, []).append(s)

    lines = [
        f"# {mode_label} Scan Report \u2014 {now.strftime('%a %b %-d, %Y at %H:%M UTC')}\n",
        f"**{len(executed)} trades placed** out of {total_signals} signals found\n",
        f"| Metric | Value |",
        f"|---|---|",
        f"| Total Invested | ${total_invested:.0f} |",
        f"| Estimated Fees | ${total_fees:.2f} |",
        f"| Remaining Bankroll | {bankroll_text} |",
        f"| YES Trades | {yes_count} |",
        f"| NO Trades | {no_count} |",
        f"| With Crowd | {consensus_count} |",
        f"| Against Crowd | {contrarian_count} |",
        f"| Venues | {', '.join(f'{_venue_label(v)} {c}' for v, c in sorted(venue_counts.items()))} |",
        f"",
    ]

    lines.extend(_build_comparison_markdown_table(comparison_rows or []))

    for venue, target_date in sorted(by_venue_date.keys()):
        trades = by_venue_date[(venue, target_date)]
        lines.append(f"\n## {_venue_label(venue)} — {_friendly_date(target_date)} ({target_date})\n")
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
        lines.append("| Venue | City | Date | Bucket | Bet | Stake | Actual Temp | Result | P&L |")
        lines.append("|---|---|---|---|---|---|---|---|---|")
        for d in details:
            result_emoji = "WIN" if d["won"] else "LOSS"
            lines.append(
                f"| {_venue_label(d.get('venue', 'polymarket'))} | {d['city']} | {d['target_date']} | "
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

            lines.append(f"### {_venue_label(d.get('venue', 'polymarket'))} | {d['city']} \u2014 {d['bucket'][:50]}\n")
            lines.append(
                f"The actual high was **{d['actual_temp_f']:.1f}\u00b0F**. "
                f"Did it land in the bucket? **{in_bucket}.**\n"
            )
            lines.append(
                f"We bet **{d['side']}** at ${d['size']:.0f}. "
                f"{_venue_label(d.get('venue', 'polymarket'))} had it at {mkt:.0f}%, our model said {ens:.0f}%. "
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
                       bankroll, mode: str = "paper",
                       comparison_rows: Optional[list[dict]] = None):
    """
    Send a COMPACT Slack summary (5-8 lines max).
    Full details are in the markdown report on the droplet.
    """
    if not executed:
        return

    # Save the detailed report first
    report_path = save_scan_report(executed, total_signals, bankroll, mode, comparison_rows=comparison_rows)
    executed = _filter_slack_visible_items(executed)
    if not executed:
        logger.info("Skipping Slack scan alert because no visible-venue trades were executed")
        return

    mode_label = "PAPER" if mode == "paper" else "LIVE"
    total_invested = sum(s.get("trade_size", 0) for s in executed)
    total_payout = sum(
        calc_payout(
            s.get("trade_size", 0),
            s.get("market_prob", 0),
            s.get("side", "SELL"),
            entry_price=s.get("entry_price"),
            fee_pct=s.get("fee_pct"),
            venue=s.get("venue", "polymarket"),
        ).get("payout", 0)
        for s in executed
    )
    yes_count = sum(1 for s in executed if s.get("side") == "BUY")
    no_count = sum(1 for s in executed if s.get("side") == "SELL")
    avg_edge = sum(abs(s.get("edge", 0)) for s in executed) / len(executed) * 100

    # Group by date for a quick summary
    dates = sorted(set(s.get("target_date", "") for s in executed))
    cities = sorted(set(_display_city(s.get("city", "")) for s in executed))
    bets_text, omitted = _build_scan_bets_text(executed, comparison_rows or [])

    # Compact Slack message
    header_text = f"{mode_label} Scan \u2014 {len(executed)} {_venue_label(PRIMARY_VISIBLE_VENUE)} trades placed"

    summary_text = (
        f"*{len(executed)} trades* | "
        f"{yes_count} YES / {no_count} NO | "
        f"Avg edge {avg_edge:.0f}%\n"
        f"${total_invested:.0f} invested | ${math.floor(total_payout):.0f} potential payout\n"
        f"Markets: {', '.join(_friendly_date(d) for d in dates)}\n"
        f"Cities: {', '.join(cities[:6])}"
        f"{'...' if len(cities) > 6 else ''}"
    )

    bets_header = "*Bets placed*"
    if omitted > 0:
        bets_header += f" (top {len(executed) - omitted} shown)"

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
            "text": {"type": "mrkdwn", "text": f"{bets_header}\n{bets_text}"}
        },
    ]

    if SLACK_INCLUDE_REPORT_LINKS:
        blocks.append({
            "type": "context",
            "elements": [{
                "type": "mrkdwn",
                "text": f":page_facing_up: Full analysis: `{report_path}`"
            }]
        })

    send_slack_blocks(blocks, text=f"{mode_label}: {len(executed)} trades, ${total_invested:.0f} invested")


def alert_daily_summary(stats: dict):
    """
    Send a compact daily resolution pulse to Slack.
    Full details saved to markdown report.
    """
    date = stats.get("date", "Today")
    mode = stats.get("mode", "paper")
    mode_label = mode.upper()
    details = stats.get("details", [])
    total_pnl = stats.get("total_pnl", 0)
    all_time_wr = stats.get("all_time_win_rate", 0)
    all_time_trades = stats.get("all_time_trades", 0)
    pending = stats.get("pending_trades", 0)
    resolved_details = _filter_slack_visible_items(details)
    if not resolved_details:
        if details:
            save_resolution_report(details, stats)
        return

    if mode == "live" and PRIMARY_VISIBLE_VENUE == "kalshi":
        if details:
            save_resolution_report(details, stats)
        live_summary_text = _build_live_daily_summary_text(date, resolved_details)
        header_text = f"Daily Live W/L — {_friendly_date(date)}"
        blocks = [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": header_text}
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": live_summary_text}
            },
        ]
        preview_text = (
            f"Daily live W/L ({date}): "
            f"{live_summary_text.replace(chr(10), ' | ')}"
        )
        if send_slack_blocks(blocks, text=preview_text):
            return
        send_slack_message(f"*{header_text}*\n{live_summary_text}")
        return

    resolved = len(resolved_details)
    daily_pnl = sum(detail.get("pnl", 0.0) for detail in resolved_details)
    wins = sum(1 for detail in resolved_details if detail.get("won"))
    losses = sum(1 for detail in resolved_details if not detail.get("won"))
    venue_counts = {}
    for detail in resolved_details:
        venue = detail.get("venue", "polymarket")
        venue_counts[venue] = venue_counts.get(venue, 0) + 1

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

    results_text, omitted = _build_daily_result_lines(resolved_details)

    summary_text = (
        f"*{resolved} resolved* | {wins}W / {losses}L | P&L: *${daily_pnl:+.0f}*\n"
        f"*All-time {_venue_label(PRIMARY_VISIBLE_VENUE)}:* {all_time_trades} trades | "
        f"{all_time_wr:.0%} win rate | P&L: *${total_pnl:+.0f}*"
    )

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"Daily Pulse ({mode_label}) \u2014 {date}"}
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": summary_text}
        },
    ]

    if results_text:
        blocks.append({"type": "divider"})
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": results_text}
        })

    footer_parts = []
    if pending > 0:
        footer_parts.append(f":hourglass_flowing_sand: {pending} trades still pending")
    if report_path and SLACK_INCLUDE_REPORT_LINKS:
        footer_parts.append(f":page_facing_up: Full report: `{report_path}`")
    if omitted > 0 and not report_path:
        footer_parts.append(f"{omitted} more results omitted")

    if footer_parts:
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": " | ".join(footer_parts)}]
        })

    preview_text = f"Daily pulse ({mode_label.lower()}): ${daily_pnl:+.2f} | {verdict}"
    if send_slack_blocks(blocks, text=preview_text):
        return

    logger.warning("Daily summary blocks failed, sending plain-text fallback")
    fallback_text = _build_daily_fallback_text(
        date=date,
        verdict=verdict,
        resolved=resolved,
        wins=wins,
        losses=losses,
        daily_pnl=daily_pnl,
        total_pnl=total_pnl,
        all_time_wr=all_time_wr,
        all_time_trades=all_time_trades,
        pending=pending,
        report_path=report_path if SLACK_INCLUDE_REPORT_LINKS else "",
        mode=mode,
        venue_counts=venue_counts,
    )
    if not send_slack_message(fallback_text):
        logger.warning("Daily summary fallback message also failed")


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
        f"US cities only | venue-specific forecast window | "
        f"Min 8% edge after fees"
    )
    send_slack_message(text)
