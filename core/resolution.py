"""
Trade Resolution Tracker.

Checks unresolved trades, fetches actual observed temperatures,
determines win/loss, calculates P&L, and updates the database.
"""

import logging
import re
import requests
from datetime import datetime, date, timedelta, timezone

from config.settings import CITIES
from core.database import get_unresolved_trades, resolve_trade, get_trade_stats
from core.alerts import alert_daily_summary, alert_error

logger = logging.getLogger(__name__)


def get_actual_high_temp(lat: float, lon: float, target_date: str) -> float | None:
    """
    Fetch the actual observed high temperature from Open-Meteo.
    Returns temperature in Fahrenheit, or None if not available yet.
    """
    # Primary: forecast API with past_days (best for recent dates)
    try:
        params = {
            "latitude": lat, "longitude": lon,
            "daily": "temperature_2m_max",
            "temperature_unit": "fahrenheit",
            "timezone": "auto",
            "past_days": 7, "forecast_days": 1,
        }
        resp = requests.get("https://api.open-meteo.com/v1/forecast",
                            params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        dates = data.get("daily", {}).get("time", [])
        temps = data.get("daily", {}).get("temperature_2m_max", [])
        for i, d in enumerate(dates):
            if d == target_date and i < len(temps) and temps[i] is not None:
                return temps[i]
    except Exception as e:
        logger.debug(f"Forecast API failed for {target_date}: {e}")

    # Fallback: archive API
    try:
        params = {
            "latitude": lat, "longitude": lon,
            "start_date": target_date, "end_date": target_date,
            "daily": "temperature_2m_max",
            "temperature_unit": "fahrenheit",
            "timezone": "auto",
        }
        resp = requests.get("https://archive-api.open-meteo.com/v1/archive",
                            params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        temps = data.get("daily", {}).get("temperature_2m_max", [])
        if temps and temps[0] is not None:
            return temps[0]
    except Exception as e:
        logger.debug(f"Archive API failed for {target_date}: {e}")

    return None


def check_bucket_hit(actual_temp_f: float, bucket_question: str,
                     city_key: str) -> bool | None:
    """
    Determine if the actual temperature fell in the trade's bucket.
    Parses the bucket bounds from the stored question text.
    Returns True if temp is in bucket, False if not, None if can't parse.
    """
    q = bucket_question
    q_lower = q.lower()
    is_f = "°c" not in q_lower and " c" not in q_lower

    actual = actual_temp_f
    if not is_f:
        # Convert actual from F to C for comparison
        actual = (actual_temp_f - 32) * 5 / 9

    # Pattern 1: "between X° and Y°" or "between X and Y"
    m = re.search(r'between\s+(\d+)\s*°?\s*(?:F|C|f|c)?\s*and\s+(\d+)', q)
    if m:
        low, high = float(m.group(1)), float(m.group(2))
        return low <= actual < high

    # Pattern 2: "be X-Y°F" or "be X - Y°F" (hyphenated range)
    m = re.search(r'be\s+(\d+)\s*-\s*(\d+)\s*°', q)
    if m:
        low, high = float(m.group(1)), float(m.group(2))
        return low <= actual < high

    # Pattern 3: Standalone hyphenated range "X-Y°F" anywhere in text
    m = re.search(r'(\d+)\s*-\s*(\d+)\s*°', q)
    if m:
        low, high = float(m.group(1)), float(m.group(2))
        return low <= actual < high

    # Pattern 3b: Range using "to", e.g. "70° to 71°"
    m = re.search(r'(\d+)\s*°?\s*(?:F|C|f|c)?\s*to\s*(\d+)', q)
    if m:
        low, high = float(m.group(1)), float(m.group(2))
        return low <= actual < high

    # Pattern 4: "X° or below" / "X° or lower"
    m = re.search(r'(\d+)\s*°?\s*(?:F|C|f|c)?\s*or\s+(?:below|lower)', q_lower)
    if m:
        high = float(m.group(1))
        return actual <= high

    # Pattern 5: "X° or higher" / "X° or above"
    m = re.search(r'(\d+)\s*°?\s*(?:F|C|f|c)?\s*or\s+(?:higher|above)', q_lower)
    if m:
        low = float(m.group(1))
        return actual >= low

    # Pattern 6: "be X°" (exact single degree bucket)
    m = re.search(r'be\s+(\d+)\s*°', q)
    if m:
        target = float(m.group(1))
        return target <= actual < target + 1

    logger.warning(f"Could not parse bucket from: {bucket_question}")
    return None


def resolve_pending_trades(mode: str = "paper") -> dict:
    """
    Main resolution function. Checks all unresolved trades and resolves
    any whose target_date has passed.

    Returns summary stats including per-trade details for Slack.
    """
    trades = get_unresolved_trades(mode=mode)
    if not trades:
        logger.info("No unresolved trades to check")
        return {"checked": 0, "resolved": 0, "wins": 0, "losses": 0,
                "pnl": 0, "details": []}

    today = date.today()
    resolved_count = 0
    wins = 0
    losses = 0
    total_pnl = 0.0
    skipped = 0
    details = []  # Per-trade details for Slack recap

    for trade in trades:
        target_date = trade.get("target_date", "")
        if not target_date:
            continue

        # Only resolve trades whose date has passed
        try:
            trade_date = datetime.strptime(target_date, "%Y-%m-%d").date()
        except ValueError:
            continue

        if trade_date >= today:
            skipped += 1
            continue  # Not yet resolvable

        city_key = trade.get("city", "")
        city_config = CITIES.get(city_key)
        if not city_config:
            logger.warning(f"Unknown city {city_key} for trade {trade['id']}")
            continue

        # Fetch actual temperature
        actual_f = get_actual_high_temp(
            city_config["lat"], city_config["lon"], target_date
        )

        if actual_f is None:
            logger.info(
                f"No actual temp yet for {city_key} on {target_date}, "
                f"will retry later"
            )
            continue

        # Check if the bucket was hit
        in_bucket = check_bucket_hit(actual_f, trade["bucket_question"], city_key)
        if in_bucket is None:
            logger.warning(
                f"Could not parse bucket for trade {trade['id']}: "
                f"{trade['bucket_question']}"
            )
            continue

        # Determine win/loss
        side = trade["side"]
        market_price = trade["price"]
        entry_price = trade.get("entry_price")
        size = trade["size_usd"]

        if side == "BUY":
            won = in_bucket  # YES bet wins if bucket was hit
        else:
            won = not in_bucket  # NO bet wins if bucket was NOT hit

        # Calculate P&L (prediction market math)
        if won:
            if side == "BUY":
                share_price = entry_price if entry_price is not None else market_price
                shares = size / max(share_price, 0.03)
                pnl = shares - size  # shares * $1 - cost
            else:
                share_price = entry_price if entry_price is not None else max(1.0 - market_price, 0.03)
                shares = size / max(share_price, 0.03)
                pnl = shares - size
            pnl = min(pnl, size * 19)  # Cap at 20x
            outcome = "win"
            wins += 1
        else:
            pnl = -size
            outcome = "loss"
            losses += 1

        total_pnl += pnl

        # Update the database — NOW with actual_temp
        resolve_trade(
            trade_id=trade["id"],
            outcome=outcome,
            pnl=pnl,
            resolution_price=1.0 if in_bucket else 0.0,
            actual_temp=actual_f,
        )

        resolved_count += 1

        # Collect details for Slack recap
        city_name = city_config.get("name", city_key.replace("_", " ").title())
        details.append({
            "venue": trade.get("venue", "polymarket"),
            "city": city_name,
            "target_date": target_date,
            "bucket": trade["bucket_question"],
            "side": "YES" if side == "BUY" else "NO",
            "size": size,
            "market_price": market_price,
            "ensemble_prob": trade.get("ensemble_prob", 0),
            "actual_temp_f": actual_f,
            "in_bucket": in_bucket,
            "won": won,
            "pnl": pnl,
        })

        logger.info(
            f"Resolved trade {trade['id']}: {side} {city_key} {target_date} "
            f"-> actual={actual_f:.1f}°F, in_bucket={in_bucket}, "
            f"{outcome.upper()}, P&L=${pnl:+.2f}"
        )

    summary = {
        "checked": len(trades),
        "skipped_future": skipped,
        "resolved": resolved_count,
        "wins": wins,
        "losses": losses,
        "pnl": total_pnl,
        "details": details,
    }

    logger.info(
        f"Resolution complete: {resolved_count} resolved "
        f"({wins}W/{losses}L, ${total_pnl:+.2f}), "
        f"{skipped} still pending"
    )

    return summary


def run_daily_recap(mode: str = "paper"):
    """
    Run resolution and send a daily Slack recap.
    Called once per day after markets close.
    """
    logger.info("Running daily resolution and recap...")

    # Resolve any pending trades
    resolution = resolve_pending_trades(mode=mode)

    # Get overall stats
    stats = get_trade_stats(mode=mode)

    yesterday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    alert_daily_summary({
        "date": yesterday,
        "trades_resolved": resolution["resolved"],
        "daily_pnl": resolution["pnl"],
        "wins": resolution["wins"],
        "losses": resolution["losses"],
        "details": resolution["details"],
        "total_pnl": stats.get("total_pnl", 0),
        "all_time_win_rate": stats.get("win_rate", 0),
        "all_time_trades": stats.get("total_trades", 0),
        "all_time_wins": stats.get("wins", 0),
        "all_time_losses": stats.get("losses", 0),
        "pending_trades": stats.get("pending_trades", 0),
    })

    return resolution
