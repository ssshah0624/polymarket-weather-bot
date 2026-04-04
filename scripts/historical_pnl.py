#!/usr/bin/env python3
"""
Historical P&L Analysis — "What if we had been running the bot?"

For each of the past N days:
1. Fetch resolved Polymarket weather markets for that date
2. Get GFS ensemble forecast using past_days parameter (archived forecasts)
3. Calculate what signals the bot would have generated
4. Fetch ACTUAL observed high temperature
5. Score each bet as win/loss and calculate P&L
"""

import sys
import json
import requests
import logging
from pathlib import Path
from datetime import datetime, date, timedelta

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import (
    GAMMA_API_BASE, CITIES, EDGE_THRESHOLD, KELLY_FRACTION,
    MAX_TRADE_SIZE, ENSEMBLE_API_BASE,
)
from core.data.polymarket import (
    parse_market_buckets, extract_event_city, extract_event_date,
)
from core.data.ensemble import calc_bucket_probability
from core.strategy.edge import analyze_event_buckets, rank_opportunities
from core.strategy.kelly import calculate_trade_size

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

HEADERS = {"User-Agent": "PolymarketWeatherBot/1.0"}
MAX_POSITIONS_PER_DAY = 20
MAX_PER_EVENT = 3


def fetch_ensemble_with_past_days(lat, lon, past_days=5):
    """Fetch ensemble data including past days of archived forecasts."""
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m",
        "models": "gfs_seamless",
        "temperature_unit": "fahrenheit",
        "past_days": past_days,
        "forecast_days": 1,
    }
    try:
        resp = requests.get(ENSEMBLE_API_BASE, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.warning(f"Ensemble fetch failed: {e}")
        return None


def extract_ensemble_maxes_for_date(data, target_date):
    """Extract daily max temps per ensemble member for a specific date.
    Uses ALL hours for the date (times are UTC, daily max covers full day)."""
    hourly = data.get("hourly", {})
    times = hourly.get("time", [])
    members = {k: v for k, v in hourly.items() if k.startswith("temperature_2m_member")}

    if not members or not times:
        return None

    # Use all hours for target date (API returns UTC times)
    indices = [idx for idx, t in enumerate(times) if target_date in t]

    if not indices:
        return None

    maxes = []
    for key, temps in members.items():
        valid = [temps[i] for i in indices if i < len(temps) and temps[i] is not None]
        if valid:
            maxes.append(max(valid))

    return maxes if maxes else None


def get_actual_high_temp(lat, lon, target_date):
    """Fetch actual observed high temperature in Fahrenheit."""
    # Try forecast API with past_days (works for recent dates)
    try:
        params = {
            "latitude": lat,
            "longitude": lon,
            "daily": "temperature_2m_max",
            "temperature_unit": "fahrenheit",
            "timezone": "auto",
            "past_days": 7,
            "forecast_days": 1,
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
    except Exception:
        pass

    # Fallback: archive API
    try:
        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": target_date,
            "end_date": target_date,
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
    except Exception:
        pass

    return None


def fetch_events_for_date(target_date):
    """Fetch weather events matching a target date from Polymarket."""
    all_events = []
    offset = 0
    while True:
        params = {"tag_slug": "weather", "limit": 100, "offset": offset}
        try:
            resp = requests.get(f"{GAMMA_API_BASE}/events", params=params,
                                headers=HEADERS, timeout=30)
            resp.raise_for_status()
            events = resp.json()
        except Exception:
            break
        if not events:
            break
        all_events.extend(events)
        offset += len(events)
        if len(events) < 100:
            break

    dt = datetime.strptime(target_date, "%Y-%m-%d")
    patterns = [dt.strftime("%B %-d"), dt.strftime("%B %d"), target_date]

    matched = []
    for event in all_events:
        title = event.get("title", "")
        if "highest temperature" in title.lower():
            for pat in patterns:
                if pat.lower() in title.lower():
                    matched.append(event)
                    break
    return matched


def score_bet(side, bucket_low, bucket_high, is_fahrenheit, actual_temp_f):
    """Determine if a bet won or lost."""
    if not is_fahrenheit:
        low_f = bucket_low * 9 / 5 + 32 if bucket_low != -999 else -999
        high_f = bucket_high * 9 / 5 + 32 if bucket_high != 999 else 999
    else:
        low_f = bucket_low
        high_f = bucket_high

    in_bucket = low_f <= actual_temp_f < high_f
    won = in_bucket if side == "BUY" else not in_bucket
    return {"in_bucket": in_bucket, "won": won}


def analyze_day(target_date, bankroll=1000.0):
    """Simulate what the bot would have done on a given day."""
    print(f"\n{'='*70}")
    print(f"  Analyzing: {target_date}")
    print(f"{'='*70}")

    events = fetch_events_for_date(target_date)
    if not events:
        print(f"  No temperature events found for {target_date}")
        return {"date": target_date, "trades": [], "pnl": 0, "wins": 0, "losses": 0}

    print(f"  Found {len(events)} temperature events")

    all_signals = []
    ensemble_cache = {}

    for event in events:
        title = event.get("title", "")
        city_key = extract_event_city(event)
        if not city_key or city_key not in CITIES:
            continue

        city_config = CITIES[city_key]
        lat = city_config["lat"]
        lon = city_config["lon"]

        buckets = parse_market_buckets(event)
        if not buckets:
            continue

        # Get ensemble data (with past_days to cover recent dates)
        cache_key = city_key
        if cache_key not in ensemble_cache:
            data = fetch_ensemble_with_past_days(lat, lon, past_days=5)
            if data:
                member_temps = extract_ensemble_maxes_for_date(data, target_date)
                ensemble_cache[cache_key] = member_temps
            else:
                ensemble_cache[cache_key] = None

        member_temps = ensemble_cache[cache_key]
        if not member_temps:
            continue

        # Calculate ensemble probabilities
        enriched = []
        for bucket in buckets:
            low = bucket.get("temp_low")
            high = bucket.get("temp_high")
            if low is None or high is None:
                continue

            if not bucket.get("is_fahrenheit"):
                low_f = low * 9 / 5 + 32 if low != -999 else -999
                high_f = high * 9 / 5 + 32 if high != 999 else 999
                prob = calc_bucket_probability(member_temps, low_f, high_f)
            else:
                prob = calc_bucket_probability(member_temps, low, high)

            enriched.append({
                **bucket,
                "ensemble_prob": prob,
                "ensemble_meta": {
                    "member_count": len(member_temps),
                    "mean": sum(member_temps) / len(member_temps),
                    "min": min(member_temps),
                    "max": max(member_temps),
                },
            })

        analyzed = analyze_event_buckets(enriched)
        opportunities = rank_opportunities(analyzed)

        for opp in opportunities:
            opp["city_key"] = city_key
            opp["event_title"] = title
            opp["lat"] = lat
            opp["lon"] = lon
            all_signals.append(opp)

    all_signals.sort(key=lambda s: abs(s.get("edge", 0)), reverse=True)

    # Simulate position sizing
    trades = []
    remaining = bankroll
    event_counts = {}

    for sig in all_signals:
        if len(trades) >= MAX_POSITIONS_PER_DAY:
            break

        edge = sig["edge"]
        ens_prob = sig["ensemble_prob"]
        market_price = sig.get("market_prob", 0)

        # Skip markets at extreme prices (0% or 100%) — no real liquidity
        if market_price <= 0.02 or market_price >= 0.98:
            continue

        if edge > 0:
            sizing = calculate_trade_size(edge, ens_prob, remaining, 0)
        else:
            sizing = calculate_trade_size(abs(edge), 1 - ens_prob, remaining, 0)

        size = sizing.get("size", 0)
        if size <= 0 or size > remaining:
            continue

        evt = sig.get("event_title", "")
        event_counts[evt] = event_counts.get(evt, 0)
        if event_counts[evt] >= MAX_PER_EVENT:
            continue
        event_counts[evt] += 1

        remaining -= size
        trades.append({**sig, "trade_size": size})

    if not trades:
        print(f"  No tradeable signals found")
        return {"date": target_date, "trades": [], "pnl": 0, "wins": 0, "losses": 0}

    print(f"  Generated {len(trades)} trades (from {len(all_signals)} signals)")

    # Fetch actual temps and score
    actual_temps = {}
    total_pnl = 0
    wins = 0
    losses = 0

    for trade in trades:
        city_key = trade["city_key"]
        lat = trade["lat"]
        lon = trade["lon"]

        if city_key not in actual_temps:
            actual_temps[city_key] = get_actual_high_temp(lat, lon, target_date)

        actual_f = actual_temps[city_key]
        if actual_f is None:
            trade["actual_temp"] = None
            trade["outcome"] = "N/A"
            trade["pnl"] = 0
            continue

        trade["actual_temp"] = actual_f
        side = "BUY" if trade["edge"] > 0 else "SELL"
        result = score_bet(
            side=side,
            bucket_low=trade.get("temp_low", 0),
            bucket_high=trade.get("temp_high", 0),
            is_fahrenheit=trade.get("is_fahrenheit", True),
            actual_temp_f=actual_f,
        )

        market_price = trade.get("market_prob", 0)
        size = trade["trade_size"]

        # P&L calculation:
        # BUY at price p: pay $size, get $size/p shares. Win = $size/p - $size. Loss = -$size.
        # SELL at price p: sell $size worth of NO shares at (1-p). Win = $size*(p/(1-p)). Loss = -$size.
        # Clamp to avoid extreme payouts from near-0 or near-1 prices.
        if result["won"]:
            if side == "BUY":
                pnl = size * (1.0 / max(market_price, 0.05) - 1.0)
            else:
                pnl = size * (market_price / max(1.0 - market_price, 0.05))
            pnl = min(pnl, size * 19)  # Cap at 20x (buying at 5%)
            trade["outcome"] = "WIN"
            trade["pnl"] = pnl
            wins += 1
        else:
            trade["outcome"] = "LOSS"
            trade["pnl"] = -size
            losses += 1

        total_pnl += trade["pnl"]

    # Print results table
    print(f"\n  {'City':<15} {'Bucket':<18} {'Side':<5} {'Size':>7} {'Mkt':>6} {'Ens':>6} {'Edge':>7} {'Actual':>8} {'Result':>6} {'P&L':>9}")
    print(f"  {'-'*15} {'-'*18} {'-'*5} {'-'*7} {'-'*6} {'-'*6} {'-'*7} {'-'*8} {'-'*6} {'-'*9}")

    for t in trades:
        city = t.get("city_key", "?")[:15]
        low = t.get("temp_low", 0)
        high = t.get("temp_high", 0)
        is_f = t.get("is_fahrenheit", True)

        # Always show in Fahrenheit
        if not is_f:
            disp_low = low * 9/5 + 32 if low != -999 else -999
            disp_high = high * 9/5 + 32 if high != 999 else 999
        else:
            disp_low, disp_high = low, high

        if disp_low == -999:
            bucket = f"<={disp_high:.0f}°F"
        elif disp_high == 999:
            bucket = f">={disp_low:.0f}°F"
        else:
            bucket = f"{disp_low:.0f}-{disp_high:.0f}°F"

        side = "BUY" if t["edge"] > 0 else "SELL"
        actual = f"{t['actual_temp']:.1f}°F" if t.get("actual_temp") else "N/A"
        outcome = t.get("outcome", "?")
        pnl = t.get("pnl", 0)

        print(f"  {city:<15} {bucket:<18} {side:<5} ${t['trade_size']:>6.2f} {t['market_prob']:>5.0%} {t['ensemble_prob']:>5.0%} {t['edge']*100:>+6.1f}% {actual:>8} {outcome:>6} ${pnl:>+8.2f}")

    print(f"\n  Day Summary: {wins}W / {losses}L | P&L: ${total_pnl:+.2f}")

    return {
        "date": target_date,
        "trades": trades,
        "wins": wins,
        "losses": losses,
        "pnl": total_pnl,
        "actual_temps": actual_temps,
    }


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Historical P&L analysis")
    parser.add_argument("--days", type=int, default=3, help="Number of past days")
    parser.add_argument("--bankroll", type=float, default=1000.0, help="Bankroll per day")
    args = parser.parse_args()

    today = date.today()
    total_pnl = 0
    total_wins = 0
    total_losses = 0
    total_trades = 0
    daily_results = []

    for i in range(args.days, 0, -1):
        d = today - timedelta(days=i)
        result = analyze_day(d.strftime("%Y-%m-%d"), bankroll=args.bankroll)
        daily_results.append(result)
        total_pnl += result.get("pnl", 0)
        total_wins += result.get("wins", 0)
        total_losses += result.get("losses", 0)
        total_trades += len(result.get("trades", []))

    # Grand summary
    print(f"\n{'='*70}")
    print(f"  GRAND TOTAL — Past {args.days} Days")
    print(f"{'='*70}")
    print(f"  Total Trades:  {total_trades}")
    print(f"  Wins:          {total_wins}")
    print(f"  Losses:        {total_losses}")
    if total_trades > 0:
        print(f"  Win Rate:      {total_wins / total_trades:.1%}")
    print(f"  Total P&L:     ${total_pnl:+.2f}")
    print(f"  Capital Used:  ${args.bankroll:.2f}/day x {args.days} days = ${args.bankroll * args.days:.2f}")
    if total_trades > 0:
        print(f"  ROI:           {total_pnl / (args.bankroll * args.days):.1%}")

    print(f"\n  Per-Day Breakdown:")
    print(f"  {'Date':<12} {'Trades':>7} {'W/L':>7} {'P&L':>11}")
    print(f"  {'-'*12} {'-'*7} {'-'*7} {'-'*11}")
    for r in daily_results:
        w = r.get("wins", 0)
        l = r.get("losses", 0)
        n = len(r.get("trades", []))
        print(f"  {r['date']:<12} {n:>7} {f'{w}W/{l}L':>7} ${r.get('pnl', 0):>+10.2f}")


if __name__ == "__main__":
    main()
