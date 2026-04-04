#!/usr/bin/env python3
"""
Quick Historical P&L — streamlined version.
Analyzes the past 2-3 days against actual temperatures.
"""

import sys
import requests
from pathlib import Path
from datetime import datetime, date, timedelta

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import GAMMA_API_BASE, CITIES, ENSEMBLE_API_BASE
from core.data.polymarket import parse_market_buckets, extract_event_city
from core.data.ensemble import calc_bucket_probability
from core.strategy.edge import analyze_event_buckets, rank_opportunities
from core.strategy.kelly import calculate_trade_size

HEADERS = {"User-Agent": "PolymarketWeatherBot/1.0"}


def fetch_all_weather_events():
    """Fetch all weather events from Polymarket (one-time)."""
    all_events = []
    offset = 0
    while True:
        params = {"tag_slug": "weather", "limit": 100, "offset": offset}
        resp = requests.get(f"{GAMMA_API_BASE}/events", params=params,
                            headers=HEADERS, timeout=30)
        events = resp.json()
        if not events:
            break
        all_events.extend(events)
        offset += len(events)
        if len(events) < 100:
            break
    return all_events


def match_events_for_date(all_events, target_date):
    """Filter events matching a specific date."""
    dt = datetime.strptime(target_date, "%Y-%m-%d")
    patterns = [dt.strftime("%B %-d").lower(), target_date]
    matched = []
    for e in all_events:
        title = e.get("title", "").lower()
        if "highest temperature" in title:
            for p in patterns:
                if p in title:
                    matched.append(e)
                    break
    return matched


def get_ensemble_maxes(lat, lon, target_date):
    """Fetch ensemble member daily maxes for a past date."""
    params = {
        "latitude": lat, "longitude": lon,
        "hourly": "temperature_2m",
        "models": "gfs_seamless",
        "temperature_unit": "fahrenheit",
        "past_days": 5, "forecast_days": 1,
    }
    resp = requests.get(ENSEMBLE_API_BASE, params=params, timeout=30)
    data = resp.json()
    times = data.get("hourly", {}).get("time", [])
    members = {k: v for k, v in data.get("hourly", {}).items()
               if k.startswith("temperature_2m_member")}

    indices = [i for i, t in enumerate(times) if target_date in t]
    if not indices:
        return None

    maxes = []
    for k, v in members.items():
        vals = [v[i] for i in indices if i < len(v) and v[i] is not None]
        if vals:
            maxes.append(max(vals))
    return maxes if maxes else None


def get_actual_temp(lat, lon, target_date):
    """Get actual observed high temperature."""
    params = {
        "latitude": lat, "longitude": lon,
        "daily": "temperature_2m_max",
        "temperature_unit": "fahrenheit",
        "timezone": "auto",
        "past_days": 7, "forecast_days": 1,
    }
    resp = requests.get("https://api.open-meteo.com/v1/forecast",
                        params=params, timeout=15)
    data = resp.json()
    dates = data.get("daily", {}).get("time", [])
    temps = data.get("daily", {}).get("temperature_2m_max", [])
    for i, d in enumerate(dates):
        if d == target_date and i < len(temps):
            return temps[i]
    return None


def analyze_day(all_events, target_date, bankroll=1000.0):
    """Full analysis for one day."""
    print(f"\n{'='*70}")
    print(f"  {target_date}")
    print(f"{'='*70}")

    events = match_events_for_date(all_events, target_date)
    if not events:
        print("  No events found")
        return {"date": target_date, "trades": [], "wins": 0, "losses": 0, "pnl": 0}

    print(f"  Found {len(events)} temperature events")

    # Build signals
    all_signals = []
    ens_cache = {}

    for event in events:
        city_key = extract_event_city(event)
        if not city_key or city_key not in CITIES:
            continue

        city = CITIES[city_key]
        buckets = parse_market_buckets(event)
        if not buckets:
            continue

        if city_key not in ens_cache:
            try:
                ens_cache[city_key] = get_ensemble_maxes(
                    city["lat"], city["lon"], target_date)
            except Exception:
                ens_cache[city_key] = None

        member_temps = ens_cache[city_key]
        if not member_temps:
            continue

        enriched = []
        for b in buckets:
            low, high = b.get("temp_low"), b.get("temp_high")
            if low is None or high is None:
                continue
            if not b.get("is_fahrenheit"):
                lf = low * 9/5 + 32 if low != -999 else -999
                hf = high * 9/5 + 32 if high != 999 else 999
                prob = calc_bucket_probability(member_temps, lf, hf)
            else:
                prob = calc_bucket_probability(member_temps, low, high)
            enriched.append({
                **b,
                "ensemble_prob": prob,
                "ensemble_meta": {
                    "member_count": len(member_temps),
                    "mean": sum(member_temps) / len(member_temps),
                    "min": min(member_temps),
                    "max": max(member_temps),
                },
            })

        analyzed = analyze_event_buckets(enriched)
        opps = rank_opportunities(analyzed)
        for o in opps:
            o["city_key"] = city_key
            o["event_title"] = event.get("title", "")
            o["lat"] = city["lat"]
            o["lon"] = city["lon"]
            all_signals.append(o)

    all_signals.sort(key=lambda s: abs(s.get("edge", 0)), reverse=True)

    # Position sizing with filters
    trades = []
    remaining = bankroll
    evt_counts = {}

    for sig in all_signals:
        if len(trades) >= 20:
            break
        mp = sig.get("market_prob", 0)
        if mp <= 0.02 or mp >= 0.98:
            continue
        edge = sig["edge"]
        ep = sig["ensemble_prob"]
        if edge > 0:
            sizing = calculate_trade_size(edge, ep, remaining, 0)
        else:
            sizing = calculate_trade_size(abs(edge), 1 - ep, remaining, 0)
        size = sizing.get("size", 0)
        if size <= 0 or size > remaining:
            continue
        evt = sig.get("event_title", "")
        evt_counts[evt] = evt_counts.get(evt, 0)
        if evt_counts[evt] >= 3:
            continue
        evt_counts[evt] += 1
        remaining -= size
        trades.append({**sig, "trade_size": size})

    if not trades:
        print("  No tradeable signals (after filtering 0%/100% markets)")
        return {"date": target_date, "trades": [], "wins": 0, "losses": 0, "pnl": 0}

    print(f"  {len(trades)} trades placed (from {len(all_signals)} signals)")

    # Score against actuals
    actual_cache = {}
    wins, losses, total_pnl = 0, 0, 0.0

    for t in trades:
        ck = t["city_key"]
        if ck not in actual_cache:
            try:
                actual_cache[ck] = get_actual_temp(t["lat"], t["lon"], target_date)
            except Exception:
                actual_cache[ck] = None

        actual = actual_cache.get(ck)
        if actual is None:
            t["outcome"] = "N/A"
            t["pnl"] = 0.0
            t["actual_temp"] = None
            continue

        t["actual_temp"] = actual
        side = "BUY" if t["edge"] > 0 else "SELL"
        low, high = t.get("temp_low", 0), t.get("temp_high", 0)
        is_f = t.get("is_fahrenheit", True)

        if not is_f:
            lf = low * 9/5 + 32 if low != -999 else -999
            hf = high * 9/5 + 32 if high != 999 else 999
        else:
            lf, hf = low, high

        in_bucket = lf <= actual < hf
        won = in_bucket if side == "BUY" else not in_bucket

        mp = t.get("market_prob", 0)
        sz = t["trade_size"]

        if won:
            if side == "BUY":
                pnl = sz * (1.0 / max(mp, 0.05) - 1.0)
            else:
                pnl = sz * (mp / max(1.0 - mp, 0.05))
            pnl = min(pnl, sz * 19)
            t["outcome"] = "WIN"
            t["pnl"] = pnl
            wins += 1
        else:
            t["outcome"] = "LOSS"
            t["pnl"] = -sz
            losses += 1

        total_pnl += t["pnl"]

    # Print table
    hdr = f"  {'City':<14} {'Bucket':<16} {'Side':<5} {'$Size':>6} {'Mkt':>5} {'Ens':>5} {'Edge':>6} {'Actual':>8} {'':>4} {'P&L':>9}"
    print(hdr)
    print(f"  {'-'*94}")

    for t in trades:
        ck = t.get("city_key", "?")[:14]
        low, high = t.get("temp_low", 0), t.get("temp_high", 0)
        is_f = t.get("is_fahrenheit", True)
        if not is_f:
            dl = low * 9/5 + 32 if low != -999 else -999
            dh = high * 9/5 + 32 if high != 999 else 999
        else:
            dl, dh = low, high

        if dl == -999:
            bkt = f"<={dh:.0f}F"
        elif dh == 999:
            bkt = f">={dl:.0f}F"
        else:
            bkt = f"{dl:.0f}-{dh:.0f}F"

        side = "BUY" if t["edge"] > 0 else "SELL"
        act = f"{t['actual_temp']:.1f}F" if t.get("actual_temp") else "N/A"
        outcome = t.get("outcome", "?")
        pnl = t.get("pnl", 0)

        print(f"  {ck:<14} {bkt:<16} {side:<5} ${t['trade_size']:>5.0f} {t['market_prob']:>4.0%} {t['ensemble_prob']:>4.0%} {t['edge']*100:>+5.0f}% {act:>8} {outcome:>4} ${pnl:>+8.2f}")

    print(f"\n  Summary: {wins}W / {losses}L | P&L: ${total_pnl:+.2f}")
    return {"date": target_date, "trades": trades, "wins": wins, "losses": losses, "pnl": total_pnl}


def main():
    print("Fetching all Polymarket weather events...")
    all_events = fetch_all_weather_events()
    print(f"Loaded {len(all_events)} events")

    today = date.today()
    results = []
    total_pnl = 0
    total_wins = 0
    total_losses = 0
    total_trades = 0

    for i in range(3, 0, -1):
        d = today - timedelta(days=i)
        r = analyze_day(all_events, d.strftime("%Y-%m-%d"))
        results.append(r)
        total_pnl += r["pnl"]
        total_wins += r["wins"]
        total_losses += r["losses"]
        total_trades += len(r["trades"])

    print(f"\n{'='*70}")
    print(f"  GRAND TOTAL — Past 3 Days")
    print(f"{'='*70}")
    print(f"  Trades:    {total_trades}")
    print(f"  Record:    {total_wins}W / {total_losses}L", end="")
    if total_trades > 0:
        print(f" ({total_wins/total_trades:.0%} win rate)")
    else:
        print()
    print(f"  P&L:       ${total_pnl:+.2f}")
    print(f"  Capital:   $1,000/day x 3 = $3,000")
    if total_trades > 0:
        print(f"  ROI:       {total_pnl/3000:.1%}")

    print(f"\n  {'Date':<12} {'Trades':>7} {'W/L':>8} {'P&L':>11}")
    print(f"  {'-'*40}")
    for r in results:
        w, l = r["wins"], r["losses"]
        n = len(r["trades"])
        print(f"  {r['date']:<12} {n:>7} {f'{w}W/{l}L':>8} ${r['pnl']:>+10.2f}")


if __name__ == "__main__":
    main()
