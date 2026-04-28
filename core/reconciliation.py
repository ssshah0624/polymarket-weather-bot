"""
Reconcile saved scan reports with the trade ledger.

This is primarily used to recover historical Kalshi paper trades when a scan
report exists but the corresponding DB row is missing.
"""

from __future__ import annotations

import json
import logging
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from config.settings import (
    CITIES,
    KALSHI_API_KEY_ID,
    KALSHI_PRIVATE_KEY_PATH,
    KALSHI_USE_DEMO,
    PROJECT_ROOT,
)
from core.alerts import calc_fee_pct
from core.data.kalshi import _event_title, extract_market_city, extract_market_date, parse_market_bucket
from core.database import (
    Trade,
    WeatherComparisonSnapshot,
    has_logged_trade,
    log_trade,
    session_scope,
)
from core.execution.kalshi_client import KalshiClient

logger = logging.getLogger(__name__)

REPORTS_DIR = PROJECT_ROOT / "data" / "reports"

_REPORT_FILENAME_RE = re.compile(r"scan_(\d{4}-\d{2}-\d{2}_\d{4})\.md$")
_SECTION_RE = re.compile(r"^##\s+(?P<venue>[A-Za-z]+)\s+—.+\((?P<target_date>\d{4}-\d{2}-\d{2})\)\s*$")
_HEADING_RE = re.compile(
    r"^###\s+(?P<venue>[A-Za-z]+)\s+\|\s+(?P<city>.+?)\s+\([^)]+\)\s+\|\s+(?P<bucket>.+)\s*$"
)
_PRICE_RE = re.compile(
    r"prices this at\s+(?P<market>\d+(?:\.\d+)?)%,\s+but our models say(?: only)?\s+"
    r"(?P<model>\d+(?:\.\d+)?)%",
    re.IGNORECASE,
)
_SIMULATION_RE = re.compile(r"averaging\s+(?P<avg>-?\d+(?:\.\d+)?)°F", re.IGNORECASE)
_TRADE_RE = re.compile(
    r"^\*\*(?P<side>YES|NO)\s+\$(?P<stake>\d+(?:\.\d+)?)\s+to\s+win\s+\$(?P<payout>\d+(?:\.\d+)?)\s+"
    r"\([^)]+\)\s+\|\s+Edge\s+(?P<edge>\d+(?:\.\d+)?)%\s+\|\s+(?P<crowd>.+)\*\*\s*$"
)


def _report_timestamp(path: Path) -> Optional[datetime]:
    match = _REPORT_FILENAME_RE.match(path.name)
    if not match:
        return None
    return datetime.strptime(match.group(1), "%Y-%m-%d_%H%M").replace(tzinfo=timezone.utc)


def _normalize_city_key(city_label: str) -> Optional[str]:
    normalized = city_label.strip().lower().replace(".", "")
    normalized = re.sub(r"\s+", " ", normalized)
    for key, cfg in CITIES.items():
        aliases = {
            key,
            cfg.get("name", "").lower(),
            *(name.lower() for name in cfg.get("polymarket_names", [])),
            *(name.lower() for name in cfg.get("kalshi_names", [])),
        }
        if normalized in aliases:
            return key
    return None


def _build_bucket_question(city_key: str, bucket_label: str, target_date: str) -> str:
    city_name = CITIES[city_key]["name"]
    target_dt = datetime.strptime(target_date, "%Y-%m-%d")
    return (
        f"Will the highest temperature in {city_name} be {bucket_label} "
        f"on {target_dt.strftime('%B')} {target_dt.day}?"
    )


def _parse_trade_block(lines: list[str], *, venue: str, city_label: str,
                       bucket_label: str, target_date: str,
                       report_timestamp: Optional[datetime]) -> Optional[dict]:
    market_prob = None
    model_prob = None
    model_expected_high = None
    trade_meta = None

    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            continue
        price_match = _PRICE_RE.search(line)
        if price_match:
            market_prob = float(price_match.group("market")) / 100.0
            model_prob = float(price_match.group("model")) / 100.0
            continue
        sim_match = _SIMULATION_RE.search(line)
        if sim_match:
            model_expected_high = float(sim_match.group("avg"))
            continue
        trade_match = _TRADE_RE.match(line)
        if trade_match:
            trade_meta = trade_match.groupdict()

    if trade_meta is None or market_prob is None or model_prob is None:
        return None

    city_key = _normalize_city_key(city_label)
    if city_key is None:
        logger.warning("Skipping report trade with unknown city label: %s", city_label)
        return None

    side = "BUY" if trade_meta["side"] == "YES" else "SELL"
    signed_edge = float(trade_meta["edge"]) / 100.0
    if side == "SELL":
        signed_edge *= -1

    bucket_question = _build_bucket_question(city_key, bucket_label, target_date)
    entry_price = market_prob if side == "BUY" else max(1.0 - market_prob, 0.0)
    fee_pct = calc_fee_pct(market_prob, venue=venue)

    return {
        "timestamp": report_timestamp,
        "venue": venue,
        "event_title": f"{venue.title()} {CITIES[city_key]['name']} {bucket_label}",
        "city": city_key,
        "target_date": target_date,
        "bucket_question": bucket_question,
        "side": side,
        "trade_size": float(trade_meta["stake"]),
        "market_prob": market_prob,
        "entry_price": entry_price,
        "ensemble_prob": model_prob,
        "edge": signed_edge,
        "fee_pct": fee_pct,
        "is_contrarian": "against crowd" in trade_meta["crowd"].lower(),
        "model_expected_high": model_expected_high,
        "signal": "report_backfill",
        "strategy_version": "report_backfill",
    }


def parse_scan_report(path: str | Path, venue: str = "kalshi") -> list[dict]:
    """Parse trade narratives from one markdown scan report."""
    report_path = Path(path)
    target_venue = (venue or "").lower()
    report_timestamp = _report_timestamp(report_path)
    trades: list[dict] = []

    current_section_venue = None
    current_target_date = None
    current_trade_meta = None
    current_block: list[str] = []

    def flush_current_trade():
        nonlocal current_trade_meta, current_block
        if current_trade_meta is None or current_target_date is None:
            current_trade_meta = None
            current_block = []
            return
        parsed = _parse_trade_block(
            current_block,
            venue=current_trade_meta["venue"],
            city_label=current_trade_meta["city"],
            bucket_label=current_trade_meta["bucket"],
            target_date=current_target_date,
            report_timestamp=report_timestamp,
        )
        if parsed is not None and parsed["venue"] == target_venue:
            trades.append(parsed)
        current_trade_meta = None
        current_block = []

    for raw_line in report_path.read_text().splitlines():
        section_match = _SECTION_RE.match(raw_line)
        if section_match:
            flush_current_trade()
            current_section_venue = section_match.group("venue").lower()
            current_target_date = section_match.group("target_date")
            continue

        heading_match = _HEADING_RE.match(raw_line)
        if heading_match:
            flush_current_trade()
            heading_venue = heading_match.group("venue").lower()
            if heading_venue == current_section_venue and current_target_date:
                current_trade_meta = {
                    "venue": heading_venue,
                    "city": heading_match.group("city"),
                    "bucket": heading_match.group("bucket"),
                }
            continue

        if raw_line.strip() == "---":
            flush_current_trade()
            continue

        if current_trade_meta is not None:
            current_block.append(raw_line)

    flush_current_trade()
    return trades


def backfill_scan_reports(report_dir: str | Path = REPORTS_DIR,
                          venue: str = "kalshi",
                          mode: str = "paper") -> dict:
    """Backfill missing venue trades from saved scan reports into the DB."""
    report_root = Path(report_dir)
    inserted = 0
    skipped = 0
    parsed = 0
    inserted_trades: list[dict] = []

    for path in sorted(report_root.glob("scan_*.md")):
        for trade in parse_scan_report(path, venue=venue):
            parsed += 1
            if has_logged_trade(
                city=trade["city"],
                target_date=trade["target_date"],
                bucket_question=trade["bucket_question"],
                side=trade["side"],
                mode=mode,
                venue=trade["venue"],
            ):
                skipped += 1
                continue
            log_trade(trade, mode=mode)
            inserted += 1
            inserted_trades.append(trade)

    summary = {
        "venue": venue,
        "parsed": parsed,
        "inserted": inserted,
        "skipped": skipped,
        "trades": inserted_trades,
    }
    logger.info(
        "Backfilled %s trades from scan reports for %s (%s skipped duplicates)",
        inserted,
        venue,
        skipped,
    )
    return summary


def _has_logged_venue_order(order_id: str, mode: str = "live", venue: str = "kalshi") -> bool:
    if not order_id:
        return False
    with session_scope() as session:
        existing = session.query(Trade).filter_by(
            venue=venue,
            mode=mode,
            venue_order_id=order_id,
        ).first()
        return existing is not None


def _latest_snapshot_candidate(city: str, target_date: str, bucket_question: str, side: str, mode: str = "live") -> dict:
    with session_scope() as session:
        rows = (
            session.query(WeatherComparisonSnapshot)
            .filter_by(mode=mode, city=city, target_date=target_date)
            .order_by(WeatherComparisonSnapshot.timestamp.desc())
            .all()
        )
        snapshots = [
            {
                "candidate_bets_json": row.candidate_bets_json,
                "model_summary_json": row.model_summary_json,
                "model_expected_high": row.model_expected_high,
                "model_spread": row.model_spread,
                "kalshi_implied_high": row.kalshi_implied_high,
                "strategy_version": row.strategy_version,
            }
            for row in rows
        ]

    for row in snapshots:
        try:
            candidates = json.loads(row["candidate_bets_json"] or "[]")
            model_summary = json.loads(row["model_summary_json"] or "{}")
        except json.JSONDecodeError:
            continue
        for candidate in candidates:
            if (
                candidate.get("venue") == "kalshi"
                and candidate.get("bucket_question") == bucket_question
                and candidate.get("side") == side
            ):
                return {
                    "candidate": candidate,
                    "model_expected_high": row["model_expected_high"],
                    "model_spread": row["model_spread"],
                    "venue_implied_high": row["kalshi_implied_high"],
                    "strategy_version": row["strategy_version"],
                    "model_summary": model_summary,
                }
    return {}


def _aggregate_fill_orders(fills: list[dict]) -> list[dict]:
    grouped: dict[str, dict] = {}
    for fill in fills:
        order_id = fill.get("order_id")
        ticker = fill.get("ticker") or fill.get("market_ticker")
        side = fill.get("side")
        if not order_id or not ticker or side not in {"yes", "no"}:
            continue

        count = float(fill.get("count_fp") or fill.get("count") or 0)
        yes_price = float(fill.get("yes_price_dollars") or 0)
        no_price = float(fill.get("no_price_dollars") or 0)
        fee = float(fill.get("fee_cost") or 0)
        created_time = fill.get("created_time")

        row = grouped.setdefault(
            order_id,
            {
                "order_id": order_id,
                "ticker": ticker,
                "side": side,
                "contracts": 0.0,
                "cost": 0.0,
                "fees": 0.0,
                "yes_notional": 0.0,
                "no_notional": 0.0,
                "first_fill_at": created_time,
                "last_fill_at": created_time,
            },
        )
        row["contracts"] += count
        row["cost"] += count * (yes_price if side == "yes" else no_price)
        row["fees"] += fee
        row["yes_notional"] += count * yes_price
        row["no_notional"] += count * no_price
        if created_time and (row["first_fill_at"] is None or created_time < row["first_fill_at"]):
            row["first_fill_at"] = created_time
        if created_time and (row["last_fill_at"] is None or created_time > row["last_fill_at"]):
            row["last_fill_at"] = created_time

    return list(grouped.values())


def backfill_kalshi_live_fills(client: KalshiClient | None = None, limit: int = 200) -> dict:
    """Backfill missing live Kalshi trades from authenticated fills into the DB."""
    client = client or KalshiClient(
        api_key_id=KALSHI_API_KEY_ID,
        private_key_path=KALSHI_PRIVATE_KEY_PATH,
        use_demo=KALSHI_USE_DEMO,
    )

    fills = client.get_fills(limit=limit)
    aggregated_orders = _aggregate_fill_orders(fills)
    inserted = 0
    skipped = 0
    inserted_trades: list[dict] = []
    market_cache: dict[str, dict] = {}

    for order in aggregated_orders:
        order_id = order["order_id"]
        if _has_logged_venue_order(order_id, mode="live", venue="kalshi"):
            skipped += 1
            continue

        ticker = order["ticker"]
        market = market_cache.get(ticker)
        if market is None:
            market = client.get_market(ticker)
            market_cache[ticker] = market

        city = extract_market_city(market)
        target_date = extract_market_date(market)
        bucket = parse_market_bucket(market)
        if not city or not target_date or not bucket:
            logger.warning("Skipping Kalshi fill backfill with unparsable market: %s", ticker)
            skipped += 1
            continue

        contracts = order["contracts"]
        if contracts <= 0:
            skipped += 1
            continue

        avg_yes_price = order["yes_notional"] / contracts if contracts else 0.0
        avg_no_price = order["no_notional"] / contracts if contracts else 0.0
        side = "BUY" if order["side"] == "yes" else "SELL"
        entry_price = order["cost"] / contracts if contracts else 0.0
        snapshot_match = _latest_snapshot_candidate(
            city=city,
            target_date=target_date,
            bucket_question=bucket["question"],
            side=side,
            mode="live",
        )
        candidate = snapshot_match.get("candidate", {})
        model_summary = snapshot_match.get("model_summary", {})
        forecast_context = {
            "source": "kalshi_live_fill_backfill",
            "selected_prob": candidate.get("model_probability"),
            "market_prob": avg_yes_price,
            "entry_price": entry_price,
            "yes_price": avg_yes_price,
            "no_price": avg_no_price,
            "ensemble_mean": snapshot_match.get("model_expected_high"),
            "ensemble_spread": snapshot_match.get("model_spread"),
            "ensemble_members": model_summary.get("ensemble_members"),
            "nws_temp": model_summary.get("nws_temp"),
            "forecast_horizon_days": model_summary.get("forecast_horizon_days"),
        }
        forecast_context = {k: v for k, v in forecast_context.items() if v is not None}

        trade = {
            "timestamp": order["first_fill_at"],
            "submitted_at": order["first_fill_at"],
            "filled_at": order["last_fill_at"],
            "venue": "kalshi",
            "event_title": _event_title(market, city, target_date),
            "event_id": market.get("event_ticker") or ticker,
            "venue_event_id": market.get("event_ticker") or ticker,
            "city": city,
            "target_date": target_date,
            "bucket_question": bucket["question"],
            "market_id": ticker,
            "venue_market_id": ticker,
            "venue_order_id": order_id,
            "side": side,
            "trade_size": round(order["cost"], 2),
            "intended_size_usd": round(order["cost"], 2),
            "filled_size_usd": round(order["cost"], 2),
            "filled_contracts": int(round(contracts)),
            "market_prob": round(avg_yes_price, 4),
            "entry_price": round(entry_price, 4),
            "expected_entry_price": candidate.get("entry_price", round(entry_price, 4)),
            "fill_price": round(entry_price, 4),
            "ensemble_prob": candidate.get("model_probability", 0.0),
            "edge": candidate.get("edge", 0.0),
            "signal": "live_fill_backfill",
            "order_status": "executed",
            "fee_usd": round(order["fees"], 2),
            "is_contrarian": "crowd" in (candidate.get("rationale") or "").lower() and "against" in (candidate.get("rationale") or "").lower(),
            "strategy_version": snapshot_match.get("strategy_version") or "live_fill_backfill",
            "model_expected_high": snapshot_match.get("model_expected_high"),
            "model_spread": snapshot_match.get("model_spread"),
            "venue_implied_high": snapshot_match.get("venue_implied_high"),
            "forecast_context": forecast_context,
        }

        log_trade(trade, mode="live")
        inserted += 1
        inserted_trades.append(trade)

    summary = {
        "parsed": len(aggregated_orders),
        "inserted": inserted,
        "skipped": skipped,
        "trades": inserted_trades,
    }
    logger.info(
        "Backfilled %s live Kalshi orders from fills (%s skipped)",
        inserted,
        skipped,
    )
    return summary
