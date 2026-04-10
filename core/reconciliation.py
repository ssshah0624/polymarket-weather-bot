"""
Reconcile saved scan reports with the trade ledger.

This is primarily used to recover historical Kalshi paper trades when a scan
report exists but the corresponding DB row is missing.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from config.settings import CITIES, PROJECT_ROOT
from core.alerts import calc_fee_pct
from core.database import has_logged_trade, log_trade

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
