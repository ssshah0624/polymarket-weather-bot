#!/usr/bin/env python3
"""
Replay Kalshi selector policies against stored comparison snapshots.

This compares:
- old_raw: all candidate Kalshi bets as recorded
- normalized: all candidate Kalshi bets, but normalized to one event budget
- hedged: the current hedged selector in core.strategy.signals

It grades against resolved Kalshi trades in the local SQLite DB.
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.resolution import check_bucket_hit
from core.strategy.signals import (
    _rebalance_selected_event_signals,
    _select_kalshi_event_signals,
)
from core.tuning import get_effective_strategy_params

MARKET_TIMEZONE = ZoneInfo("America/New_York")


@dataclass
class ReplaySignal:
    city: str
    target_date: str
    bucket_question: str
    side: str
    edge: float
    trade_size: float
    selected_prob: float
    entry_price: float
    temp_low: float | None
    temp_high: float | None
    forecast_context: dict

    def to_selector_dict(self) -> dict:
        return {
            "venue": "kalshi",
            "city": self.city,
            "target_date": self.target_date,
            "bucket_question": self.bucket_question,
            "side": self.side,
            "edge": self.edge,
            "trade_size": self.trade_size,
            "selected_prob": self.selected_prob,
            "entry_price": self.entry_price,
            "temp_low": self.temp_low,
            "temp_high": self.temp_high,
            "forecast_context": dict(self.forecast_context),
        }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db-path",
        default="data/trades.db",
        help="Path to the SQLite database",
    )
    parser.add_argument(
        "--cutoff-date",
        default=None,
        help="Only include target dates on or before YYYY-MM-DD",
    )
    parser.add_argument(
        "--mode",
        default=None,
        help="Optional snapshot mode filter, e.g. paper or live",
    )
    parser.add_argument(
        "--max-examples",
        type=int,
        default=12,
        help="Max example events to include in output",
    )
    return parser.parse_args()


def _parse_bucket(question: str) -> tuple[float | None, float | None]:
    question = question.replace("°", "")

    match = re.search(r"(\d+)\D+or\D+below", question, re.IGNORECASE)
    if match:
        return -999.0, float(match.group(1)) + 1.0

    match = re.search(r"(\d+)\D+or\D+above", question, re.IGNORECASE)
    if match:
        return float(match.group(1)), 999.0

    match = re.search(r"(\d+)\D+(\d+)", question)
    if match:
        return float(match.group(1)), float(match.group(2)) + 1.0

    return None, None


def _load_resolved_actuals(conn: sqlite3.Connection) -> dict[tuple[str, str], float]:
    rows = conn.execute(
        """
        SELECT city, target_date, actual_temp
        FROM trades
        WHERE venue = 'kalshi'
          AND resolved = 1
          AND actual_temp IS NOT NULL
        """
    ).fetchall()
    return {(row["city"], row["target_date"]): float(row["actual_temp"]) for row in rows}


def _load_first_snapshot_rows(
    conn: sqlite3.Connection,
    cutoff_date: str | None,
    mode: str | None,
    resolved_actuals: dict[tuple[str, str], float],
) -> list[sqlite3.Row]:
    query = """
        SELECT *
        FROM weather_comparison_snapshots
        WHERE candidate_bets_json IS NOT NULL
          AND candidate_bets_json != '[]'
    """
    params: list[object] = []
    if cutoff_date:
        query += " AND target_date <= ?"
        params.append(cutoff_date)
    if mode:
        query += " AND mode = ?"
        params.append(mode)
    query += " ORDER BY timestamp ASC"

    rows = conn.execute(query, params).fetchall()
    first_rows: dict[tuple[str, str, str], sqlite3.Row] = {}
    for row in rows:
        if (row["city"], row["target_date"]) not in resolved_actuals:
            continue
        key = (row["mode"], row["city"], row["target_date"])
        if key not in first_rows:
            first_rows[key] = row
    return list(first_rows.values())


def _snapshot_anchor_temp(row: sqlite3.Row) -> float | None:
    model_summary = json.loads(row["model_summary_json"] or "{}")
    anchor = model_summary.get("forecast_anchor_temp")
    if anchor is None:
        anchor = model_summary.get("nws_hourly_max_temp")
    if anchor is None:
        anchor = model_summary.get("nws_temp")
    if anchor is None:
        anchor = row["model_expected_high"]
    return float(anchor) if anchor is not None else None


def _candidate_signals(row: sqlite3.Row) -> list[ReplaySignal]:
    anchor = _snapshot_anchor_temp(row)
    candidate_bets = json.loads(row["candidate_bets_json"] or "[]")
    signals: list[ReplaySignal] = []

    for bet in candidate_bets:
        if bet.get("venue") != "kalshi":
            continue
        trade_size = float(bet.get("trade_size") or 0.0)
        if trade_size <= 0:
            continue
        temp_low, temp_high = _parse_bucket(bet["bucket_question"])
        signals.append(
            ReplaySignal(
                city=row["city"],
                target_date=row["target_date"],
                bucket_question=bet["bucket_question"],
                side=bet["side"],
                edge=float(bet.get("edge") or 0.0),
                trade_size=trade_size,
                selected_prob=float(bet.get("model_probability") or 0.0),
                entry_price=float(bet.get("entry_price") or 0.0),
                temp_low=temp_low,
                temp_high=temp_high,
                forecast_context={
                    "selected_prob": float(bet.get("model_probability") or 0.0),
                    "forecast_anchor_temp": anchor,
                    "ensemble_mean": row["model_expected_high"],
                },
            )
        )

    return signals


def _score_signal(signal: dict, actual_temp: float) -> float:
    stake = float(signal.get("trade_size") or 0.0)
    entry_price = float(signal.get("entry_price") or 0.0)
    if stake <= 0 or entry_price <= 0 or entry_price >= 1:
        return 0.0

    in_bucket = check_bucket_hit(actual_temp, signal["bucket_question"], signal["city"])
    if signal["side"] == "BUY":
        return round(stake * ((1 / entry_price) - 1), 2) if in_bucket else round(-stake, 2)
    return round(stake * ((1 / (1 - entry_price)) - 1), 2) if not in_bucket else round(-stake, 2)


def _normalize_event_budget(signals: list[ReplaySignal]) -> list[dict]:
    if not signals:
        return []
    event_budget = max(signal.trade_size for signal in signals)
    total_size = sum(signal.trade_size for signal in signals)
    normalized = []
    for signal in signals:
        item = signal.to_selector_dict()
        item["trade_size"] = round(event_budget * (signal.trade_size / total_size), 2) if total_size else 0.0
        normalized.append(item)
    return normalized


def _policy_signals(policy: str, signals: list[ReplaySignal], strategy_params: dict) -> list[dict]:
    if policy == "old_raw":
        return [signal.to_selector_dict() for signal in signals]
    if policy == "normalized":
        return _normalize_event_budget(signals)
    if policy == "hedged":
        selected = _select_kalshi_event_signals(
            [signal.to_selector_dict() for signal in signals],
            strategy_params,
        )
        return _rebalance_selected_event_signals(selected)
    raise ValueError(f"Unknown policy: {policy}")


def _policy_summary() -> dict:
    return {
        "events": 0,
        "bets": 0,
        "stake": 0.0,
        "pnl": 0.0,
        "profitable_events": 0,
    }


def _lead_hours_from_snapshot(row: sqlite3.Row) -> float | None:
    timestamp = row["timestamp"]
    if not timestamp:
        return None
    try:
        captured_at = datetime.fromisoformat(str(timestamp).replace("Z", "+00:00")).astimezone(MARKET_TIMEZONE)
        target_settlement = (
            datetime.strptime(row["target_date"], "%Y-%m-%d").replace(tzinfo=MARKET_TIMEZONE)
            + timedelta(days=1)
        )
    except ValueError:
        return None
    return round((target_settlement - captured_at).total_seconds() / 3600.0, 2)


def _lead_bucket(lead_hours: float | None) -> str:
    if lead_hours is None:
        return "unknown"
    if lead_hours <= 6:
        return "0-6h"
    if lead_hours <= 12:
        return "6-12h"
    if lead_hours <= 24:
        return "12-24h"
    if lead_hours <= 36:
        return "24-36h"
    if lead_hours <= 48:
        return "36-48h"
    return "48h+"


def main() -> int:
    args = _parse_args()
    conn = sqlite3.connect(args.db_path)
    conn.row_factory = sqlite3.Row

    resolved_actuals = _load_resolved_actuals(conn)
    snapshot_rows = _load_first_snapshot_rows(
        conn,
        cutoff_date=args.cutoff_date,
        mode=args.mode,
        resolved_actuals=resolved_actuals,
    )

    strategy_params = get_effective_strategy_params("kalshi")
    policies = ("old_raw", "normalized", "hedged")
    summary = {policy: _policy_summary() for policy in policies}
    city_summary = {policy: defaultdict(lambda: {"events": 0, "bets": 0, "stake": 0.0, "pnl": 0.0}) for policy in policies}
    date_summary = {policy: defaultdict(lambda: {"events": 0, "bets": 0, "stake": 0.0, "pnl": 0.0}) for policy in policies}
    lead_summary = {policy: defaultdict(lambda: {"events": 0, "bets": 0, "stake": 0.0, "pnl": 0.0}) for policy in policies}
    examples = []

    for row in snapshot_rows:
        candidate_signals = _candidate_signals(row)
        if not candidate_signals:
            continue

        actual_temp = resolved_actuals[(row["city"], row["target_date"])]
        lead_hours = _lead_hours_from_snapshot(row)
        lead_bucket = _lead_bucket(lead_hours)
        event_examples = {}

        for policy in policies:
            selected_signals = _policy_signals(policy, candidate_signals, strategy_params)
            event_pnl = sum(_score_signal(signal, actual_temp) for signal in selected_signals)
            event_stake = sum(float(signal.get("trade_size") or 0.0) for signal in selected_signals)

            summary[policy]["events"] += 1
            summary[policy]["bets"] += len(selected_signals)
            summary[policy]["stake"] += event_stake
            summary[policy]["pnl"] += event_pnl
            if event_pnl > 0:
                summary[policy]["profitable_events"] += 1

            city_row = city_summary[policy][row["city"]]
            city_row["events"] += 1
            city_row["bets"] += len(selected_signals)
            city_row["stake"] += event_stake
            city_row["pnl"] += event_pnl

            date_row = date_summary[policy][row["target_date"]]
            date_row["events"] += 1
            date_row["bets"] += len(selected_signals)
            date_row["stake"] += event_stake
            date_row["pnl"] += event_pnl

            lead_row = lead_summary[policy][lead_bucket]
            lead_row["events"] += 1
            lead_row["bets"] += len(selected_signals)
            lead_row["stake"] += event_stake
            lead_row["pnl"] += event_pnl

            event_examples[policy] = {
                "pnl": round(event_pnl, 2),
                "signals": [
                    (
                        signal["bucket_question"],
                        signal["side"],
                        round(float(signal.get("trade_size") or 0.0), 2),
                    )
                    for signal in selected_signals
                ],
            }

        if len(examples) < args.max_examples:
            examples.append(
                {
                    "city": row["city"],
                    "target_date": row["target_date"],
                    "actual_temp": actual_temp,
                    "lead_hours": lead_hours,
                    "lead_bucket": lead_bucket,
                    "policies": event_examples,
                }
            )

    output = {
        "db_path": str(Path(args.db_path).resolve()),
        "cutoff_date": args.cutoff_date,
        "mode": args.mode,
        "summary": {
            policy: {
                **values,
                "stake": round(values["stake"], 2),
                "pnl": round(values["pnl"], 2),
                "roi_pct": round((values["pnl"] / values["stake"] * 100), 2) if values["stake"] else 0.0,
            }
            for policy, values in summary.items()
        },
        "by_city": {
            policy: {
                city: {
                    **values,
                    "stake": round(values["stake"], 2),
                    "pnl": round(values["pnl"], 2),
                    "roi_pct": round((values["pnl"] / values["stake"] * 100), 2) if values["stake"] else 0.0,
                }
                for city, values in sorted(city_rows.items())
            }
            for policy, city_rows in city_summary.items()
        },
        "by_date": {
            policy: {
                target_date: {
                    **values,
                    "stake": round(values["stake"], 2),
                    "pnl": round(values["pnl"], 2),
                    "roi_pct": round((values["pnl"] / values["stake"] * 100), 2) if values["stake"] else 0.0,
                }
                for target_date, values in sorted(date_rows.items())
            }
            for policy, date_rows in date_summary.items()
        },
        "by_lead_bucket": {
            policy: {
                bucket: {
                    **values,
                    "stake": round(values["stake"], 2),
                    "pnl": round(values["pnl"], 2),
                    "roi_pct": round((values["pnl"] / values["stake"] * 100), 2) if values["stake"] else 0.0,
                }
                for bucket, values in sorted(lead_rows.items())
            }
            for policy, lead_rows in lead_summary.items()
        },
        "examples": examples,
    }
    print(json.dumps(output, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
