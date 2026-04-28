"""
Runtime Kalshi tuning overrides.

Stores bounded strategy parameter adjustments outside tracked config so the
paper bot can safely self-tune and report what changed.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from config.settings import (
    CONTRARIAN_DISCOUNT,
    EDGE_THRESHOLD,
    KALSHI_ADJACENT_SPLIT_BOUNDARY_DISTANCE_F,
    KALSHI_ADJACENT_SPLIT_MIN_COMBINED_PROB,
    KALSHI_ADJACENT_SPLIT_MIN_SECOND_BUCKET_PROB,
    KALSHI_BUCKET_CENTER_TOLERANCE_F,
    KALSHI_FEE_BUFFER_PCT,
    KALSHI_HEDGED_PRIMARY_WEIGHT,
    KALSHI_LOCAL_LADDER_MAX_BUCKETS,
    KALSHI_LOCAL_LADDER_MAX_DISTANCE_F,
    KALSHI_NWS_BLEND_WEIGHT,
    KALSHI_NWS_DISAGREEMENT_THRESHOLD_F,
    KALSHI_SELL_OUTSIDE_BUCKET_MARGIN_F,
    KALSHI_SKIP_MARKET_DIVERGENCE_F,
    KALSHI_TUNING_HISTORY_PATH,
    KALSHI_TUNING_OVERRIDES_PATH,
    KELLY_FRACTION,
    MAX_DAILY_LOSS,
    MAX_TRADE_SIZE,
)

TUNABLE_KEYS = (
    "edge_threshold",
    "contrarian_discount",
    "kalshi_fee_buffer_pct",
    "max_trade_size",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _round_param(key: str, value: float) -> float:
    if key == "max_trade_size":
        return round(value, 2)
    return round(value, 4)


def _avg_pnl(stats: dict) -> float:
    trades = stats.get("trades", 0)
    return stats.get("pnl", 0.0) / trades if trades else 0.0


def _win_rate(stats: dict) -> float:
    trades = stats.get("trades", 0)
    return stats.get("wins", 0) / trades if trades else 0.0


def get_base_strategy_params(venue: str | None = None) -> dict:
    """Base params from tracked config before runtime overrides."""
    return {
        "venue": venue or "polymarket",
        "edge_threshold": EDGE_THRESHOLD,
        "kelly_fraction": KELLY_FRACTION,
        "contrarian_discount": CONTRARIAN_DISCOUNT,
        "kalshi_fee_buffer_pct": KALSHI_FEE_BUFFER_PCT,
        "kalshi_nws_blend_weight": KALSHI_NWS_BLEND_WEIGHT,
        "kalshi_nws_disagreement_threshold_f": KALSHI_NWS_DISAGREEMENT_THRESHOLD_F,
        "kalshi_adjacent_split_min_combined_prob": KALSHI_ADJACENT_SPLIT_MIN_COMBINED_PROB,
        "kalshi_adjacent_split_min_second_bucket_prob": KALSHI_ADJACENT_SPLIT_MIN_SECOND_BUCKET_PROB,
        "kalshi_adjacent_split_boundary_distance_f": KALSHI_ADJACENT_SPLIT_BOUNDARY_DISTANCE_F,
        "kalshi_hedged_primary_weight": KALSHI_HEDGED_PRIMARY_WEIGHT,
        "kalshi_bucket_center_tolerance_f": KALSHI_BUCKET_CENTER_TOLERANCE_F,
        "kalshi_sell_outside_bucket_margin_f": KALSHI_SELL_OUTSIDE_BUCKET_MARGIN_F,
        "kalshi_skip_market_divergence_f": KALSHI_SKIP_MARKET_DIVERGENCE_F,
        "kalshi_local_ladder_max_buckets": KALSHI_LOCAL_LADDER_MAX_BUCKETS,
        "kalshi_local_ladder_max_distance_f": KALSHI_LOCAL_LADDER_MAX_DISTANCE_F,
        "max_trade_size": MAX_TRADE_SIZE,
        "max_daily_loss": MAX_DAILY_LOSS,
    }


def load_kalshi_tuning_state(path: Path | None = None) -> dict:
    """Load runtime tuning state from disk, or defaults if absent."""
    tuning_path = path or KALSHI_TUNING_OVERRIDES_PATH
    if not tuning_path.exists():
        return {"overrides": {}, "positive_streaks": {}, "updated_at": None}
    data = json.loads(tuning_path.read_text())
    return {
        "overrides": data.get("overrides", {}),
        "positive_streaks": data.get("positive_streaks", {}),
        "updated_at": data.get("updated_at"),
    }


def save_kalshi_tuning_state(state: dict, path: Path | None = None) -> None:
    tuning_path = path or KALSHI_TUNING_OVERRIDES_PATH
    tuning_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "overrides": state.get("overrides", {}),
        "positive_streaks": state.get("positive_streaks", {}),
        "updated_at": state.get("updated_at") or _utc_now(),
    }
    tuning_path.write_text(json.dumps(payload, indent=2, sort_keys=True))


def get_effective_strategy_params(venue: str) -> dict:
    """Merge base params with runtime overrides for Kalshi only."""
    params = get_base_strategy_params(venue)
    if venue != "kalshi":
        return params
    state = load_kalshi_tuning_state()
    params.update(state.get("overrides", {}))
    params["positive_streaks"] = state.get("positive_streaks", {})
    return params


def _append_history(entry: dict, path: Path | None = None) -> None:
    history_path = path or KALSHI_TUNING_HISTORY_PATH
    history_path.parent.mkdir(parents=True, exist_ok=True)
    with history_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, sort_keys=True) + "\n")


def _build_change(key: str, current: float, target: float, reason: str, direction: str) -> dict | None:
    rounded_target = _round_param(key, target)
    rounded_current = _round_param(key, current)
    if rounded_target == rounded_current:
        return None
    return {
        "parameter": key,
        "from": rounded_current,
        "to": rounded_target,
        "direction": direction,
        "reason": reason,
    }


def evaluate_kalshi_tuning(analysis: dict, base_params: dict, current_params: dict,
                           state: dict | None = None) -> dict:
    """Evaluate bounded Kalshi parameter changes from recent realized performance."""
    state = state or load_kalshi_tuning_state()
    current_overrides = dict(state.get("overrides", {}))
    prior_streaks = dict(state.get("positive_streaks", {}))
    positive_streaks = dict(prior_streaks)
    candidates: list[dict] = []
    held: list[str] = []

    total = analysis.get("total", 0)
    total_pnl = analysis.get("total_pnl", 0.0)
    win_rate = analysis.get("win_rate", 0.0)
    edge_stats = analysis.get("edge_stats", {})
    stance_stats = analysis.get("stance_stats", {})
    calibration_mae = analysis.get("calibration_mean_abs_error")

    current_edge = current_params["edge_threshold"]
    base_edge = base_params["edge_threshold"]
    low_edge = edge_stats.get("5-10%", {})
    medium_edge = edge_stats.get("10-20%", {})
    if low_edge.get("trades", 0) >= 12 and _win_rate(low_edge) < 0.45:
        change = _build_change(
            "edge_threshold",
            current_edge,
            min(current_edge + 0.01, 0.16),
            f"5-10% edge bucket won {_win_rate(low_edge):.0%} over {low_edge['trades']} Kalshi trades",
            "tighten",
        )
        if change:
            candidates.append(change)
        positive_streaks["edge_threshold"] = 0
    else:
        favorable = (
            current_edge > base_edge
            and medium_edge.get("trades", 0) >= 15
            and win_rate >= 0.55
            and total_pnl > 0
        )
        positive_streaks["edge_threshold"] = positive_streaks.get("edge_threshold", 0) + 1 if favorable else 0
        if favorable and positive_streaks["edge_threshold"] >= 2:
            change = _build_change(
                "edge_threshold",
                current_edge,
                max(current_edge - 0.01, base_edge),
                f"10-20% edge bucket stayed healthy across {medium_edge['trades']} trades for 2 evaluations",
                "loosen",
            )
            if change:
                candidates.append(change)
        elif current_edge > base_edge:
            held.append("Edge threshold easing is waiting for two consecutive strong Kalshi evaluations.")

    current_contrarian = current_params["contrarian_discount"]
    base_contrarian = base_params["contrarian_discount"]
    contrarian = stance_stats.get("contrarian", {})
    consensus = stance_stats.get("consensus", {})
    contrarian_gap = _win_rate(consensus) - _win_rate(contrarian)
    contrarian_pnl_gap = _avg_pnl(consensus) - _avg_pnl(contrarian)
    if (
        contrarian.get("trades", 0) >= 12
        and consensus.get("trades", 0) >= 12
        and contrarian_gap >= 0.15
        and contrarian_pnl_gap > 0
    ):
        change = _build_change(
            "contrarian_discount",
            current_contrarian,
            max(current_contrarian - 0.10, 0.30),
            f"Contrarian trades lagged consensus by {contrarian_gap:.0%} over {contrarian['trades']} trades",
            "tighten",
        )
        if change:
            candidates.append(change)
        positive_streaks["contrarian_discount"] = 0
    else:
        favorable = (
            current_contrarian < base_contrarian
            and contrarian.get("trades", 0) >= 12
            and consensus.get("trades", 0) >= 12
            and contrarian_gap <= 0.05
        )
        positive_streaks["contrarian_discount"] = positive_streaks.get("contrarian_discount", 0) + 1 if favorable else 0
        if favorable and positive_streaks["contrarian_discount"] >= 2:
            change = _build_change(
                "contrarian_discount",
                current_contrarian,
                min(current_contrarian + 0.05, base_contrarian),
                f"Contrarian win-rate gap narrowed to {contrarian_gap:.0%} for 2 evaluations",
                "loosen",
            )
            if change:
                candidates.append(change)
        elif current_contrarian < base_contrarian:
            held.append("Contrarian sizing recovery is waiting for two consecutive balanced evaluations.")

    current_fee = current_params["kalshi_fee_buffer_pct"]
    base_fee = base_params["kalshi_fee_buffer_pct"]
    if total >= 15 and total_pnl < 0 and calibration_mae is not None and calibration_mae >= 0.10:
        change = _build_change(
            "kalshi_fee_buffer_pct",
            current_fee,
            min(current_fee + 0.005, 0.03),
            f"Kalshi calibration error averaged {calibration_mae:.0%} over {total} resolved trades",
            "tighten",
        )
        if change:
            candidates.append(change)
        positive_streaks["kalshi_fee_buffer_pct"] = 0
    else:
        favorable = (
            current_fee > base_fee
            and total >= 20
            and total_pnl > 0
            and calibration_mae is not None
            and calibration_mae <= 0.05
        )
        positive_streaks["kalshi_fee_buffer_pct"] = positive_streaks.get("kalshi_fee_buffer_pct", 0) + 1 if favorable else 0
        if favorable and positive_streaks["kalshi_fee_buffer_pct"] >= 2:
            change = _build_change(
                "kalshi_fee_buffer_pct",
                current_fee,
                max(current_fee - 0.005, base_fee),
                f"Kalshi calibration error held at {calibration_mae:.0%} for 2 evaluations",
                "loosen",
            )
            if change:
                candidates.append(change)
        elif current_fee > base_fee:
            held.append("Kalshi fee buffer easing is waiting for two consecutive calibrated evaluations.")

    current_max = current_params["max_trade_size"]
    base_max = base_params["max_trade_size"]
    if total >= 15 and total_pnl <= -100:
        change = _build_change(
            "max_trade_size",
            current_max,
            max(current_max * 0.9, 25.0),
            f"Trailing Kalshi P&L is ${total_pnl:+.2f} across {total} resolved trades",
            "tighten",
        )
        if change:
            candidates.append(change)
        positive_streaks["max_trade_size"] = 0
    else:
        favorable = current_max < base_max and total_pnl >= 100 and win_rate >= 0.55
        positive_streaks["max_trade_size"] = positive_streaks.get("max_trade_size", 0) + 1 if favorable else 0
        if favorable and positive_streaks["max_trade_size"] >= 2:
            change = _build_change(
                "max_trade_size",
                current_max,
                min(current_max * 1.1, base_max),
                f"Trailing Kalshi P&L is ${total_pnl:+.2f} with a {win_rate:.0%} win rate for 2 evaluations",
                "loosen",
            )
            if change:
                candidates.append(change)
        elif current_max < base_max:
            held.append("Max trade size recovery is waiting for two consecutive strong Kalshi evaluations.")

    applied_changes = candidates[:2]
    next_overrides = dict(current_overrides)
    for change in applied_changes:
        base_value = _round_param(change["parameter"], base_params[change["parameter"]])
        if change["to"] == base_value:
            next_overrides.pop(change["parameter"], None)
        else:
            next_overrides[change["parameter"]] = change["to"]
        if change["direction"] == "tighten":
            positive_streaks[change["parameter"]] = 0
        else:
            positive_streaks[change["parameter"]] = 0

    effective_params = dict(base_params)
    effective_params.update(next_overrides)

    summary = {
        "evaluated_at": _utc_now(),
        "analysis_total": total,
        "applied_changes": applied_changes,
        "held_notes": held,
        "effective_params": effective_params,
        "next_state": {
            "overrides": next_overrides,
            "positive_streaks": positive_streaks,
            "updated_at": _utc_now(),
        },
    }
    return summary


def apply_kalshi_tuning(decision: dict,
                        path: Path | None = None,
                        history_path: Path | None = None) -> dict:
    """Persist tuning decision and append an audit event."""
    state = decision.get("next_state", {})
    save_kalshi_tuning_state(state, path=path)
    entry = {
        "timestamp": decision.get("evaluated_at") or _utc_now(),
        "applied_changes": decision.get("applied_changes", []),
        "held_notes": decision.get("held_notes", []),
        "effective_params": decision.get("effective_params", {}),
        "analysis_total": decision.get("analysis_total", 0),
    }
    _append_history(entry, path=history_path)
    return entry


def format_param_value(key: str, value: float) -> str:
    if key == "kalshi_fee_buffer_pct":
        return f"{value:.1%}"
    if key in {"edge_threshold", "contrarian_discount", "kelly_fraction"}:
        return f"{value:.0%}"
    if key == "max_trade_size":
        return f"${value:.0f}"
    return str(value)


def summarize_effective_params(params: dict) -> str:
    return " | ".join(
        [
            f"Min edge {format_param_value('edge_threshold', params['edge_threshold'])}",
            f"Kelly {format_param_value('kelly_fraction', params['kelly_fraction'])}",
            f"Contrarian {format_param_value('contrarian_discount', params['contrarian_discount'])}",
            f"Fee buffer {format_param_value('kalshi_fee_buffer_pct', params['kalshi_fee_buffer_pct'])}",
            f"Max size {format_param_value('max_trade_size', params['max_trade_size'])}",
        ]
    )
