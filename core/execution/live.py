"""
Kalshi live trading execution engine.
"""

from __future__ import annotations

from collections import Counter
import json
import logging
import math
import uuid
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from config.settings import (
    KALSHI_API_KEY_ID,
    KALSHI_LIVE_ALLOWED_DRIFT_CENTS,
    KALSHI_LIVE_BANKROLL_SLICE_USD,
    KALSHI_LIVE_BUDGET_ALLOCATION_USD,
    KALSHI_LIVE_CONTRARIAN_MAX_TRADE_SIZE_USD,
    KALSHI_LIVE_EMPIRICAL_LOOKBACK_DAYS,
    KALSHI_LIVE_EMPIRICAL_RANKING_ENABLED,
    KALSHI_LIVE_ENABLED,
    KALSHI_LIVE_MAX_DAILY_LOSS_USD,
    KALSHI_LIVE_MAX_BUY_TRADE_SIZE_USD,
    KALSHI_LIVE_MAX_EVENT_PACKAGES,
    KALSHI_LIVE_MAX_OPEN_EXPOSURE_USD,
    KALSHI_LIVE_MAX_POSITIONS,
    KALSHI_LIVE_MAX_SELL_TRADE_SIZE_USD,
    KALSHI_LIVE_MAX_TRADE_SIZE_USD,
    KALSHI_LIVE_MIN_CASH_BUFFER_USD,
    KALSHI_LIVE_NEXT_DAY_CAPITAL_PCT,
    KALSHI_LIVE_ORDER_SLIPPAGE_CENTS,
    KALSHI_LIVE_SCALE_TO_AVAILABLE_CASH,
    KALSHI_LIVE_SAME_DAY_TOP_UP_ENABLED,
    KALSHI_LIVE_SETTLEMENT_BUFFER_MINUTES,
    KALSHI_LIVE_TARGET_TOLERANCE_USD,
    KALSHI_PRIVATE_KEY_PATH,
    KALSHI_USE_DEMO,
    SCAN_INTERVAL_SECONDS,
)
from core.alerts import alert_bot_started, alert_error, alert_scan_summary, setup_logging
from core.database import (
    Trade,
    get_open_exposure_usd,
    get_realized_pnl_for_trading_day,
    get_trade_cost_for_trading_day,
    get_trade_count_for_trading_day,
    get_trade_stats,
    get_traded_buckets,
    get_unresolved_trades,
    log_trade,
    log_weather_comparison_snapshot,
    session_scope,
)
from core.execution.kalshi_client import KalshiClient, KalshiClientError
from core.strategy.edge import calc_fee_pct, calculate_edge
from core.strategy.signals import finalize_scan_comparisons, scan_all_markets

logger = logging.getLogger(__name__)

MAX_PER_EVENT = 3
TRADING_DAY_TIMEZONE = ZoneInfo("America/New_York")


def _to_float(value) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


class LiveTrader:
    """Executes real Kalshi orders within strict live risk limits."""

    def __init__(self, client: KalshiClient | None = None):
        if not KALSHI_LIVE_ENABLED:
            raise RuntimeError("KALSHI_LIVE_ENABLED must be true to run live trading")
        self.client = client or KalshiClient(
            api_key_id=KALSHI_API_KEY_ID,
            private_key_path=KALSHI_PRIVATE_KEY_PATH,
            use_demo=KALSHI_USE_DEMO,
        )
        self.visible_venue = "kalshi"
        self.last_decision_summary: dict = {}

    def _signal_label(self, signal: dict) -> str:
        return (
            f"{signal.get('city', '?')} {signal.get('target_date', '?')} | "
            f"{signal.get('bucket_question', 'unknown bucket')}"
        )

    def _signal_is_same_day(self, signal: dict) -> bool:
        return bool((signal.get("forecast_context") or {}).get("same_day_live"))

    def _signal_edge_bucket(self, signal: dict) -> str:
        edge_pct = abs(_to_float(signal.get("edge"))) * 100.0
        if edge_pct >= 30:
            return "30%+"
        if edge_pct >= 20:
            return "20-30%"
        if edge_pct >= 10:
            return "10-20%"
        if edge_pct >= 5:
            return "5-10%"
        return "<5%"

    def _record_skip(self, summary: dict, category: str, signal: dict | None = None, detail: str | None = None):
        summary.setdefault("skip_counts", Counter())
        summary.setdefault("skip_examples", {})
        summary["skip_counts"][category] += 1
        if signal is None:
            return
        examples = summary["skip_examples"].setdefault(category, [])
        if len(examples) >= 3:
            return
        label = self._signal_label(signal)
        examples.append(f"{label} | {detail}" if detail else label)

    def _log_decision_summary(self, summary: dict):
        skip_counts = summary.get("skip_counts", Counter())
        headline_parts = [
            f"scan_signals={summary.get('scan_signals', 0)}",
            f"kalshi_candidates={summary.get('kalshi_candidates', 0)}",
            f"planned={summary.get('planned_signals', 0)}",
            f"planned_usd={summary.get('planned_trade_size_usd', 0.0):.2f}",
            f"target_usd={summary.get('planned_target_budget_usd', 0.0):.2f}",
            f"deduped={skip_counts.get('duplicate_unresolved', 0)}",
            f"filled={summary.get('filled', 0)}",
        ]
        for category, count in sorted(skip_counts.items()):
            if category == "duplicate_unresolved":
                continue
            headline_parts.append(f"{category}={count}")
        logger.info("Kalshi live decision summary: %s", " | ".join(headline_parts))

        for category, examples in sorted(summary.get("skip_examples", {}).items()):
            for example in examples:
                logger.info("  %s: %s", category, example)

    def _live_bankroll_state(self, balance_snapshot) -> dict:
        today = datetime.now(TRADING_DAY_TIMEZONE).strftime("%Y-%m-%d")
        daily_pnl = get_realized_pnl_for_trading_day(today, mode="live", venue=self.visible_venue)
        trading_day_cost = get_trade_cost_for_trading_day(today, mode="live", venue=self.visible_venue)
        trading_day_positions = get_trade_count_for_trading_day(today, mode="live", venue=self.visible_venue)
        tracked_open_exposure = get_open_exposure_usd(mode="live", venue=self.visible_venue)
        unresolved = get_unresolved_trades(mode="live", venue=self.visible_venue)
        portfolio = self.client.get_portfolio_exposure()
        # Use the authenticated account snapshot as the live source of truth for open exposure.
        # The local DB can lag settlement and otherwise overstate unresolved cost.
        effective_open_exposure = max(portfolio.total_cost_usd, 0.0)
        effective_open_positions = max(portfolio.open_positions, 0)

        available_cash = max(balance_snapshot.available_cash_usd - KALSHI_LIVE_MIN_CASH_BUFFER_USD, 0.0)
        remaining_daily_loss = max(KALSHI_LIVE_MAX_DAILY_LOSS_USD + min(daily_pnl, 0.0), 0.0)
        target_budget_cap = self._target_budget_cap(available_cash)
        remaining_slice = self._remaining_slice_budget(available_cash, trading_day_cost)
        remaining_open_exposure = self._remaining_open_exposure_budget(effective_open_exposure)

        return {
            "today": today,
            "daily_pnl": daily_pnl,
            "trading_day_cost": trading_day_cost,
            "trading_day_positions": trading_day_positions,
            "open_exposure": effective_open_exposure,
            "tracked_open_exposure": tracked_open_exposure,
            "account_total_cost": portfolio.total_cost_usd,
            "account_market_value": portfolio.market_value_usd,
            "open_positions": effective_open_positions,
            "tracked_open_positions": len(unresolved),
            "account_open_positions": portfolio.open_positions,
            "total_open_positions": effective_open_positions,
            "available_cash": available_cash,
            "remaining_daily_loss": remaining_daily_loss,
            "target_budget_cap_usd": target_budget_cap,
            "remaining_slice": remaining_slice,
            "remaining_open_exposure": remaining_open_exposure,
        }

    def _target_budget_cap(self, available_cash: float) -> float:
        target_cap = min(available_cash, KALSHI_LIVE_BUDGET_ALLOCATION_USD)
        if KALSHI_LIVE_SCALE_TO_AVAILABLE_CASH:
            return max(target_cap, 0.0)
        return max(target_cap, 0.0)

    def _remaining_slice_budget(self, available_cash: float, trading_day_cost: float) -> float:
        budget_remaining = KALSHI_LIVE_BUDGET_ALLOCATION_USD - trading_day_cost
        return max(min(available_cash, budget_remaining), 0.0)

    def _remaining_open_exposure_budget(self, effective_open_exposure: float) -> float:
        return max(KALSHI_LIVE_MAX_OPEN_EXPOSURE_USD - effective_open_exposure, 0.0)

    def _load_empirical_live_segment_stats(self) -> dict:
        cutoff = (
            datetime.now(TRADING_DAY_TIMEZONE).date() - timedelta(days=KALSHI_LIVE_EMPIRICAL_LOOKBACK_DAYS)
        ).strftime("%Y-%m-%d")
        stats: dict[str, dict] = {
            "baseline": {"trades": 0, "stake": 0.0, "pnl": 0.0},
            "event_selection": {},
            "edge_bucket": {},
            "side": {},
            "city": {},
            "lead_bucket": {},
            "stance": {},
        }
        with session_scope() as session:
            rows = (
                session.query(
                    Trade.resolution_source,
                    Trade.forecast_context_json,
                    Trade.size_usd,
                    Trade.pnl,
                    Trade.edge,
                    Trade.side,
                    Trade.city,
                    Trade.is_contrarian,
                )
                .filter(
                    Trade.mode == "live",
                    Trade.venue == self.visible_venue,
                    Trade.resolved == True,
                    Trade.target_date >= cutoff,
                )
                .all()
            )

        def bucket(name: str, key: str):
            group = stats[name].setdefault(key, {"trades": 0, "stake": 0.0, "pnl": 0.0})
            return group

        for row in rows:
            if row.resolution_source == "manual_exit":
                continue
            context = json.loads(row.forecast_context_json or "{}")
            event_selection = context.get("event_selection", "none")
            if event_selection not in {"hedged", "same_day_sell_ladder"}:
                continue

            stake = _to_float(row.size_usd)
            pnl = _to_float(row.pnl)
            if stake <= 0:
                continue

            stats["baseline"]["trades"] += 1
            stats["baseline"]["stake"] += stake
            stats["baseline"]["pnl"] += pnl
            bucket("event_selection", event_selection)["trades"] += 1
            bucket("event_selection", event_selection)["stake"] += stake
            bucket("event_selection", event_selection)["pnl"] += pnl
            edge_bucket = self._signal_edge_bucket({"edge": row.edge})
            bucket("edge_bucket", edge_bucket)["trades"] += 1
            bucket("edge_bucket", edge_bucket)["stake"] += stake
            bucket("edge_bucket", edge_bucket)["pnl"] += pnl
            bucket("side", row.side or "BUY")["trades"] += 1
            bucket("side", row.side or "BUY")["stake"] += stake
            bucket("side", row.side or "BUY")["pnl"] += pnl
            bucket("city", row.city or "unknown")["trades"] += 1
            bucket("city", row.city or "unknown")["stake"] += stake
            bucket("city", row.city or "unknown")["pnl"] += pnl
            lead_bucket = context.get("forecast_lead_bucket", "unknown")
            bucket("lead_bucket", lead_bucket)["trades"] += 1
            bucket("lead_bucket", lead_bucket)["stake"] += stake
            bucket("lead_bucket", lead_bucket)["pnl"] += pnl
            stance = "contrarian" if row.is_contrarian else "consensus"
            bucket("stance", stance)["trades"] += 1
            bucket("stance", stance)["stake"] += stake
            bucket("stance", stance)["pnl"] += pnl
        return stats

    def _empirical_segment_multiplier(
        self,
        segment_row: dict | None,
        baseline_roi: float,
        *,
        min_trades: int,
        scale: float,
        low: float = 0.7,
        high: float = 1.3,
    ) -> float | None:
        if not segment_row or segment_row.get("trades", 0) < min_trades or segment_row.get("stake", 0.0) <= 0:
            return None
        roi = segment_row["pnl"] / segment_row["stake"]
        confidence = min(segment_row["trades"] / 12.0, 1.0)
        multiplier = 1.0 + (roi - baseline_roi) * scale * confidence
        return max(min(multiplier, high), low)

    def _apply_empirical_live_weights(self, signals: list[dict], decision_summary: dict) -> list[dict]:
        if not signals or not KALSHI_LIVE_EMPIRICAL_RANKING_ENABLED:
            return signals

        stats = self._load_empirical_live_segment_stats()
        baseline_stake = stats["baseline"]["stake"]
        if stats["baseline"]["trades"] < 12 or baseline_stake <= 0:
            decision_summary["empirical_ranking_reference_trades"] = stats["baseline"]["trades"]
            return signals

        baseline_roi = stats["baseline"]["pnl"] / baseline_stake
        decision_summary["empirical_ranking_reference_trades"] = stats["baseline"]["trades"]
        decision_summary["empirical_ranking_baseline_roi_pct"] = round(baseline_roi * 100.0, 2)

        weighted = []
        for signal in signals:
            context = signal.get("forecast_context") or {}
            event_selection = context.get("event_selection", "none")
            lead_bucket = context.get("forecast_lead_bucket", "unknown")
            multipliers = []

            for segment_name, segment_key, min_trades, scale in (
                ("event_selection", event_selection, 8, 1.0),
                ("edge_bucket", self._signal_edge_bucket(signal), 8, 1.0),
                ("side", signal.get("side", "BUY"), 10, 0.5),
                ("lead_bucket", lead_bucket, 6, 0.7),
                ("city", signal.get("city", "unknown"), 5, 0.6),
                ("stance", "contrarian" if signal.get("is_contrarian") else "consensus", 8, 0.4),
            ):
                multiplier = self._empirical_segment_multiplier(
                    stats[segment_name].get(segment_key),
                    baseline_roi,
                    min_trades=min_trades,
                    scale=scale,
                )
                if multiplier is not None:
                    multipliers.append(multiplier)

            if event_selection in {"single_bucket", "local_ladder", "none"}:
                multipliers.append(0.7)
            if lead_bucket in {"0-6h", "6-12h"}:
                multipliers.append(0.8)

            empirical_weight = 1.0
            for multiplier in multipliers:
                empirical_weight *= multiplier
            empirical_weight = round(max(min(empirical_weight, 1.75), 0.45), 4)
            updated = {**signal}
            updated["trade_size"] = round(max(_to_float(signal.get("trade_size")) * empirical_weight, 0.0), 2)
            updated["forecast_context"] = {
                **context,
                "live_empirical_weight": empirical_weight,
                "live_rank_score": round(abs(_to_float(signal.get("edge"))) * empirical_weight, 4),
            }
            weighted.append(updated)

        weighted.sort(
            key=lambda s: (
                _to_float((s.get("forecast_context") or {}).get("live_rank_score")),
                _to_float(s.get("trade_size")),
                abs(_to_float(s.get("edge"))),
            ),
            reverse=True,
        )
        return weighted

    def _should_skip_market(self, market: dict) -> str | None:
        close_raw = market.get("close_time") or market.get("expiration_time") or market.get("settlement_time")
        if not close_raw:
            return None
        try:
            close_at = datetime.fromisoformat(str(close_raw).replace("Z", "+00:00"))
        except ValueError:
            return None
        minutes_until_close = (close_at - datetime.now(timezone.utc)).total_seconds() / 60.0
        if minutes_until_close <= KALSHI_LIVE_SETTLEMENT_BUFFER_MINUTES:
            return (
                f"Market closes in {minutes_until_close:.0f}m which is inside the "
                f"{KALSHI_LIVE_SETTLEMENT_BUFFER_MINUTES}m live buffer"
            )
        return None

    def _refresh_signal_pricing(self, signal: dict) -> tuple[dict | None, str | None]:
        market = self.client.get_market(signal["venue_market_id"])
        skip_reason = self._should_skip_market(market)
        if skip_reason:
            return None, skip_reason

        yes_price = _to_float(market.get("yes_ask_dollars") or signal.get("yes_price"))
        no_price = _to_float(market.get("no_ask_dollars") or signal.get("no_price"))
        if yes_price <= 0 or no_price <= 0:
            return None, "Live market quotes are unavailable"

        expected_entry = _to_float(signal.get("entry_price"))
        live_entry = yes_price if signal.get("side") == "BUY" else no_price
        adverse_drift_cents = max(0.0, (live_entry - expected_entry) * 100.0)
        if adverse_drift_cents - KALSHI_LIVE_ALLOWED_DRIFT_CENTS > 1e-6:
            return None, (
                f"Quote drifted by {adverse_drift_cents:.1f}c which exceeds the "
                f"{KALSHI_LIVE_ALLOWED_DRIFT_CENTS}c live drift cap"
            )

        selected_prob = _to_float(signal.get("selected_prob"))
        fee_pct = calc_fee_pct(live_entry, venue=self.visible_venue)
        edge_value = calculate_edge(selected_prob, live_entry, fee_pct=fee_pct)
        signed_edge = edge_value if signal.get("side") == "BUY" else -edge_value
        if abs(signed_edge) < abs(signal.get("edge", 0)) and abs(signed_edge) < 0.08:
            return None, "Live edge no longer clears the minimum threshold"

        enriched = {
            **signal,
            "market_prob": yes_price,
            "yes_price": yes_price,
            "no_price": no_price,
            "entry_price": live_entry,
            "expected_entry_price": expected_entry,
            "fee_pct": fee_pct,
            "edge": signed_edge,
            "quote_drift_cents": round(adverse_drift_cents, 2),
            "forecast_context": {
                **(signal.get("forecast_context") or {}),
                "refreshed_yes_ask": yes_price,
                "refreshed_no_ask": no_price,
                "quote_drift_cents": round(adverse_drift_cents, 2),
            },
        }
        return enriched, None

    def _max_signal_size(self, signal: dict) -> float:
        if signal.get("side") == "SELL":
            base_cap = KALSHI_LIVE_MAX_SELL_TRADE_SIZE_USD
        elif signal.get("side") == "BUY":
            base_cap = KALSHI_LIVE_MAX_BUY_TRADE_SIZE_USD
        else:
            base_cap = KALSHI_LIVE_MAX_TRADE_SIZE_USD
        if signal.get("is_contrarian"):
            base_cap = min(base_cap, KALSHI_LIVE_CONTRARIAN_MAX_TRADE_SIZE_USD)
        return max(base_cap, 0.0)

    def _cycle_target_budget(self, state: dict) -> float:
        return max(
            min(
                state["available_cash"],
                state["remaining_slice"],
                state["remaining_open_exposure"],
            ),
            0.0,
        )

    def _should_allow_same_day_top_up(self, state: dict) -> bool:
        if not KALSHI_LIVE_SAME_DAY_TOP_UP_ENABLED:
            return False
        lower_bound = max(KALSHI_LIVE_BANKROLL_SLICE_USD - KALSHI_LIVE_TARGET_TOLERANCE_USD, 0.0)
        if state["trading_day_cost"] >= lower_bound:
            return False
        return self._cycle_target_budget(state) >= 1.0

    def _build_execution_plan(self, actionable: list[dict], state: dict, decision_summary: dict) -> list[dict]:
        planned = []
        event_counts = {}
        projected_open_positions = state["open_positions"]

        for signal in actionable:
            if projected_open_positions >= KALSHI_LIVE_MAX_POSITIONS:
                self._record_skip(
                    decision_summary,
                    "max_positions",
                    signal,
                    f"Reached live max positions ({KALSHI_LIVE_MAX_POSITIONS})",
                )
                break

            event_key = signal.get("event_id", "")
            if event_key not in event_counts and len(event_counts) >= KALSHI_LIVE_MAX_EVENT_PACKAGES:
                self._record_skip(
                    decision_summary,
                    "event_package_cap",
                    signal,
                    f"Reached live event-package cap ({KALSHI_LIVE_MAX_EVENT_PACKAGES})",
                )
                continue
            if event_counts.get(event_key, 0) >= MAX_PER_EVENT:
                self._record_skip(
                    decision_summary,
                    "per_event_cap",
                    signal,
                    f"Reached per-event cap ({MAX_PER_EVENT})",
                )
                continue

            planned.append(signal)
            event_counts[event_key] = event_counts.get(event_key, 0) + 1
            projected_open_positions += 1

        return planned

    def _allocate_target_trade_sizes(self, signals: list[dict], state: dict, target_budget: float | None = None) -> list[dict]:
        if not signals:
            return []

        target_budget = round(self._cycle_target_budget(state) if target_budget is None else target_budget, 2)
        if target_budget < 1.0:
            return signals

        base_sizes = [max(min(_to_float(signal.get("trade_size")), self._max_signal_size(signal)), 0.0) for signal in signals]
        total_base = round(sum(base_sizes), 2)
        if total_base <= 0 or total_base >= target_budget:
            return signals

        allocated = list(base_sizes)
        headroom = [max(round(self._max_signal_size(signal) - base_size, 2), 0.0) for signal, base_size in zip(signals, base_sizes)]
        remaining_extra = round(target_budget - total_base, 2)

        while remaining_extra >= 0.01:
            active_indexes = [idx for idx, room in enumerate(headroom) if room >= 0.01]
            if not active_indexes:
                break

            active_weight = sum(base_sizes[idx] for idx in active_indexes)
            if active_weight <= 0:
                per_signal_extra = round(remaining_extra / len(active_indexes), 2)
                weights = {idx: per_signal_extra for idx in active_indexes}
            else:
                weights = {
                    idx: round(remaining_extra * (base_sizes[idx] / active_weight), 2)
                    for idx in active_indexes
                }

            distributed = 0.0
            for idx in active_indexes:
                proposed_extra = max(weights[idx], 0.01)
                increment = min(headroom[idx], proposed_extra, remaining_extra)
                increment = round(increment, 2)
                if increment < 0.01:
                    continue
                allocated[idx] = round(allocated[idx] + increment, 2)
                headroom[idx] = round(headroom[idx] - increment, 2)
                remaining_extra = round(remaining_extra - increment, 2)
                distributed = round(distributed + increment, 2)
                if remaining_extra < 0.01:
                    break

            if distributed < 0.01:
                break

        scaled = []
        for signal, base_size, allocated_size in zip(signals, base_sizes, allocated):
            updated = {**signal}
            updated["trade_size"] = allocated_size
            updated["forecast_context"] = {
                **(signal.get("forecast_context") or {}),
                "base_trade_size": base_size,
                "target_trade_size": allocated_size,
                "cycle_target_budget_usd": target_budget,
                "cycle_target_scale_factor": round(allocated_size / base_size, 4) if base_size > 0 else 1.0,
            }
            scaled.append(updated)
        return scaled

    def _consume_planned_signal_budget(self, state: dict, signals: list[dict]) -> float:
        spent = round(sum(_to_float(signal.get("trade_size")) for signal in signals), 2)
        state["available_cash"] = max(state["available_cash"] - spent, 0.0)
        state["remaining_slice"] = max(state["remaining_slice"] - spent, 0.0)
        state["remaining_open_exposure"] = max(state["remaining_open_exposure"] - spent, 0.0)
        state["open_positions"] += len(signals)
        return spent

    def _capital_pool_targets(
        self,
        total_budget: float,
        *,
        has_next_day: bool,
        has_same_day: bool,
    ) -> tuple[float, float]:
        if not has_same_day:
            return round(total_budget, 2), 0.0
        if not has_next_day:
            return 0.0, round(total_budget, 2)
        if not KALSHI_LIVE_SCALE_TO_AVAILABLE_CASH or not KALSHI_LIVE_SAME_DAY_TOP_UP_ENABLED:
            return round(total_budget, 2), 0.0
        next_day_pct = max(min(KALSHI_LIVE_NEXT_DAY_CAPITAL_PCT, 1.0), 0.0)
        next_day_target = round(total_budget * next_day_pct, 2)
        same_day_target = round(max(total_budget - next_day_target, 0.0), 2)
        return next_day_target, same_day_target

    def _build_pooled_execution_plan(self, actionable: list[dict], state: dict, decision_summary: dict) -> list[dict]:
        if not actionable:
            return []

        prioritized = self._apply_empirical_live_weights(actionable, decision_summary)
        next_day_signals = [signal for signal in prioritized if not self._signal_is_same_day(signal)]
        same_day_signals = [signal for signal in prioritized if self._signal_is_same_day(signal)]
        total_budget = round(self._cycle_target_budget(state), 2)
        next_day_target, same_day_target = self._capital_pool_targets(
            total_budget,
            has_next_day=bool(next_day_signals),
            has_same_day=bool(same_day_signals),
        )

        decision_summary["next_day_pool_reserve_usd"] = next_day_target
        decision_summary["same_day_pool_reserve_usd"] = same_day_target
        decision_summary["next_day_pool_target_usd"] = next_day_target
        decision_summary["same_day_pool_target_usd"] = same_day_target

        planned: list[dict] = []
        planning_state = dict(state)

        if next_day_signals:
            next_day_plan = self._build_execution_plan(next_day_signals, planning_state, decision_summary)
            next_day_plan = self._allocate_target_trade_sizes(
                next_day_plan,
                planning_state,
                target_budget=min(next_day_target, self._cycle_target_budget(planning_state)),
            )
            decision_summary["next_day_planned_usd"] = self._consume_planned_signal_budget(planning_state, next_day_plan)
            planned.extend(next_day_plan)
        else:
            decision_summary["next_day_planned_usd"] = 0.0

        if same_day_signals:
            same_day_target = round(
                min(
                    same_day_target + max(next_day_target - decision_summary["next_day_planned_usd"], 0.0),
                    self._cycle_target_budget(planning_state),
                ),
                2,
            )
            decision_summary["same_day_pool_target_usd"] = same_day_target
            same_day_plan = self._build_execution_plan(same_day_signals, planning_state, decision_summary)
            same_day_plan = self._allocate_target_trade_sizes(
                same_day_plan,
                planning_state,
                target_budget=min(same_day_target, self._cycle_target_budget(planning_state)),
            )
            decision_summary["same_day_planned_usd"] = self._consume_planned_signal_budget(planning_state, same_day_plan)
            planned.extend(same_day_plan)
        else:
            decision_summary["same_day_planned_usd"] = 0.0

        return planned

    def _size_live_order(self, signal: dict, state: dict) -> dict | None:
        base_size = min(_to_float(signal.get("trade_size")), self._max_signal_size(signal))

        size_cap = min(
            base_size,
            state["available_cash"],
            state["remaining_slice"],
            state["remaining_open_exposure"],
        )
        if size_cap < 1.0:
            return None

        live_entry = _to_float(signal.get("entry_price"))
        count = int(math.floor(size_cap / max(live_entry, 0.01)))
        if count < 1:
            return None

        intended_cost = round(count * live_entry, 2)
        return {
            "count": count,
            "intended_size_usd": intended_cost,
            "live_limit_price": round(
                min(
                    live_entry + (KALSHI_LIVE_ORDER_SLIPPAGE_CENTS / 100.0),
                    _to_float(signal.get("expected_entry_price", live_entry))
                    + (KALSHI_LIVE_ALLOWED_DRIFT_CENTS / 100.0),
                ),
                4,
            ),
        }

    def _execute_live_trade(self, signal: dict, state: dict) -> tuple[dict | None, str | None]:
        refreshed_signal, skip_reason = self._refresh_signal_pricing(signal)
        if skip_reason:
            logger.info("Skipping live Kalshi trade: %s", skip_reason)
            return None, skip_reason

        sizing = self._size_live_order(refreshed_signal, state)
        if not sizing:
            logger.info("Skipping live Kalshi trade: insufficient live risk budget")
            return None, "Insufficient live risk budget"

        side = "yes" if refreshed_signal.get("side") == "BUY" else "no"
        client_order_id = str(uuid.uuid4())
        try:
            order = self.client.place_marketable_buy(
                ticker=refreshed_signal["venue_market_id"],
                side=side,
                count=sizing["count"],
                limit_price=sizing["live_limit_price"],
                client_order_id=client_order_id,
                max_cost_buffer_cents=KALSHI_LIVE_ORDER_SLIPPAGE_CENTS,
            )
        except KalshiClientError as exc:
            message = str(exc)
            if "fill_or_kill_insufficient_resting_volume" in message:
                return None, "No fill; insufficient resting volume for fill-or-kill order"
            raise
        latest_order = order.get("latest_order", order)
        fill_summary = self.client.summarize_fill(
            order=latest_order,
            fills=order.get("fills", []),
            side=side,
            expected_entry_price=_to_float(refreshed_signal.get("expected_entry_price", refreshed_signal["entry_price"])),
        )

        if fill_summary["filled_contracts"] <= 0:
            logger.info(
                "No live fill for %s (%s); status=%s",
                refreshed_signal.get("bucket_question", ""),
                refreshed_signal.get("venue_market_id"),
                fill_summary.get("order_status"),
            )
            return None, f"No fill; order status={fill_summary.get('order_status', 'unknown')}"

        fill_price = fill_summary["fill_price"] or refreshed_signal["entry_price"]
        filled_size_usd = fill_summary["filled_size_usd"] or round(fill_summary["filled_contracts"] * fill_price, 2)
        fee_usd = fill_summary.get("fee_usd", 0.0)

        executed = {
            **refreshed_signal,
            "client_order_id": client_order_id,
            "venue_order_id": latest_order.get("order_id"),
            "trade_size": filled_size_usd,
            "intended_size_usd": sizing["intended_size_usd"],
            "filled_size_usd": filled_size_usd,
            "filled_contracts": fill_summary["filled_contracts"],
            "entry_price": fill_price,
            "fill_price": fill_price,
            "expected_entry_price": refreshed_signal.get("expected_entry_price"),
            "fee_usd": fee_usd,
            "order_status": fill_summary.get("order_status"),
            "submitted_at": fill_summary.get("submitted_at"),
            "filled_at": fill_summary.get("filled_at"),
            "wallet_balance_snapshot": state["available_cash"],
            "forecast_context": {
                **(refreshed_signal.get("forecast_context") or {}),
                "intended_contracts": sizing["count"],
                "filled_contracts": fill_summary["filled_contracts"],
                "expected_entry_price": refreshed_signal.get("expected_entry_price"),
                "fill_price": fill_price,
                "fee_usd": fee_usd,
                "quote_drift_cents": refreshed_signal.get("quote_drift_cents"),
                "fill_drift_cents": fill_summary.get("adverse_drift_cents"),
            },
        }
        log_trade(executed, mode="live")
        logger.info(
            "LIVE [kalshi]: %s %s contracts=%s fill=%0.4f edge=%+.1f%%",
            "YES" if executed.get("side") == "BUY" else "NO",
            executed.get("bucket_question", "")[:48],
            executed["filled_contracts"],
            fill_price,
            executed.get("edge", 0.0) * 100,
        )
        return executed, None

    def run_scan_cycle(self) -> list[dict]:
        balance = self.client.get_balance()
        state = self._live_bankroll_state(balance)
        decision_summary = {
            "scan_signals": 0,
            "kalshi_candidates": 0,
            "filled": 0,
            "skip_counts": Counter(),
            "skip_examples": {},
        }

        if state["available_cash"] <= 0:
            logger.warning("Kalshi live cash buffer gate hit; skipping")
            decision_summary["skip_counts"]["cash_buffer_gate"] += 1
            self.last_decision_summary = decision_summary
            return []
        if state["remaining_daily_loss"] <= 0:
            logger.warning("Kalshi live daily loss gate hit; skipping")
            decision_summary["skip_counts"]["daily_loss_gate"] += 1
            self.last_decision_summary = decision_summary
            return []
        if state["remaining_slice"] <= 0 or state["remaining_open_exposure"] <= 0:
            logger.warning("Kalshi live bankroll/exposure gate hit; skipping")
            decision_summary["skip_counts"]["exposure_gate"] += 1
            self.last_decision_summary = decision_summary
            return []
        if state["open_positions"] >= KALSHI_LIVE_MAX_POSITIONS:
            logger.warning("Kalshi live open-position cap hit; skipping")
            decision_summary["skip_counts"]["max_positions_gate"] += 1
            self.last_decision_summary = decision_summary
            return []

        bankroll = {"kalshi": min(state["remaining_slice"], state["available_cash"])}
        daily_pnl = {"kalshi": state["daily_pnl"], "polymarket": 0.0}
        allow_same_day_live = self._should_allow_same_day_top_up(state)
        decision_summary["same_day_top_up_enabled"] = allow_same_day_live
        decision_summary["scale_to_available_cash"] = KALSHI_LIVE_SCALE_TO_AVAILABLE_CASH
        decision_summary["next_day_capital_pct"] = round(max(min(KALSHI_LIVE_NEXT_DAY_CAPITAL_PCT, 1.0), 0.0), 2)
        if KALSHI_LIVE_SCALE_TO_AVAILABLE_CASH:
            decision_summary["target_lower_bound_usd"] = round(
                max(state["target_budget_cap_usd"] - KALSHI_LIVE_TARGET_TOLERANCE_USD, 0.0),
                2,
            )
        else:
            decision_summary["target_lower_bound_usd"] = round(
                max(KALSHI_LIVE_BANKROLL_SLICE_USD - KALSHI_LIVE_TARGET_TOLERANCE_USD, 0.0),
                2,
            )

        try:
            scan_result = scan_all_markets(
                bankroll=bankroll,
                daily_pnl=daily_pnl,
                return_context=True,
                allow_same_day_live=allow_same_day_live,
            )
        except Exception as exc:
            logger.error("Live Kalshi scan failed: %s", exc, exc_info=True)
            alert_error(str(exc), context="Kalshi live scan")
            decision_summary["skip_counts"]["scan_failure"] += 1
            self.last_decision_summary = decision_summary
            return []

        decision_summary["scan_signals"] = len(scan_result.get("signals", []))
        signals = [
            s for s in scan_result.get("signals", [])
            if s.get("venue") == self.visible_venue and s.get("trade_size", 0) > 0
        ]
        decision_summary["kalshi_candidates"] = len(signals)
        comparison_rows = scan_result.get("comparisons", [])
        total_signals = len(signals)
        existing_buckets = get_traded_buckets(mode="live")
        actionable = []
        for signal in signals:
            bucket_key = (
                signal.get("venue", self.visible_venue),
                signal.get("city", ""),
                signal.get("target_date", ""),
                signal.get("bucket_question", ""),
            )
            if bucket_key in existing_buckets:
                self._record_skip(
                    decision_summary,
                    "duplicate_unresolved",
                    signal,
                    "Already have an unresolved live Kalshi trade on this bucket",
                )
                continue
            actionable.append(signal)

        planned_signals = self._build_pooled_execution_plan(actionable, state, decision_summary)
        decision_summary["planned_signals"] = len(planned_signals)
        decision_summary["planned_target_budget_usd"] = round(self._cycle_target_budget(state), 2)
        decision_summary["planned_trade_size_usd"] = round(
            sum(_to_float(signal.get("trade_size")) for signal in planned_signals), 2
        )

        executed = []
        abort_reason = None
        for signal in planned_signals:
            balance = self.client.get_balance()
            state = self._live_bankroll_state(balance)
            if state["available_cash"] <= 0:
                self._record_skip(
                    decision_summary,
                    "cash_buffer_gate",
                    signal,
                    "Authenticated Kalshi cash is below the live cash buffer",
                )
                break
            if state["remaining_slice"] <= 0 or state["remaining_open_exposure"] <= 0:
                self._record_skip(
                    decision_summary,
                    "account_exposure_gate",
                    signal,
                    (
                        f"Authenticated exposure is already ${state['open_exposure']:.2f}; "
                        "blocking additional live orders"
                    ),
                )
                break
            if state["open_positions"] >= KALSHI_LIVE_MAX_POSITIONS:
                self._record_skip(
                    decision_summary,
                    "max_positions",
                    signal,
                    (
                        f"Trading-day positions={state['open_positions']} "
                        f"which meets/exceeds the cap ({KALSHI_LIVE_MAX_POSITIONS})"
                    ),
                )
                break
            try:
                trade, skip_reason = self._execute_live_trade(signal, state)
            except KalshiClientError as exc:
                logger.error("Live Kalshi order failed: %s", exc)
                alert_error(str(exc), context="Kalshi live order placement")
                self._record_skip(decision_summary, "client_error", signal, str(exc))
                abort_reason = "Client error after order attempt; aborting remaining live cycle for safety"
                break
            if trade:
                executed.append(trade)
                decision_summary["filled"] += 1
                spent = (trade.get("filled_size_usd") or trade.get("trade_size") or 0.0) + (trade.get("fee_usd") or 0.0)
                state["available_cash"] = max(state["available_cash"] - spent, 0.0)
                state["remaining_slice"] = max(state["remaining_slice"] - spent, 0.0)
                state["remaining_open_exposure"] = max(state["remaining_open_exposure"] - spent, 0.0)
                state["open_positions"] += 1
            elif skip_reason:
                category = "pricing_or_market"
                if "risk budget" in skip_reason.lower():
                    category = "risk_budget"
                elif "drift" in skip_reason.lower():
                    category = "quote_drift"
                elif "edge" in skip_reason.lower():
                    category = "edge_decay"
                elif "close" in skip_reason.lower() or "buffer" in skip_reason.lower():
                    category = "settlement_buffer"
                elif "quotes are unavailable" in skip_reason.lower():
                    category = "missing_quote"
                elif "no fill" in skip_reason.lower():
                    category = "no_fill"
                self._record_skip(decision_summary, category, signal, skip_reason)

        if abort_reason:
            logger.warning(abort_reason)
            decision_summary["abort_reason"] = abort_reason

        finalized_comparisons = finalize_scan_comparisons(comparison_rows, executed=executed)
        for row in finalized_comparisons:
            log_weather_comparison_snapshot(row, mode="live")
        self.last_decision_summary = decision_summary
        self._log_decision_summary(decision_summary)
        if executed:
            alert_scan_summary(
                executed=executed,
                total_signals=total_signals,
                bankroll={"kalshi": balance.available_cash_usd},
                mode="live",
                comparison_rows=finalized_comparisons,
            )
        return executed

    def run_loop(self, interval: int = None):
        scan_interval = interval or SCAN_INTERVAL_SECONDS
        logger.info("Starting Kalshi live trading loop (scan every %ss)", scan_interval)
        alert_bot_started("live")
        while True:
            try:
                self.run_scan_cycle()
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                logger.error("Live trading loop error: %s", exc, exc_info=True)
                alert_error(str(exc), context="Kalshi live loop")
            finally:
                logger.info("Sleeping %ss before next live scan", scan_interval)
                import time
                time.sleep(scan_interval)


def main():
    setup_logging()
    trader = LiveTrader()
    trader.run_loop()
