"""
Paper trading execution engine.
Simulates trades against live Polymarket markets using real ensemble data,
but with a virtual bankroll. Logs everything to the database and sends
ONE consolidated Slack alert per scan cycle.
"""

import logging
import time
from datetime import datetime, timezone

from config.settings import (
    KALSHI_PAPER_BANKROLL,
    MAX_DAILY_LOSS,
    PRIMARY_VISIBLE_VENUE,
    POLYMARKET_PAPER_BANKROLL,
    SCAN_INTERVAL_SECONDS,
)
from core.strategy.signals import scan_all_markets, finalize_scan_comparisons
from core.database import (
    get_trade_stats,
    get_traded_buckets,
    log_trade,
    log_weather_comparison_snapshot,
)
from core.alerts import (
    alert_scan_summary,
    alert_daily_summary,
    alert_error,
    alert_bot_started,
    setup_logging,
)

logger = logging.getLogger(__name__)

# Limits
MAX_POSITIONS_PER_VENUE = 20
MAX_PER_EVENT = 3
MIN_BANKROLL = 50.0


class PaperTrader:
    def __init__(self, initial_bankroll: float = 1000.0, venue_bankrolls: dict | None = None):
        if venue_bankrolls is not None:
            self.bankrolls = dict(venue_bankrolls)
        else:
            self.bankrolls = {
                "polymarket": POLYMARKET_PAPER_BANKROLL if initial_bankroll == 1000.0 else initial_bankroll,
                "kalshi": KALSHI_PAPER_BANKROLL if initial_bankroll == 1000.0 else initial_bankroll,
            }
        self.initial_bankroll = dict(self.bankrolls)
        self.daily_pnl = 0.0
        self.daily_pnl_by_venue = {venue: 0.0 for venue in self.bankrolls}
        self.total_pnl = 0.0
        self.trades_today = 0
        self.positions = []
        self.last_scan_time = None
        self.current_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    def run_scan_cycle(self) -> list[dict]:
        """
        Run a single scan cycle. Places trades silently, then sends
        ONE consolidated Slack message at the end.
        """
        bankroll_text = ", ".join(
            f"{venue}=${amount:.2f}" for venue, amount in sorted(self.bankrolls.items())
        )
        logger.info(f"\n{'='*60}")
        logger.info(f"Paper scan — Bankrolls: {bankroll_text} | "
                     f"Positions: {len(self.positions)}")
        logger.info(f"{'='*60}")

        # Day rollover
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if today != self.current_date:
            self._handle_day_rollover()
            self.current_date = today

        viable_venues = [venue for venue, amount in self.bankrolls.items() if amount >= MIN_BANKROLL]
        if not viable_venues:
            logger.warning("All venue bankrolls are below minimum, skipping")
            return []

        if self.daily_pnl < -MAX_DAILY_LOSS:
            logger.warning(f"Daily loss limit hit (${self.daily_pnl:.2f}), skipping")
            return []

        # Scan
        try:
            scan_result = scan_all_markets(
                bankroll=self.bankrolls,
                daily_pnl=self.daily_pnl_by_venue,
                return_context=True,
            )
        except Exception as e:
            logger.error(f"Scan failed: {e}", exc_info=True)
            alert_error(str(e), context="Market scan cycle")
            return []

        signals = scan_result.get("signals", [])
        comparison_rows = scan_result.get("comparisons", [])

        if not signals:
            self._log_comparison_rows(finalize_scan_comparisons(comparison_rows, executed=[]))
            logger.info("No actionable signals found")
            return []

        actionable = [s for s in signals if s.get("trade_size", 0) > 0]
        if not actionable:
            self._log_comparison_rows(finalize_scan_comparisons(comparison_rows, executed=[]))
            logger.info(f"Found {len(signals)} signals but none met sizing criteria")
            return []

        total_signals = len(actionable)

        # Dedup: skip buckets we already have trades on (from DB)
        existing_buckets = get_traded_buckets(mode="paper")
        actionable = [
            s for s in actionable
            if (s.get("venue", "polymarket"), s.get("city", ""), s.get("target_date", ""), s.get("bucket_question", ""))
            not in existing_buckets
        ]
        deduped = total_signals - len(actionable)
        if deduped > 0:
            logger.info(f"Filtered out {deduped} duplicate signals (already in DB)")

        if not actionable:
            self._log_comparison_rows(finalize_scan_comparisons(comparison_rows, executed=[]))
            logger.info("All signals already have existing trades")
            return []

        # Execute trades (no per-trade Slack messages)
        executed = []
        event_counts = {}
        placed_this_cycle = set()  # Track within this scan cycle to prevent same-cycle dupes

        for signal in actionable:
            venue = signal.get("venue", "polymarket")
            venue_positions = sum(1 for p in self.positions if p.get("venue") == venue)
            if venue_positions >= MAX_POSITIONS_PER_VENUE:
                logger.info(f"Max positions reached for {venue} ({MAX_POSITIONS_PER_VENUE})")
                break

            size = signal.get("trade_size", 0)
            if size > self.bankrolls.get(venue, 0):
                continue

            # In-cycle dedup: skip if we already placed this exact bucket in this scan
            bucket_key = (
                venue,
                signal.get("city", ""),
                signal.get("target_date", ""),
                signal.get("bucket_question", ""),
            )
            if bucket_key in placed_this_cycle:
                logger.info(f"Skipping in-cycle duplicate: {venue} {signal.get('bucket_question', '')[:40]}")
                continue

            event_id = (venue, signal.get("event_id", "unknown"))
            event_counts[event_id] = event_counts.get(event_id, 0)
            if event_counts[event_id] >= MAX_PER_EVENT:
                continue

            if self._execute_paper_trade(signal):
                executed.append(signal)
                event_counts[event_id] += 1
                placed_this_cycle.add(bucket_key)

        # Send ONE consolidated Slack message for all trades
        finalized_comparisons = finalize_scan_comparisons(comparison_rows, executed=executed)
        self._log_comparison_rows(finalized_comparisons)
        if executed:
            executed_by_venue = {}
            for signal in executed:
                venue = signal.get("venue", "polymarket")
                executed_by_venue[venue] = executed_by_venue.get(venue, 0) + 1
            visible_count = executed_by_venue.get(PRIMARY_VISIBLE_VENUE, 0)
            logger.info(
                "Executed trades by venue: %s | visible in Slack: %s %s",
                ", ".join(f"{venue}={count}" for venue, count in sorted(executed_by_venue.items())),
                visible_count,
                PRIMARY_VISIBLE_VENUE,
            )
            alert_scan_summary(
                executed=executed,
                total_signals=total_signals,
                bankroll=self.bankrolls,
                mode="paper",
                comparison_rows=finalized_comparisons,
            )

        logger.info(f"Executed {len(executed)} paper trades out of {total_signals} signals")
        return executed

    def _log_comparison_rows(self, comparison_rows: list[dict]):
        """Persist city/date comparison snapshots without breaking the scan loop."""
        for row in comparison_rows:
            try:
                log_weather_comparison_snapshot(row, mode="paper")
            except Exception as exc:
                logger.error(f"Failed to log weather comparison snapshot: {exc}", exc_info=True)

    def _execute_paper_trade(self, signal: dict) -> bool:
        """Execute a paper trade: deduct bankroll, log to DB. No Slack here."""
        size = signal.get("trade_size", 0)
        if size <= 0:
            return False

        venue = signal.get("venue", "polymarket")
        yes_no = "YES" if signal.get("side") == "BUY" else "NO"
        position = {
            "venue": venue,
            "event_id": signal.get("event_id"),
            "event_title": signal.get("event_title", ""),
            "bucket_question": signal.get("bucket_question", ""),
            "side": signal.get("side"),
            "yes_no": yes_no,
            "size": size,
            "entry_price": signal.get("entry_price", signal.get("market_prob", 0)),
            "target_date": signal.get("target_date"),
        }

        try:
            self.bankrolls[venue] = self.bankrolls.get(venue, 0.0) - size
            self.trades_today += 1
            self.positions.append(position)
            log_trade(signal, mode="paper")
        except Exception as e:
            logger.error(f"Failed to log trade: {e}")
            self.bankrolls[venue] = self.bankrolls.get(venue, 0.0) + size
            self.trades_today = max(self.trades_today - 1, 0)
            if self.positions and self.positions[-1] == position:
                self.positions.pop()
            return False

        logger.info(
            f"PAPER [{venue}]: ${size:.2f} on {yes_no} | "
            f"{signal.get('bucket_question', '')[:50]} | "
            f"Edge: {signal.get('edge', 0)*100:+.1f}%"
        )
        return True

    def _handle_day_rollover(self):
        logger.info(f"Day rollover: {self.current_date}")
        stats = get_trade_stats(mode="paper", venue=PRIMARY_VISIBLE_VENUE)
        stats["date"] = self.current_date
        stats["trades_executed"] = self.trades_today
        stats["daily_pnl"] = self.daily_pnl_by_venue.get(PRIMARY_VISIBLE_VENUE, 0.0)
        stats["resolved_today"] = 0
        alert_daily_summary(stats)
        self.daily_pnl = 0.0
        self.daily_pnl_by_venue = {venue: 0.0 for venue in self.daily_pnl_by_venue}
        self.trades_today = 0

    def run_loop(self, interval: int = None):
        scan_interval = interval or SCAN_INTERVAL_SECONDS
        logger.info(f"Starting paper trading loop (scan every {scan_interval}s)")
        alert_bot_started("paper")
        while True:
            try:
                self.run_scan_cycle()
                self.last_scan_time = datetime.now(timezone.utc)
            except KeyboardInterrupt:
                logger.info("Stopped by user")
                break
            except Exception as e:
                logger.error(f"Error in scan cycle: {e}", exc_info=True)
                alert_error(str(e), context="Paper trading loop")
            logger.info(f"Next scan in {scan_interval}s...")
            time.sleep(scan_interval)

    def get_status(self) -> dict:
        return {
            "mode": "paper",
            "bankroll": sum(self.bankrolls.values()),
            "bankrolls": dict(self.bankrolls),
            "initial_bankroll": self.initial_bankroll,
            "daily_pnl": self.daily_pnl,
            "total_pnl": self.total_pnl,
            "trades_today": self.trades_today,
            "open_positions": len(self.positions),
            "last_scan": self.last_scan_time.isoformat() if self.last_scan_time else None,
        }
