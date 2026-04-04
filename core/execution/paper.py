"""
Paper trading execution engine.
Simulates trades against live Polymarket markets using real ensemble data,
but with a virtual bankroll. Logs everything to the database and sends
ONE consolidated Slack alert per scan cycle.
"""

import logging
import time
from datetime import datetime, timezone

from config.settings import TRADING_MODE, SCAN_INTERVAL_SECONDS, MAX_TRADE_SIZE, MAX_DAILY_LOSS
from core.strategy.signals import scan_all_markets
from core.database import log_trade, get_trade_stats, get_traded_buckets
from core.alerts import (
    alert_scan_summary,
    alert_daily_summary,
    alert_error,
    alert_bot_started,
    setup_logging,
)

logger = logging.getLogger(__name__)

# Limits
MAX_POSITIONS = 20
MAX_PER_EVENT = 3
MIN_BANKROLL = 50.0


class PaperTrader:
    def __init__(self, initial_bankroll: float = 1000.0):
        self.bankroll = initial_bankroll
        self.initial_bankroll = initial_bankroll
        self.daily_pnl = 0.0
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
        logger.info(f"\n{'='*60}")
        logger.info(f"Paper scan — Bankroll: ${self.bankroll:.2f} | "
                     f"Positions: {len(self.positions)}/{MAX_POSITIONS}")
        logger.info(f"{'='*60}")

        # Day rollover
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if today != self.current_date:
            self._handle_day_rollover()
            self.current_date = today

        if self.bankroll < MIN_BANKROLL:
            logger.warning(f"Bankroll too low (${self.bankroll:.2f}), skipping")
            return []

        if self.daily_pnl < -MAX_DAILY_LOSS:
            logger.warning(f"Daily loss limit hit (${self.daily_pnl:.2f}), skipping")
            return []

        # Scan
        try:
            signals = scan_all_markets(
                bankroll=self.bankroll,
                daily_pnl=self.daily_pnl,
            )
        except Exception as e:
            logger.error(f"Scan failed: {e}", exc_info=True)
            alert_error(str(e), context="Market scan cycle")
            return []

        if not signals:
            logger.info("No actionable signals found")
            return []

        actionable = [s for s in signals if s.get("trade_size", 0) > 0]
        if not actionable:
            logger.info(f"Found {len(signals)} signals but none met sizing criteria")
            return []

        total_signals = len(actionable)

        # Dedup: skip buckets we already have trades on (from DB)
        existing_buckets = get_traded_buckets(mode="paper")
        actionable = [
            s for s in actionable
            if (s.get("city", ""), s.get("target_date", ""), s.get("bucket_question", ""))
            not in existing_buckets
        ]
        deduped = total_signals - len(actionable)
        if deduped > 0:
            logger.info(f"Filtered out {deduped} duplicate signals (already in DB)")

        if not actionable:
            logger.info("All signals already have existing trades")
            return []

        # Execute trades (no per-trade Slack messages)
        executed = []
        event_counts = {}
        placed_this_cycle = set()  # Track within this scan cycle to prevent same-cycle dupes

        for signal in actionable:
            if len(self.positions) >= MAX_POSITIONS:
                logger.info(f"Max positions reached ({MAX_POSITIONS})")
                break

            size = signal.get("trade_size", 0)
            if size > self.bankroll:
                continue

            # In-cycle dedup: skip if we already placed this exact bucket in this scan
            bucket_key = (signal.get("city", ""), signal.get("target_date", ""), signal.get("bucket_question", ""))
            if bucket_key in placed_this_cycle:
                logger.info(f"Skipping in-cycle duplicate: {bucket_key[0]} {bucket_key[2][:40]}")
                continue

            event_id = signal.get("event_id", "unknown")
            event_counts[event_id] = event_counts.get(event_id, 0)
            if event_counts[event_id] >= MAX_PER_EVENT:
                continue

            if self._execute_paper_trade(signal):
                executed.append(signal)
                event_counts[event_id] += 1
                placed_this_cycle.add(bucket_key)

        # Send ONE consolidated Slack message for all trades
        if executed:
            alert_scan_summary(
                executed=executed,
                total_signals=total_signals,
                bankroll=self.bankroll,
                mode="paper",
            )

        logger.info(f"Executed {len(executed)} paper trades out of {total_signals} signals")
        return executed

    def _execute_paper_trade(self, signal: dict) -> bool:
        """Execute a paper trade: deduct bankroll, log to DB. No Slack here."""
        size = signal.get("trade_size", 0)
        if size <= 0:
            return False

        self.bankroll -= size
        self.trades_today += 1

        yes_no = "YES" if signal.get("side") == "BUY" else "NO"

        self.positions.append({
            "event_id": signal.get("event_id"),
            "event_title": signal.get("event_title", ""),
            "bucket_question": signal.get("bucket_question", ""),
            "side": signal.get("side"),
            "yes_no": yes_no,
            "size": size,
            "entry_price": signal.get("market_prob", 0),
            "target_date": signal.get("target_date"),
        })

        try:
            log_trade(signal, mode="paper")
        except Exception as e:
            logger.error(f"Failed to log trade: {e}")

        logger.info(
            f"PAPER: ${size:.2f} on {yes_no} | "
            f"{signal.get('bucket_question', '')[:50]} | "
            f"Edge: {signal.get('edge', 0)*100:+.1f}%"
        )
        return True

    def _handle_day_rollover(self):
        logger.info(f"Day rollover: {self.current_date}")
        stats = get_trade_stats(mode="paper")
        stats["date"] = self.current_date
        stats["trades_executed"] = self.trades_today
        stats["daily_pnl"] = self.daily_pnl
        stats["resolved_today"] = 0
        alert_daily_summary(stats)
        self.daily_pnl = 0.0
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
            "bankroll": self.bankroll,
            "initial_bankroll": self.initial_bankroll,
            "daily_pnl": self.daily_pnl,
            "total_pnl": self.total_pnl,
            "trades_today": self.trades_today,
            "open_positions": len(self.positions),
            "last_scan": self.last_scan_time.isoformat() if self.last_scan_time else None,
        }
