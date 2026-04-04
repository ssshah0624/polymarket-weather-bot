"""
SQLite database for trade logs, market snapshots, and backtest data.
"""

import logging
from datetime import datetime, timezone
from pathlib import Path
from contextlib import contextmanager

from sqlalchemy import (
    create_engine, Column, Integer, Float, String, Boolean, DateTime, Text,
    Index,
)
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.pool import StaticPool

from config.settings import DB_PATH

logger = logging.getLogger(__name__)

Base = declarative_base()


# ============================================================
# Models
# ============================================================

class Trade(Base):
    """Record of every trade (paper or live)."""
    __tablename__ = "trades"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    mode = Column(String(10))  # paper | live
    event_title = Column(String(200))
    event_id = Column(String(50))
    city = Column(String(50))
    target_date = Column(String(10))
    bucket_question = Column(String(300))
    market_id = Column(String(100))
    token_id = Column(String(100))
    side = Column(String(4))  # BUY | SELL
    size_usd = Column(Float)
    price = Column(Float)  # market price at time of trade
    ensemble_prob = Column(Float)
    edge = Column(Float)
    kelly_pct = Column(Float)
    signal = Column(String(20))
    fee_usd = Column(Float, default=0.0)  # estimated fee
    # Resolution
    resolved = Column(Boolean, default=False)
    outcome = Column(String(10))  # win | loss | push
    pnl = Column(Float, default=0.0)
    resolution_price = Column(Float)
    actual_temp = Column(Float)  # actual observed temperature

    __table_args__ = (
        Index("ix_trades_date", "target_date"),
        Index("ix_trades_city", "city"),
        Index("ix_trades_mode", "mode"),
    )


class MarketSnapshot(Base):
    """Point-in-time snapshot of a market's pricing for backtesting."""
    __tablename__ = "market_snapshots"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    event_title = Column(String(200))
    city = Column(String(50))
    target_date = Column(String(10))
    bucket_question = Column(String(300))
    market_prob = Column(Float)
    ensemble_prob = Column(Float)
    ensemble_mean = Column(Float)
    ensemble_spread = Column(Float)
    ensemble_members = Column(Integer)
    nws_temp = Column(Float)
    edge = Column(Float)
    signal = Column(String(20))
    # Actual outcome (filled after resolution)
    actual_temp = Column(Float)
    bucket_hit = Column(Boolean)

    __table_args__ = (
        Index("ix_snapshots_date", "target_date"),
        Index("ix_snapshots_city", "city"),
    )


class DailySummary(Base):
    """Daily performance summary."""
    __tablename__ = "daily_summaries"

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(String(10), unique=True)
    mode = Column(String(10))
    markets_scanned = Column(Integer, default=0)
    edges_found = Column(Integer, default=0)
    trades_executed = Column(Integer, default=0)
    wins = Column(Integer, default=0)
    losses = Column(Integer, default=0)
    daily_pnl = Column(Float, default=0.0)
    total_pnl = Column(Float, default=0.0)


# ============================================================
# Database Session Management
# ============================================================

_engine = None
_Session = None


def get_engine():
    global _engine
    if _engine is None:
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        _engine = create_engine(
            f"sqlite:///{DB_PATH}",
            echo=False,
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        Base.metadata.create_all(_engine)
        logger.info(f"Database initialized at {DB_PATH}")
    return _engine


def get_session():
    global _Session
    if _Session is None:
        _Session = sessionmaker(bind=get_engine())
    return _Session()


@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    session = get_session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


# ============================================================
# Trade Operations
# ============================================================

def log_trade(signal: dict, mode: str = "paper") -> Trade:
    """Log a trade to the database."""
    from core.alerts import calc_fee_usd
    fee = calc_fee_usd(signal.get("trade_size", 0), signal.get("market_prob", 0))

    with session_scope() as session:
        trade = Trade(
            mode=mode,
            event_title=signal.get("event_title", ""),
            event_id=str(signal.get("event_id", "")),
            city=signal.get("city", ""),
            target_date=signal.get("target_date", ""),
            bucket_question=signal.get("bucket_question", ""),
            market_id=signal.get("market_id", ""),
            token_id=signal.get("yes_token_id") if signal.get("side") == "BUY" else signal.get("no_token_id"),
            side=signal.get("side", ""),
            size_usd=signal.get("trade_size", 0),
            price=signal.get("market_prob", 0),
            ensemble_prob=signal.get("ensemble_prob", 0),
            edge=signal.get("edge", 0),
            kelly_pct=signal.get("kelly_pct", 0),
            signal=signal.get("signal", ""),
            fee_usd=fee,
        )
        session.add(trade)
        session.flush()
        logger.info(f"Trade logged: {trade.side} ${trade.size_usd:.2f} on {trade.bucket_question[:50]}")
        return trade


def has_existing_trade(city: str, target_date: str, bucket_question: str, mode: str = "paper") -> bool:
    """
    Check if we already have an unresolved trade on this exact bucket.
    Prevents duplicate bets on the same outcome across scan cycles.
    """
    with session_scope() as session:
        existing = session.query(Trade).filter_by(
            city=city,
            target_date=target_date,
            bucket_question=bucket_question,
            mode=mode,
            resolved=False,
        ).first()
        return existing is not None


def get_traded_buckets(mode: str = "paper") -> set:
    """
    Get a set of (city, target_date, bucket_question) tuples for all
    unresolved trades. Used for fast dedup checking.
    """
    with session_scope() as session:
        trades = session.query(
            Trade.city, Trade.target_date, Trade.bucket_question
        ).filter_by(mode=mode, resolved=False).all()
        return {(t.city, t.target_date, t.bucket_question) for t in trades}


def resolve_trade(trade_id: int, outcome: str, pnl: float,
                  resolution_price: float = 0.0, actual_temp: float = None):
    """Resolve a trade with its outcome."""
    with session_scope() as session:
        trade = session.query(Trade).filter_by(id=trade_id).first()
        if trade:
            trade.resolved = True
            trade.outcome = outcome
            trade.pnl = pnl
            trade.resolution_price = resolution_price
            if actual_temp is not None:
                trade.actual_temp = actual_temp


def get_unresolved_trades(mode: str = None) -> list[dict]:
    """Get all unresolved trades as dicts."""
    with session_scope() as session:
        q = session.query(Trade).filter_by(resolved=False)
        if mode:
            q = q.filter_by(mode=mode)
        return [
            {
                "id": t.id, "event_title": t.event_title, "city": t.city,
                "target_date": t.target_date, "bucket_question": t.bucket_question,
                "side": t.side, "size_usd": t.size_usd, "price": t.price,
                "ensemble_prob": t.ensemble_prob, "edge": t.edge,
                "fee_usd": t.fee_usd or 0.0,
            }
            for t in q.all()
        ]


def get_daily_pnl(date: str = None, mode: str = "paper") -> float:
    """Get total P&L for a specific date."""
    if date is None:
        date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    with session_scope() as session:
        trades = session.query(Trade).filter_by(target_date=date, mode=mode, resolved=True).all()
        return sum(t.pnl for t in trades)


def get_total_pnl(mode: str = "paper") -> float:
    """Get all-time P&L."""
    with session_scope() as session:
        trades = session.query(Trade).filter_by(mode=mode, resolved=True).all()
        return sum(t.pnl for t in trades)


def get_trade_stats(mode: str = "paper") -> dict:
    """Get aggregate trade statistics."""
    with session_scope() as session:
        all_trades = session.query(Trade).filter_by(mode=mode, resolved=True).all()
        wins = [t for t in all_trades if t.outcome == "win"]
        losses = [t for t in all_trades if t.outcome == "loss"]
        pending = session.query(Trade).filter_by(mode=mode, resolved=False).count()

        total = len(all_trades)
        total_fees = sum(t.fee_usd or 0 for t in all_trades)

        # P&L by side
        yes_trades = [t for t in all_trades if t.side == "BUY"]
        no_trades = [t for t in all_trades if t.side == "SELL"]
        yes_pnl = sum(t.pnl for t in yes_trades)
        no_pnl = sum(t.pnl for t in no_trades)

        # Best/worst cities
        city_pnl = {}
        for t in all_trades:
            city_pnl[t.city] = city_pnl.get(t.city, 0) + t.pnl
        sorted_cities = sorted(city_pnl.items(), key=lambda x: x[1], reverse=True)
        best_cities = [c.replace("_", " ").title() for c, p in sorted_cities if p > 0][:3]
        worst_cities = [c.replace("_", " ").title() for c, p in sorted_cities if p < 0][-3:]

        return {
            "total_trades": total,
            "wins": len(wins),
            "losses": len(losses),
            "win_rate": len(wins) / total if total > 0 else 0,
            "total_pnl": sum(t.pnl for t in all_trades),
            "avg_pnl": sum(t.pnl for t in all_trades) / total if total > 0 else 0,
            "avg_edge": sum(abs(t.edge) for t in all_trades) / total if total > 0 else 0,
            "total_fees": total_fees,
            "yes_pnl": yes_pnl,
            "no_pnl": no_pnl,
            "best_cities": best_cities,
            "worst_cities": worst_cities,
            "pending_trades": pending,
        }


# ============================================================
# Snapshot Operations
# ============================================================

def log_snapshot(signal: dict):
    """Log a market snapshot for backtesting data collection."""
    with session_scope() as session:
        meta = signal.get("ensemble_meta", {})
        nws = signal.get("nws_forecast") or {}
        snap = MarketSnapshot(
            event_title=signal.get("event_title", ""),
            city=signal.get("city", ""),
            target_date=signal.get("target_date", ""),
            bucket_question=signal.get("bucket_question", ""),
            market_prob=signal.get("market_prob", 0),
            ensemble_prob=signal.get("ensemble_prob", 0),
            ensemble_mean=meta.get("mean"),
            ensemble_spread=meta.get("spread"),
            ensemble_members=meta.get("member_count"),
            nws_temp=nws.get("temp"),
            edge=signal.get("edge", 0),
            signal=signal.get("signal", ""),
        )
        session.add(snap)
