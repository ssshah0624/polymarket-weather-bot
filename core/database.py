"""
SQLite database for trade logs, market snapshots, and backtest data.
"""

import logging
import json
from datetime import date, datetime, timezone
from pathlib import Path
from contextlib import contextmanager
from zoneinfo import ZoneInfo

from sqlalchemy import (
    create_engine, Column, Integer, Float, String, Boolean, DateTime, Text,
    Index, inspect, text,
)
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.pool import StaticPool

from config.settings import DB_PATH

logger = logging.getLogger(__name__)

Base = declarative_base()
TRADING_DAY_TIMEZONE = ZoneInfo("America/New_York")


# ============================================================
# Models
# ============================================================

class Trade(Base):
    """Record of every trade (paper or live)."""
    __tablename__ = "trades"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    venue = Column(String(30), default="polymarket")
    mode = Column(String(10))  # paper | live
    event_title = Column(String(200))
    event_id = Column(String(50))
    venue_event_id = Column(String(100))
    city = Column(String(50))
    target_date = Column(String(10))
    bucket_question = Column(String(300))
    market_id = Column(String(100))
    venue_market_id = Column(String(100))
    token_id = Column(String(100))
    client_order_id = Column(String(100))
    venue_order_id = Column(String(100))
    side = Column(String(4))  # BUY | SELL
    size_usd = Column(Float)
    intended_size_usd = Column(Float)
    filled_size_usd = Column(Float)
    filled_contracts = Column(Integer)
    price = Column(Float)  # market price at time of trade
    entry_price = Column(Float)  # all-in entry price for the selected side
    expected_entry_price = Column(Float)
    fill_price = Column(Float)
    order_status = Column(String(30))
    submitted_at = Column(DateTime)
    filled_at = Column(DateTime)
    rejected_reason = Column(Text)
    wallet_balance_snapshot = Column(Float)
    ensemble_prob = Column(Float)
    edge = Column(Float)
    kelly_pct = Column(Float)
    signal = Column(String(20))
    fee_usd = Column(Float, default=0.0)  # estimated fee
    is_contrarian = Column(Boolean, default=False)
    strategy_version = Column(String(50))
    model_expected_high = Column(Float)
    model_spread = Column(Float)
    venue_implied_high = Column(Float)
    settlement_station = Column(String(120))
    forecast_context_json = Column(Text)
    # Resolution
    resolved = Column(Boolean, default=False)
    resolved_at = Column(DateTime)
    outcome = Column(String(10))  # win | loss | push
    pnl = Column(Float, default=0.0)
    resolution_price = Column(Float)
    actual_temp = Column(Float)  # actual observed temperature
    resolution_source = Column(String(80))

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
    venue = Column(String(30), default="polymarket")
    event_title = Column(String(200))
    venue_event_id = Column(String(100))
    venue_market_id = Column(String(100))
    city = Column(String(50))
    target_date = Column(String(10))
    bucket_question = Column(String(300))
    market_prob = Column(Float)
    yes_price = Column(Float)
    no_price = Column(Float)
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


class WeatherComparisonSnapshot(Base):
    """City/date comparison snapshot used for explainable scan reviews."""
    __tablename__ = "weather_comparison_snapshots"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    mode = Column(String(10), default="paper")
    strategy_version = Column(String(50))
    city = Column(String(50))
    target_date = Column(String(10))
    model_expected_high = Column(Float)
    model_spread = Column(Float)
    polymarket_implied_high = Column(Float)
    kalshi_implied_high = Column(Float)
    model_summary_json = Column(Text)
    venue_availability_json = Column(Text)
    candidate_bets_json = Column(Text)
    selected_bets_json = Column(Text)
    skip_reasons_json = Column(Text)

    __table_args__ = (
        Index("ix_weather_comparison_city", "city"),
        Index("ix_weather_comparison_date", "target_date"),
        Index("ix_weather_comparison_mode", "mode"),
    )


# ============================================================
# Database Session Management
# ============================================================

_engine = None
_Session = None


def _run_schema_migrations(engine):
    """Additive sqlite migrations for new columns used by multi-venue support."""
    inspector = inspect(engine)
    table_names = set(inspector.get_table_names())

    trade_columns = {
        "venue": "VARCHAR(30) DEFAULT 'polymarket'",
        "venue_event_id": "VARCHAR(100)",
        "venue_market_id": "VARCHAR(100)",
        "client_order_id": "VARCHAR(100)",
        "venue_order_id": "VARCHAR(100)",
        "entry_price": "FLOAT",
        "intended_size_usd": "FLOAT",
        "filled_size_usd": "FLOAT",
        "filled_contracts": "INTEGER",
        "expected_entry_price": "FLOAT",
        "fill_price": "FLOAT",
        "order_status": "VARCHAR(30)",
        "submitted_at": "DATETIME",
        "filled_at": "DATETIME",
        "rejected_reason": "TEXT",
        "wallet_balance_snapshot": "FLOAT",
        "fee_usd": "FLOAT DEFAULT 0.0",
        "resolution_price": "FLOAT",
        "actual_temp": "FLOAT",
        "resolved_at": "DATETIME",
        "is_contrarian": "BOOLEAN DEFAULT 0",
        "strategy_version": "VARCHAR(50)",
        "model_expected_high": "FLOAT",
        "model_spread": "FLOAT",
        "venue_implied_high": "FLOAT",
        "settlement_station": "VARCHAR(120)",
        "forecast_context_json": "TEXT",
        "resolution_source": "VARCHAR(80)",
    }
    snapshot_columns = {
        "venue": "VARCHAR(30) DEFAULT 'polymarket'",
        "venue_event_id": "VARCHAR(100)",
        "venue_market_id": "VARCHAR(100)",
        "yes_price": "FLOAT",
        "no_price": "FLOAT",
    }

    with engine.begin() as conn:
        if "trades" in table_names:
            existing = {col["name"] for col in inspector.get_columns("trades")}
            for column_name, column_type in trade_columns.items():
                if column_name not in existing:
                    conn.execute(text(f"ALTER TABLE trades ADD COLUMN {column_name} {column_type}"))
        if "market_snapshots" in table_names:
            existing = {col["name"] for col in inspector.get_columns("market_snapshots")}
            for column_name, column_type in snapshot_columns.items():
                if column_name not in existing:
                    conn.execute(text(f"ALTER TABLE market_snapshots ADD COLUMN {column_name} {column_type}"))
        if "weather_comparison_snapshots" not in table_names:
            WeatherComparisonSnapshot.__table__.create(bind=conn)


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
        _run_schema_migrations(_engine)
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
    fee = signal.get("fee_usd")
    if fee is None:
        fee = signal.get("trade_size", 0) * signal.get("fee_pct", 0.0)
    ensemble_meta = signal.get("ensemble_meta") or {}
    nws_forecast = signal.get("nws_forecast") or {}
    forecast_context = signal.get("forecast_context") or {
        "selected_prob": signal.get("selected_prob"),
        "market_prob": signal.get("market_prob"),
        "entry_price": signal.get("entry_price"),
        "yes_price": signal.get("yes_price"),
        "no_price": signal.get("no_price"),
        "yes_edge": signal.get("yes_edge"),
        "no_edge": signal.get("no_edge"),
        "ensemble_mean": ensemble_meta.get("mean"),
        "ensemble_spread": ensemble_meta.get("spread"),
        "ensemble_min": ensemble_meta.get("min"),
        "ensemble_max": ensemble_meta.get("max"),
        "ensemble_members": ensemble_meta.get("member_count"),
        "nws_temp": nws_forecast.get("temp"),
        "nws_unit": nws_forecast.get("unit"),
        "nws_short_forecast": nws_forecast.get("short_forecast"),
        "nws_hourly_max_temp": nws_forecast.get("hourly_max_temp"),
        "nws_hourly_max_hour": nws_forecast.get("hourly_max_hour"),
        "nws_hourly_source": nws_forecast.get("hourly_source"),
        "settlement_station": signal.get("settlement_station"),
        "settlement_station_code": signal.get("settlement_station_code"),
        "settlement_station_source": signal.get("settlement_station_source"),
        "forecast_lat": signal.get("forecast_lat"),
        "forecast_lon": signal.get("forecast_lon"),
        "forecast_generated_at": signal.get("forecast_generated_at"),
    }
    forecast_context = {k: v for k, v in forecast_context.items() if v is not None}

    with session_scope() as session:
        trade_kwargs = {
            "venue": signal.get("venue", "polymarket"),
            "mode": mode,
            "event_title": signal.get("event_title", ""),
            "event_id": str(signal.get("event_id", "")),
            "venue_event_id": str(signal.get("venue_event_id", signal.get("event_id", ""))),
            "city": signal.get("city", ""),
            "target_date": signal.get("target_date", ""),
            "bucket_question": signal.get("bucket_question", ""),
            "market_id": signal.get("market_id", ""),
            "venue_market_id": str(signal.get("venue_market_id", signal.get("market_id", ""))),
            "token_id": signal.get("yes_token_id") if signal.get("side") == "BUY" else signal.get("no_token_id"),
            "client_order_id": signal.get("client_order_id"),
            "venue_order_id": signal.get("venue_order_id"),
            "side": signal.get("side", ""),
            "size_usd": signal.get("trade_size", 0),
            "intended_size_usd": signal.get("intended_size_usd", signal.get("trade_size", 0)),
            "filled_size_usd": signal.get("filled_size_usd", signal.get("trade_size", 0)),
            "filled_contracts": signal.get("filled_contracts"),
            "price": signal.get("market_prob", 0),
            "entry_price": signal.get("entry_price"),
            "expected_entry_price": signal.get("expected_entry_price", signal.get("entry_price")),
            "fill_price": signal.get("fill_price"),
            "order_status": signal.get("order_status"),
            "ensemble_prob": signal.get("ensemble_prob", 0),
            "edge": signal.get("edge", 0),
            "kelly_pct": signal.get("kelly_pct", 0),
            "signal": signal.get("signal", ""),
            "fee_usd": fee,
            "wallet_balance_snapshot": signal.get("wallet_balance_snapshot"),
            "is_contrarian": bool(signal.get("is_contrarian", False)),
            "strategy_version": signal.get("strategy_version"),
            "model_expected_high": signal.get("model_expected_high"),
            "model_spread": signal.get("model_spread"),
            "venue_implied_high": signal.get("venue_implied_high"),
            "settlement_station": signal.get("settlement_station"),
            "forecast_context_json": json.dumps(forecast_context, sort_keys=True) if forecast_context else None,
        }
        timestamp = signal.get("timestamp")
        if isinstance(timestamp, datetime):
            trade_kwargs["timestamp"] = timestamp
        elif isinstance(timestamp, date):
            trade_kwargs["timestamp"] = datetime.combine(timestamp, datetime.min.time(), tzinfo=timezone.utc)
        elif isinstance(timestamp, str):
            try:
                trade_kwargs["timestamp"] = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            except ValueError:
                pass
        for field in ("submitted_at", "filled_at"):
            value = signal.get(field)
            if isinstance(value, datetime):
                trade_kwargs[field] = value
            elif isinstance(value, str):
                try:
                    trade_kwargs[field] = datetime.fromisoformat(value.replace("Z", "+00:00"))
                except ValueError:
                    pass
        trade = Trade(**trade_kwargs)
        session.add(trade)
        session.flush()
        logger.info(f"Trade logged: {trade.side} ${trade.size_usd:.2f} on {trade.bucket_question[:50]}")
        return trade


def has_existing_trade(city: str, target_date: str, bucket_question: str,
                       mode: str = "paper", venue: str = None) -> bool:
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
        )
        if venue:
            existing = existing.filter_by(venue=venue)
        existing = existing.first()
        return existing is not None


def has_logged_trade(city: str, target_date: str, bucket_question: str,
                     side: str, mode: str = "paper", venue: str = None) -> bool:
    """Check if a matching trade row already exists, regardless of resolution state."""
    with session_scope() as session:
        existing = session.query(Trade).filter_by(
            city=city,
            target_date=target_date,
            bucket_question=bucket_question,
            side=side,
            mode=mode,
        )
        if venue:
            existing = existing.filter_by(venue=venue)
        return existing.first() is not None


def get_traded_buckets(mode: str = "paper") -> set:
    """
    Get a set of (venue, city, target_date, bucket_question) tuples for all
    unresolved trades. Used for fast dedup checking.
    """
    with session_scope() as session:
        trades = session.query(
            Trade.venue, Trade.city, Trade.target_date, Trade.bucket_question
        ).filter_by(mode=mode, resolved=False).all()
        return {(t.venue, t.city, t.target_date, t.bucket_question) for t in trades}


def resolve_trade(trade_id: int, outcome: str, pnl: float,
                  resolution_price: float = 0.0, actual_temp: float = None,
                  settlement_station: str | None = None,
                  resolution_source: str | None = None):
    """Resolve a trade with its outcome."""
    with session_scope() as session:
        trade = session.query(Trade).filter_by(id=trade_id).first()
        if trade:
            trade.resolved = True
            trade.resolved_at = datetime.now(timezone.utc)
            trade.outcome = outcome
            trade.pnl = pnl
            trade.resolution_price = resolution_price
            if actual_temp is not None:
                trade.actual_temp = actual_temp
            if settlement_station is not None:
                trade.settlement_station = settlement_station
            if resolution_source is not None:
                trade.resolution_source = resolution_source


def get_unresolved_trades(mode: str = None, venue: str = None) -> list[dict]:
    """Get all unresolved trades as dicts."""
    with session_scope() as session:
        q = session.query(Trade).filter_by(resolved=False)
        if mode:
            q = q.filter_by(mode=mode)
        if venue:
            q = q.filter_by(venue=venue)
        return [
            {
                "id": t.id, "venue": t.venue, "event_title": t.event_title, "city": t.city,
                "target_date": t.target_date, "bucket_question": t.bucket_question,
                "venue_market_id": t.venue_market_id,
                "side": t.side, "size_usd": t.size_usd, "price": t.price,
                "entry_price": t.entry_price,
                "fill_price": t.fill_price,
                "filled_size_usd": t.filled_size_usd,
                "mode": t.mode,
                "ensemble_prob": t.ensemble_prob, "edge": t.edge,
                "fee_usd": t.fee_usd or 0.0,
                "settlement_station": t.settlement_station,
                "resolution_source": t.resolution_source,
            }
            for t in q.all()
        ]


def get_daily_pnl(date: str = None, mode: str = "paper", venue: str = None) -> float:
    """Get total P&L for a specific date."""
    if date is None:
        date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    with session_scope() as session:
        query = session.query(Trade).filter_by(target_date=date, mode=mode, resolved=True)
        if venue:
            query = query.filter_by(venue=venue)
        trades = query.all()
        return sum(t.pnl for t in trades)


def get_total_pnl(mode: str = "paper", venue: str = None) -> float:
    """Get all-time P&L."""
    with session_scope() as session:
        query = session.query(Trade).filter_by(mode=mode, resolved=True)
        if venue:
            query = query.filter_by(venue=venue)
        trades = query.all()
        return sum(t.pnl for t in trades)


def get_realized_pnl_for_day(day: str, mode: str = "paper", venue: str = None) -> float:
    """Get realized P&L for trades resolved on a given UTC date."""
    with session_scope() as session:
        query = session.query(Trade).filter_by(mode=mode, resolved=True)
        if venue:
            query = query.filter_by(venue=venue)
        trades = [
            t for t in query.all()
            if t.resolved_at and t.resolved_at.strftime("%Y-%m-%d") == day
        ]
        return sum(t.pnl for t in trades)


def _to_trading_day(dt: datetime | None, tz: ZoneInfo = TRADING_DAY_TIMEZONE) -> str | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(tz).strftime("%Y-%m-%d")


def get_realized_pnl_for_trading_day(day: str, mode: str = "paper", venue: str = None) -> float:
    """Get realized P&L for trades resolved on a given America/New_York trading day."""
    with session_scope() as session:
        query = session.query(Trade).filter_by(mode=mode, resolved=True)
        if venue:
            query = query.filter_by(venue=venue)
        trades = [t for t in query.all() if _to_trading_day(t.resolved_at) == day]
        return sum(t.pnl for t in trades)


def get_trade_cost_for_trading_day(day: str, mode: str = "paper", venue: str = None) -> float:
    """Get total deployed trade cost (stake + fees) for a given America/New_York trading day."""
    with session_scope() as session:
        query = session.query(Trade).filter_by(mode=mode)
        if venue:
            query = query.filter_by(venue=venue)
        trades = [t for t in query.all() if _to_trading_day(t.timestamp) == day]
        return sum((t.size_usd or 0.0) + (t.fee_usd or 0.0) for t in trades)


def get_trade_count_for_trading_day(day: str, mode: str = "paper", venue: str = None) -> int:
    """Get total number of trades placed on a given America/New_York trading day."""
    with session_scope() as session:
        query = session.query(Trade).filter_by(mode=mode)
        if venue:
            query = query.filter_by(venue=venue)
        return sum(1 for trade in query.all() if _to_trading_day(trade.timestamp) == day)


def get_open_exposure_usd(mode: str = "paper", venue: str = None) -> float:
    """Get total unresolved exposure (stake + fees) in USD."""
    with session_scope() as session:
        query = session.query(Trade).filter_by(mode=mode, resolved=False)
        if venue:
            query = query.filter_by(venue=venue)
        trades = query.all()
        return sum((t.size_usd or 0.0) + (t.fee_usd or 0.0) for t in trades)


def get_trade_stats(mode: str = "paper", venue: str = None) -> dict:
    """Get aggregate trade statistics."""
    with session_scope() as session:
        resolved_query = session.query(Trade).filter_by(mode=mode, resolved=True)
        pending_query = session.query(Trade).filter_by(mode=mode, resolved=False)
        if venue:
            resolved_query = resolved_query.filter_by(venue=venue)
            pending_query = pending_query.filter_by(venue=venue)

        all_trades = resolved_query.all()
        wins = [t for t in all_trades if t.outcome == "win"]
        losses = [t for t in all_trades if t.outcome == "loss"]
        pending = pending_query.count()

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
            venue=signal.get("venue", "polymarket"),
            event_title=signal.get("event_title", ""),
            venue_event_id=str(signal.get("venue_event_id", signal.get("event_id", ""))),
            venue_market_id=str(signal.get("venue_market_id", signal.get("market_id", ""))),
            city=signal.get("city", ""),
            target_date=signal.get("target_date", ""),
            bucket_question=signal.get("bucket_question", ""),
            market_prob=signal.get("market_prob", 0),
            yes_price=signal.get("yes_price"),
            no_price=signal.get("no_price"),
            ensemble_prob=signal.get("ensemble_prob", 0),
            ensemble_mean=meta.get("mean"),
            ensemble_spread=meta.get("spread"),
            ensemble_members=meta.get("member_count"),
            nws_temp=nws.get("temp"),
            edge=signal.get("edge", 0),
            signal=signal.get("signal", ""),
        )
        session.add(snap)


def log_weather_comparison_snapshot(snapshot: dict, mode: str = "paper"):
    """Persist a city/date weather comparison snapshot for learning review."""
    with session_scope() as session:
        row = WeatherComparisonSnapshot(
            mode=mode,
            strategy_version=snapshot.get("strategy_version"),
            city=snapshot.get("city", ""),
            target_date=snapshot.get("target_date", ""),
            model_expected_high=snapshot.get("model_expected_high"),
            model_spread=snapshot.get("model_spread"),
            polymarket_implied_high=snapshot.get("polymarket_implied_high"),
            kalshi_implied_high=snapshot.get("kalshi_implied_high"),
            model_summary_json=json.dumps(snapshot.get("model_summary") or {}),
            venue_availability_json=json.dumps(snapshot.get("venue_availability") or {}),
            candidate_bets_json=json.dumps(snapshot.get("candidate_bets") or []),
            selected_bets_json=json.dumps(snapshot.get("selected_bets") or []),
            skip_reasons_json=json.dumps(snapshot.get("skip_reasons") or []),
        )
        session.add(row)
