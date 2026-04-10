from core import database
from core.database import Trade, log_trade, session_scope
from core.execution.paper import PaperTrader


def test_execute_paper_trade_rolls_back_when_db_log_fails(monkeypatch):
    trader = PaperTrader(venue_bankrolls={"polymarket": 100.0, "kalshi": 100.0})
    signal = {
        "venue": "kalshi",
        "trade_size": 25.0,
        "side": "BUY",
        "event_id": "event-1",
        "event_title": "Kalshi test event",
        "bucket_question": "70° to 71°",
        "entry_price": 0.44,
        "target_date": "2026-04-09",
        "edge": 0.12,
    }

    def fail_log_trade(*args, **kwargs):
        raise RuntimeError("db write failed")

    monkeypatch.setattr("core.execution.paper.log_trade", fail_log_trade)

    result = trader._execute_paper_trade(signal)

    assert result is False
    assert trader.bankrolls["kalshi"] == 100.0
    assert trader.trades_today == 0
    assert trader.positions == []


def test_log_trade_accepts_iso_timestamp_string(tmp_path, monkeypatch):
    monkeypatch.setattr(database, "DB_PATH", tmp_path / "trades.db")
    database._engine = None
    database._Session = None

    log_trade({
        "timestamp": "2026-04-09T23:15:48+00:00",
        "venue": "kalshi",
        "city": "nyc",
        "target_date": "2026-04-10",
        "bucket_question": "Will the highest temperature in New York City be 67-68°F on April 10?",
        "side": "BUY",
        "trade_size": 42.0,
        "market_prob": 0.44,
        "entry_price": 0.44,
        "ensemble_prob": 0.57,
        "edge": 0.13,
        "selected_prob": 0.57,
        "yes_price": 0.44,
        "no_price": 0.58,
        "yes_edge": 0.13,
        "no_edge": -0.11,
        "ensemble_meta": {
            "mean": 67.8,
            "spread": 5.4,
            "min": 63.2,
            "max": 71.9,
            "member_count": 30,
        },
        "nws_forecast": {
            "temp": 68,
            "unit": "F",
            "short_forecast": "Sunny",
        },
    }, mode="paper")

    with session_scope() as session:
        trade = session.query(Trade).one()
        assert trade.venue == "kalshi"
        assert trade.timestamp.isoformat().startswith("2026-04-09T23:15:48")
        assert "\"ensemble_mean\": 67.8" in trade.forecast_context_json
        assert "\"ensemble_members\": 30" in trade.forecast_context_json
        assert "\"nws_temp\": 68" in trade.forecast_context_json

    database._engine = None
    database._Session = None
