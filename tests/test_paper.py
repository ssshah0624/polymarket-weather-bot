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
