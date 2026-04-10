import core.strategy.signals as signals


def test_get_enabled_events_continues_when_polymarket_fetch_fails(monkeypatch):
    monkeypatch.setattr(signals, "ENABLE_POLYMARKET", True)
    monkeypatch.setattr(signals, "ENABLE_KALSHI", True)

    def fail_polymarket():
        raise RuntimeError("polymarket down")

    def ok_kalshi():
        return [{"venue": "kalshi", "event_id": "k1"}]

    monkeypatch.setattr(signals, "get_active_polymarket_temperature_events", fail_polymarket)
    monkeypatch.setattr(signals, "get_active_kalshi_temperature_events", ok_kalshi)

    events = signals._get_enabled_events()

    assert events == [{"venue": "kalshi", "event_id": "k1"}]


def test_get_enabled_events_returns_empty_when_all_enabled_sources_fail(monkeypatch):
    monkeypatch.setattr(signals, "ENABLE_POLYMARKET", True)
    monkeypatch.setattr(signals, "ENABLE_KALSHI", True)

    def fail():
        raise RuntimeError("source down")

    monkeypatch.setattr(signals, "get_active_polymarket_temperature_events", fail)
    monkeypatch.setattr(signals, "get_active_kalshi_temperature_events", fail)

    events = signals._get_enabled_events()

    assert events == []


def test_implied_event_temperature_handles_bounded_and_open_ended_buckets():
    buckets = [
        {"temp_low": None, "temp_high": 67, "yes_price": 0.2},
        {"temp_low": 68, "temp_high": 69, "yes_price": 0.3},
        {"temp_low": 70, "temp_high": 71, "yes_price": 0.5},
    ]

    implied = signals.implied_event_temperature(buckets)

    assert round(implied, 2) == 69.1


def test_finalize_scan_comparisons_attaches_selected_bets():
    comparison_rows = [{
        "city": "miami",
        "target_date": "2026-04-09",
        "candidate_bets": [{"venue": "polymarket", "bucket_question": "80-81F"}],
        "selected_bets": [],
        "skip_reasons": [],
    }]
    executed = [{
        "venue": "kalshi",
        "city": "miami",
        "target_date": "2026-04-09",
        "bucket_question": "82F or higher",
        "side": "SELL",
        "trade_size": 50.0,
        "entry_price": 0.62,
        "selected_prob": 0.78,
        "edge": -0.16,
    }]

    finalized = signals.finalize_scan_comparisons(comparison_rows, executed)

    assert finalized[0]["selected_bets"][0]["venue"] == "kalshi"
    assert finalized[0]["selected_bets"][0]["bucket_question"] == "82F or higher"
