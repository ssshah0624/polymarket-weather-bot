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
