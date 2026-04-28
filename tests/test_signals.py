import core.strategy.signals as signals
import logging
from datetime import datetime as real_datetime, timezone


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
        {"temp_low": -999.0, "temp_high": 67.0, "yes_price": 0.2},
        {"temp_low": 68.0, "temp_high": 70.0, "yes_price": 0.3},
        {"temp_low": 70.0, "temp_high": 999.0, "yes_price": 0.5},
    ]

    implied = signals.implied_event_temperature(buckets)

    assert round(implied, 2) == 69.4


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


def test_finalize_scan_comparisons_uses_proposed_bets_for_shadow_rows():
    comparison_rows = [{
        "city": "miami",
        "target_date": "2026-04-09",
        "candidate_bets": [{"venue": "kalshi", "bucket_question": "79-80F"}],
        "proposed_bets": [{
            "venue": "kalshi",
            "bucket_question": "79-80F",
            "side": "BUY",
            "trade_size": 5.0,
            "entry_price": 0.22,
            "model_probability": 0.41,
            "edge": 0.19,
        }],
        "selected_bets": [],
        "skip_reasons": [],
        "model_summary": {"shadow_only": True},
    }]

    finalized = signals.finalize_scan_comparisons(comparison_rows, executed=[])

    assert finalized[0]["selected_bets"][0]["bucket_question"] == "79-80F"
    assert finalized[0]["model_summary"]["selected_bets_source"] == "proposed_shadow"
    assert finalized[0]["model_summary"]["proposed_selected_bets"][0]["side"] == "BUY"


def test_finalize_scan_comparisons_marks_shadow_rows_with_no_package():
    comparison_rows = [{
        "city": "miami",
        "target_date": "2026-04-09",
        "candidate_bets": [{"venue": "kalshi", "bucket_question": "79-80F"}],
        "proposed_bets": [],
        "selected_bets": [],
        "skip_reasons": [],
        "model_summary": {"shadow_only": True},
    }]

    finalized = signals.finalize_scan_comparisons(comparison_rows, executed=[])

    assert finalized[0]["selected_bets"] == []
    assert finalized[0]["model_summary"]["selected_bets_source"] == "no_shadow_package"
    assert finalized[0]["model_summary"]["proposed_selected_bets"] == []


def test_merge_event_summaries_merges_model_summary_fields_across_venues():
    event_summaries = [
        {
            "city": "miami",
            "target_date": "2026-04-17",
            "venue": "polymarket",
            "model_summary": {"forecast_lead_hours": 24.0},
            "candidate_bets": [],
            "skip_reasons": [],
        },
        {
            "city": "miami",
            "target_date": "2026-04-17",
            "venue": "kalshi",
            "model_summary": {"shadow_only": True, "shadow_reason": "kalshi_same_day_shadow"},
            "candidate_bets": [],
            "skip_reasons": [],
        },
    ]

    merged = signals._merge_event_summaries(event_summaries)

    assert merged[0]["model_summary"]["forecast_lead_hours"] == 24.0
    assert merged[0]["model_summary"]["shadow_only"] is True
    assert merged[0]["model_summary"]["shadow_reason"] == "kalshi_same_day_shadow"


def test_log_venue_diagnostics_reports_skip_breakdown(caplog):
    event_summaries = [
        {
            "venue": "kalshi",
            "city": "nyc",
            "target_date": "2026-04-13",
            "candidate_bets": [],
            "status": "skipped_pre_analysis",
            "reason_code": "horizon_same_or_past",
            "reason_detail": "NYC same day",
        },
        {
            "venue": "kalshi",
            "city": "miami",
            "target_date": "2026-04-14",
            "candidate_bets": [{"bucket_question": "80-81F"}],
            "status": "analyzed",
            "reason_code": "no_tradeable_edges",
            "reason_detail": "Miami no tradeable edges",
        },
        {
            "venue": "kalshi",
            "city": "dallas",
            "target_date": "2026-04-14",
            "candidate_bets": [{"bucket_question": "78-79F"}],
            "status": "actionable",
            "reason_code": None,
            "reason_detail": None,
        },
    ]
    all_signals = [{"venue": "kalshi", "trade_size": 5.0}]

    with caplog.at_level(logging.INFO):
        signals._log_venue_diagnostics(event_summaries, all_signals)

    text = caplog.text
    assert "Venue diagnostics [kalshi]:" in text
    assert "events=3" in text
    assert "actionable_events=1" in text
    assert "signals=1" in text
    assert "horizon_same_or_past=1" in text
    assert "no_tradeable_edges=1" in text


def test_scan_event_uses_eastern_day_boundary_for_horizon(monkeypatch):
    class _FakeDateTime(real_datetime):
        @classmethod
        def now(cls, tz=None):
            current = cls(2026, 4, 13, 1, 7, 0, tzinfo=timezone.utc)
            return current if tz is None else current.astimezone(tz)

    monkeypatch.setattr(signals, "datetime", _FakeDateTime)
    monkeypatch.setattr(signals, "MIN_VOLUME", 1000)
    monkeypatch.setattr(signals, "MAX_FORECAST_DAYS", 2)
    monkeypatch.setattr(signals, "KALSHI_MAX_FORECAST_LEAD_HOURS", 24.0)
    monkeypatch.setattr(signals, "KALSHI_SHADOW_INCLUDE_SAME_DAY", True)
    monkeypatch.setattr(signals, "get_forecast_high", lambda *args, **kwargs: None)
    monkeypatch.setattr(signals, "get_hourly_forecast_high", lambda *args, **kwargs: None)
    monkeypatch.setattr(signals, "get_full_distribution", lambda *args, **kwargs: None)

    event = {
        "venue": "kalshi",
        "event_id": "KXHIGHNY-26APR13",
        "event_title": "Highest temperature in New York City on 2026-04-13",
        "city": "nyc",
        "target_date": "2026-04-13",
        "volume": 5000,
        "buckets": [{"question": "60° or below"}],
    }

    result = signals._scan_event(event, bankrolls={"kalshi": 1000}, daily_pnls={"kalshi": 0.0})

    assert result["signals"] == []
    assert result["event_summary"]["reason_code"] == "ensemble_unavailable"
    assert result["event_summary"]["model_summary"]["forecast_lead_bucket"] == "24-36h"


def test_scan_event_skips_kalshi_outside_24h_lead_window(monkeypatch):
    class _FakeDateTime(real_datetime):
        @classmethod
        def now(cls, tz=None):
            current = cls(2026, 4, 13, 16, 0, 0, tzinfo=timezone.utc)
            return current if tz is None else current.astimezone(tz)

    monkeypatch.setattr(signals, "datetime", _FakeDateTime)
    monkeypatch.setattr(signals, "MIN_VOLUME", 1000)
    monkeypatch.setattr(signals, "MAX_FORECAST_DAYS", 2)
    monkeypatch.setattr(signals, "KALSHI_MAX_FORECAST_LEAD_HOURS", 24.0)
    monkeypatch.setattr(signals, "KALSHI_TRADE_NEXT_DAY_ONLY", False)

    event = {
        "venue": "kalshi",
        "event_id": "KXHIGHNY-26APR15",
        "event_title": "Highest temperature in New York City on 2026-04-15",
        "city": "nyc",
        "target_date": "2026-04-15",
        "volume": 5000,
        "buckets": [{"question": "60° or below"}],
    }

    result = signals._scan_event(event, bankrolls={"kalshi": 1000}, daily_pnls={"kalshi": 0.0})

    assert result["signals"] == []
    assert result["event_summary"]["reason_code"] == "horizon_outside_kalshi_lead_window"
    assert result["event_summary"]["model_summary"]["forecast_lead_hours"] == 60.0
    assert result["event_summary"]["model_summary"]["forecast_lead_bucket"] == "48h+"
    assert result["event_summary"]["model_summary"]["trade_window_lead_hours"] == 36.0


def test_scan_event_skips_kalshi_when_not_next_day(monkeypatch):
    class _FakeDateTime(real_datetime):
        @classmethod
        def now(cls, tz=None):
            current = cls(2026, 4, 13, 16, 0, 0, tzinfo=timezone.utc)
            return current if tz is None else current.astimezone(tz)

    monkeypatch.setattr(signals, "datetime", _FakeDateTime)
    monkeypatch.setattr(signals, "MIN_VOLUME", 1000)
    monkeypatch.setattr(signals, "MAX_FORECAST_DAYS", 2)
    monkeypatch.setattr(signals, "KALSHI_TRADE_NEXT_DAY_ONLY", True)

    event = {
        "venue": "kalshi",
        "event_id": "KXHIGHNY-26APR15",
        "event_title": "Highest temperature in New York City on 2026-04-15",
        "city": "nyc",
        "target_date": "2026-04-15",
        "volume": 5000,
        "buckets": [{"question": "60° or below"}],
    }

    result = signals._scan_event(event, bankrolls={"kalshi": 1000}, daily_pnls={"kalshi": 0.0})

    assert result["signals"] == []
    assert result["event_summary"]["reason_code"] == "kalshi_not_next_day"


def test_scan_event_shadows_same_day_kalshi_without_emitting_signals(monkeypatch):
    class _FakeDateTime(real_datetime):
        @classmethod
        def now(cls, tz=None):
            current = cls(2026, 4, 13, 13, 0, 0, tzinfo=timezone.utc)
            return current if tz is None else current.astimezone(tz)

    monkeypatch.setattr(signals, "datetime", _FakeDateTime)
    monkeypatch.setattr(signals, "MIN_VOLUME", 1000)
    monkeypatch.setattr(signals, "MAX_FORECAST_DAYS", 2)
    monkeypatch.setattr(signals, "KALSHI_SHADOW_INCLUDE_SAME_DAY", True)
    monkeypatch.setattr(signals, "get_forecast_high", lambda *args, **kwargs: None)
    monkeypatch.setattr(signals, "get_hourly_forecast_high", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        signals,
        "get_full_distribution",
        lambda *args, **kwargs: [
            {
                "question": "78° to 79°",
                "ensemble_prob": 0.6,
                "selected_prob": 0.6,
                "edge": 0.15,
                "signal": "buy",
                "market_prob": 0.45,
                "selected_price": 0.45,
                "temp_low": 78,
                "temp_high": 80,
                "yes_price": 0.45,
                "no_price": 0.55,
                "ensemble_meta": {
                    "member_count": 30,
                    "mean": 77.5,
                    "min": 73.0,
                    "max": 82.0,
                    "spread": 9.0,
                },
            }
        ],
    )
    monkeypatch.setattr(
        signals,
        "analyze_event_buckets",
        lambda enriched, venue, threshold, strategy_params: [
            {
                **enriched[0],
                "preferred_side": "BUY",
                "selected_price": 0.45,
                "selected_prob": 0.6,
                "edge": 0.15,
                "market_prob": 0.45,
                "signal": "buy",
                "is_tradeable": True,
            }
        ],
    )
    monkeypatch.setattr(signals, "rank_opportunities", lambda analyzed: analyzed)
    monkeypatch.setattr(
        signals,
        "calculate_trade_size",
        lambda *args, **kwargs: {"size": 10.0, "kelly_pct": 0.02, "capped_by": "max_trade_size"},
    )

    event = {
        "venue": "kalshi",
        "event_id": "KXHIGHNY-26APR13",
        "event_title": "Highest temperature in New York City on 2026-04-13",
        "city": "nyc",
        "target_date": "2026-04-13",
        "volume": 5000,
        "buckets": [{"question": "78° to 79°"}],
    }

    result = signals._scan_event(event, bankrolls={"kalshi": 1000}, daily_pnls={"kalshi": 0.0})

    assert result["signals"] == []
    assert result["event_summary"]["reason_code"] == "kalshi_same_day_shadow"
    assert result["event_summary"]["candidate_bets"]
    assert result["event_summary"]["model_summary"]["shadow_only"] is True


def test_scan_event_allows_same_day_kalshi_when_override_enabled(monkeypatch):
    class _FakeDateTime(real_datetime):
        @classmethod
        def now(cls, tz=None):
            current = cls(2026, 4, 13, 13, 0, 0, tzinfo=timezone.utc)
            return current if tz is None else current.astimezone(tz)

    monkeypatch.setattr(signals, "datetime", _FakeDateTime)
    monkeypatch.setattr(signals, "MIN_VOLUME", 1000)
    monkeypatch.setattr(signals, "MAX_FORECAST_DAYS", 2)
    monkeypatch.setattr(signals, "KALSHI_ALLOW_SAME_DAY_TRADING", True)
    monkeypatch.setattr(signals, "KALSHI_SHADOW_INCLUDE_SAME_DAY", True)
    monkeypatch.setattr(signals, "get_forecast_high", lambda *args, **kwargs: None)
    monkeypatch.setattr(signals, "get_hourly_forecast_high", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        signals,
        "get_full_distribution",
        lambda *args, **kwargs: [
            {
                "question": "78° to 79°",
                "ensemble_prob": 0.6,
                "selected_prob": 0.6,
                "edge": 0.15,
                "signal": "buy",
                "market_prob": 0.45,
                "selected_price": 0.45,
                "temp_low": 78,
                "temp_high": 80,
                "yes_price": 0.45,
                "no_price": 0.55,
                "ensemble_meta": {
                    "member_count": 30,
                    "mean": 77.5,
                    "min": 73.0,
                    "max": 82.0,
                    "spread": 9.0,
                },
            }
        ],
    )
    candidate = {
        "question": "78° to 79°",
        "preferred_side": "BUY",
        "selected_price": 0.45,
        "selected_prob": 0.6,
        "edge": 0.15,
        "market_prob": 0.45,
        "signal": "buy",
        "is_tradeable": True,
        "temp_low": 78,
        "temp_high": 80,
        "yes_price": 0.45,
        "no_price": 0.55,
        "ensemble_meta": {
            "member_count": 30,
            "mean": 77.5,
            "min": 73.0,
            "max": 82.0,
            "spread": 9.0,
        },
    }
    monkeypatch.setattr(
        signals,
        "analyze_event_buckets",
        lambda enriched, venue, threshold, strategy_params: [{**enriched[0], **candidate}],
    )
    monkeypatch.setattr(signals, "rank_opportunities", lambda analyzed: analyzed)
    monkeypatch.setattr(
        signals,
        "calculate_trade_size",
        lambda *args, **kwargs: {"size": 10.0, "kelly_pct": 0.02, "capped_by": "max_trade_size"},
    )
    monkeypatch.setattr(signals, "_select_kalshi_event_signals", lambda candidates, strategy_params: candidates)

    event = {
        "venue": "kalshi",
        "event_id": "KXHIGHNY-26APR13",
        "event_title": "Highest temperature in New York City on 2026-04-13",
        "city": "nyc",
        "target_date": "2026-04-13",
        "volume": 5000,
        "buckets": [{"question": "78° to 79°"}],
    }

    result = signals._scan_event(event, bankrolls={"kalshi": 1000}, daily_pnls={"kalshi": 0.0})

    assert result["event_summary"]["status"] == "actionable"
    assert result["event_summary"]["model_summary"]["same_day_live"] is True
    assert result["event_summary"]["model_summary"].get("shadow_only") is not True


def test_scan_event_allows_same_day_kalshi_when_live_top_up_enabled(monkeypatch):
    class _FakeDateTime(real_datetime):
        @classmethod
        def now(cls, tz=None):
            current = cls(2026, 4, 13, 13, 0, 0, tzinfo=timezone.utc)
            return current if tz is None else current.astimezone(tz)

    monkeypatch.setattr(signals, "datetime", _FakeDateTime)
    monkeypatch.setattr(signals, "MIN_VOLUME", 1000)
    monkeypatch.setattr(signals, "MAX_FORECAST_DAYS", 2)
    monkeypatch.setattr(signals, "KALSHI_ALLOW_SAME_DAY_TRADING", False)
    monkeypatch.setattr(signals, "KALSHI_SHADOW_INCLUDE_SAME_DAY", True)
    monkeypatch.setattr(signals, "get_forecast_high", lambda *args, **kwargs: None)
    monkeypatch.setattr(signals, "get_hourly_forecast_high", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        signals,
        "get_full_distribution",
        lambda *args, **kwargs: [
            {
                "question": "78° to 79°",
                "ensemble_prob": 0.6,
                "selected_prob": 0.6,
                "edge": 0.15,
                "signal": "buy",
                "market_prob": 0.45,
                "selected_price": 0.45,
                "temp_low": 78,
                "temp_high": 80,
                "yes_price": 0.45,
                "no_price": 0.55,
                "ensemble_meta": {"member_count": 30, "mean": 77.5, "spread": 9.0},
            }
        ],
    )
    candidate = {
        "question": "78° to 79°",
        "preferred_side": "BUY",
        "selected_price": 0.45,
        "selected_prob": 0.6,
        "edge": 0.15,
        "market_prob": 0.45,
        "signal": "buy",
        "is_tradeable": True,
        "temp_low": 78,
        "temp_high": 80,
        "yes_price": 0.45,
        "no_price": 0.55,
        "ensemble_meta": {"member_count": 30, "mean": 77.5, "spread": 9.0},
    }
    monkeypatch.setattr(
        signals,
        "analyze_event_buckets",
        lambda enriched, venue, threshold, strategy_params: [{**enriched[0], **candidate}],
    )
    monkeypatch.setattr(signals, "rank_opportunities", lambda analyzed: analyzed)
    monkeypatch.setattr(
        signals,
        "calculate_trade_size",
        lambda *args, **kwargs: {"size": 10.0, "kelly_pct": 0.02, "capped_by": "max_trade_size"},
    )
    monkeypatch.setattr(signals, "_select_kalshi_event_signals", lambda candidates, strategy_params: candidates)

    event = {
        "venue": "kalshi",
        "event_id": "KXHIGHNY-26APR13",
        "event_title": "Highest temperature in New York City on 2026-04-13",
        "city": "nyc",
        "target_date": "2026-04-13",
        "volume": 5000,
        "buckets": [{"question": "78° to 79°"}],
    }

    result = signals._scan_event(
        event,
        bankrolls={"kalshi": 1000},
        daily_pnls={"kalshi": 0.0},
        allow_same_day_live=True,
    )

    assert result["event_summary"]["status"] == "actionable"
    assert result["event_summary"]["model_summary"]["same_day_live"] is True
    assert result["event_summary"]["model_summary"]["same_day_live_reason"] == "kalshi_same_day_target_top_up"
    assert result["event_summary"]["model_summary"].get("shadow_only") is not True


def test_scan_event_propagates_same_day_shadow_flag_into_signal_context(monkeypatch):
    seen = {}

    class _FakeDateTime(real_datetime):
        @classmethod
        def now(cls, tz=None):
            current = cls(2026, 4, 13, 13, 0, 0, tzinfo=timezone.utc)
            return current if tz is None else current.astimezone(tz)

    monkeypatch.setattr(signals, "datetime", _FakeDateTime)
    monkeypatch.setattr(signals, "MIN_VOLUME", 1000)
    monkeypatch.setattr(signals, "MAX_FORECAST_DAYS", 2)
    monkeypatch.setattr(signals, "KALSHI_SHADOW_INCLUDE_SAME_DAY", True)
    monkeypatch.setattr(signals, "get_forecast_high", lambda *args, **kwargs: None)
    monkeypatch.setattr(signals, "get_hourly_forecast_high", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        signals,
        "get_full_distribution",
        lambda *args, **kwargs: [
            {
                "question": "78° to 79°",
                "ensemble_prob": 0.6,
                "selected_prob": 0.6,
                "edge": 0.15,
                "signal": "buy",
                "market_prob": 0.45,
                "selected_price": 0.45,
                "temp_low": 78,
                "temp_high": 80,
                "yes_price": 0.45,
                "no_price": 0.55,
                "ensemble_meta": {
                    "member_count": 30,
                    "mean": 77.5,
                    "min": 73.0,
                    "max": 82.0,
                    "spread": 9.0,
                },
            }
        ],
    )
    monkeypatch.setattr(
        signals,
        "analyze_event_buckets",
        lambda enriched, venue, threshold, strategy_params: [
            {
                **enriched[0],
                "preferred_side": "BUY",
                "selected_price": 0.45,
                "selected_prob": 0.6,
                "edge": 0.15,
                "market_prob": 0.45,
                "signal": "buy",
                "is_tradeable": True,
            }
        ],
    )
    monkeypatch.setattr(signals, "rank_opportunities", lambda analyzed: analyzed)
    monkeypatch.setattr(
        signals,
        "calculate_trade_size",
        lambda *args, **kwargs: {"size": 10.0, "kelly_pct": 0.02, "capped_by": "max_trade_size"},
    )

    def fake_select(candidates, strategy_params):
        seen["shadow_only"] = candidates[0]["forecast_context"]["shadow_only"]
        seen["shadow_reason"] = candidates[0]["forecast_context"]["shadow_reason"]
        return []

    monkeypatch.setattr(signals, "_select_kalshi_event_signals", fake_select)

    event = {
        "venue": "kalshi",
        "event_id": "KXHIGHNY-26APR13",
        "event_title": "Highest temperature in New York City on 2026-04-13",
        "city": "nyc",
        "target_date": "2026-04-13",
        "volume": 5000,
        "buckets": [{"question": "78° to 79°"}],
    }

    signals._scan_event(event, bankrolls={"kalshi": 1000}, daily_pnls={"kalshi": 0.0})

    assert seen["shadow_only"] is True
    assert seen["shadow_reason"] == "kalshi_same_day_shadow"


def test_scan_event_records_station_aware_nws_hourly_context(monkeypatch):
    recorded = {}

    class _FakeDateTime(real_datetime):
        @classmethod
        def now(cls, tz=None):
            current = cls(2026, 4, 13, 12, 0, 0, tzinfo=timezone.utc)
            return current if tz is None else current.astimezone(tz)

    monkeypatch.setattr(signals, "datetime", _FakeDateTime)
    monkeypatch.setattr(signals, "MIN_VOLUME", 1000)
    monkeypatch.setattr(signals, "MAX_FORECAST_DAYS", 2)
    monkeypatch.setattr(signals, "KALSHI_MAX_FORECAST_LEAD_HOURS", 24.0)
    monkeypatch.setattr(signals, "KALSHI_SHADOW_INCLUDE_SAME_DAY", True)
    original_strategy_params = signals.get_effective_strategy_params
    monkeypatch.setattr(
        signals,
        "get_effective_strategy_params",
        lambda venue: {
            **original_strategy_params(venue),
            "contrarian_discount": 0.6,
        },
    )
    monkeypatch.setattr(
        signals,
        "get_climate_station_metadata",
        lambda city_key: {
            "station_name": "Central Park NY",
            "issuedby": "NYC",
            "lat": 40.7783,
            "lon": -73.9667,
            "source": "nws_cf6",
        },
    )
    monkeypatch.setattr(
        signals,
        "get_forecast_high",
        lambda *args, **kwargs: {
            "temp": 78,
            "unit": "F",
            "period_name": "Monday",
            "short_forecast": "Sunny",
        },
    )
    monkeypatch.setattr(
        signals,
        "get_hourly_forecast_high",
        lambda *args, **kwargs: {
            "temp": 78,
            "unit": "F",
            "hour": 15,
            "source": "nws_hourly_forecast",
        },
    )
    def fake_get_full_distribution(*args, **kwargs):
        recorded["distribution_kwargs"] = kwargs
        return [
            {
                "question": "78° to 79°",
                "ensemble_prob": 0.6,
                "selected_prob": 0.6,
                "edge": 0.15,
                "signal": "buy",
                "market_prob": 0.45,
                "selected_price": 0.45,
                "temp_low": 78,
                "temp_high": 80,
                "yes_price": 0.45,
                "no_price": 0.55,
                "ensemble_meta": {
                    "member_count": 30,
                    "mean": 77.5,
                    "min": 73.0,
                    "max": 82.0,
                    "spread": 9.0,
                    "raw_mean": 72.5,
                    "raw_min": 69.0,
                    "raw_max": 76.0,
                    "raw_spread": 7.0,
                    "blend_weight": 0.65,
                    "blend_source": "nws_station_forecast",
                },
            }
        ]

    monkeypatch.setattr(signals, "get_full_distribution", fake_get_full_distribution)
    monkeypatch.setattr(
        signals,
        "analyze_event_buckets",
        lambda enriched, venue, threshold, strategy_params: [
            {
                **enriched[0],
                "preferred_side": "BUY",
                "selected_price": 0.45,
                "selected_prob": 0.6,
                "edge": 0.15,
                "market_prob": 0.45,
                "signal": "buy",
                "is_tradeable": True,
            }
        ],
    )
    monkeypatch.setattr(signals, "rank_opportunities", lambda analyzed: analyzed)
    monkeypatch.setattr(
        signals,
        "calculate_trade_size",
        lambda *args, **kwargs: {"size": 10.0, "kelly_pct": 0.02, "capped_by": "max_trade_size"},
    )

    event = {
        "venue": "kalshi",
        "event_id": "KXHIGHNY-26APR14",
        "event_title": "Highest temperature in New York City on 2026-04-14",
        "city": "nyc",
        "target_date": "2026-04-14",
        "volume": 5000,
        "buckets": [{"question": "78° to 79°"}],
    }

    result = signals._scan_event(event, bankrolls={"kalshi": 1000}, daily_pnls={"kalshi": 0.0})

    signal = result["signals"][0]
    assert signal["nws_forecast"]["temp"] == 78
    assert signal["nws_forecast"]["hourly_max_temp"] == 78
    assert signal["nws_forecast"]["hourly_max_hour"] == 15
    assert signal["nws_forecast"]["settlement_station"] == "Central Park NY"
    assert result["event_summary"]["model_summary"]["nws_hourly_max_temp"] == 78
    assert result["event_summary"]["model_summary"]["forecast_anchor_temp"] == 78
    assert result["event_summary"]["model_summary"]["raw_ensemble_mean"] == 72.5
    assert result["event_summary"]["model_summary"]["forecast_lead_bucket"] == "36-48h"
    assert signal["forecast_context"]["forecast_blend_weight"] == 0.65
    assert signal["forecast_context"]["forecast_lead_bucket"] == "36-48h"
    assert signal["trade_size"] == 3.0
    assert "nws_disagreement" in signal["capped_by"]
    assert recorded["distribution_kwargs"] == {
        "lat": 40.7783,
        "lon": -73.9667,
        "anchor_temp": 78.0,
        "blend_weight": 0.65,
        "blend_source": "nws_station_forecast",
    }


def _kalshi_signal(bucket_question: str, *, side: str = "BUY", edge: float = 0.2,
                   selected_prob: float = 0.4, trade_size: float = 20.0,
                   temp_low: float = 66.0, temp_high: float = 68.0,
                   anchor: float = 67.5,
                   hourly: float | None = None,
                   daily: float | None = None,
                   venue_implied_high: float | None = None) -> dict:
    return {
        "venue": "kalshi",
        "bucket_question": bucket_question,
        "side": side,
        "edge": edge,
        "trade_size": trade_size,
        "temp_low": temp_low,
        "temp_high": temp_high,
        "selected_prob": selected_prob,
        "venue_implied_high": venue_implied_high,
        "forecast_context": {
            "selected_prob": selected_prob,
            "forecast_anchor_temp": anchor,
            "ensemble_mean": anchor,
            "nws_hourly_max_temp": hourly if hourly is not None else anchor,
            "nws_temp": daily if daily is not None else anchor,
        },
    }


def test_select_kalshi_event_signals_defaults_to_primary_bucket():
    strategy_params = signals.get_effective_strategy_params("kalshi")
    candidates = [
        _kalshi_signal("66-67", edge=0.24, selected_prob=0.45, temp_low=66, temp_high=68, anchor=66.5, hourly=67, daily=67),
        _kalshi_signal("68-69", edge=0.19, selected_prob=0.18, temp_low=68, temp_high=70, anchor=66.5, hourly=67, daily=67),
        _kalshi_signal("70-71", edge=0.12, selected_prob=0.12, temp_low=70, temp_high=72, anchor=66.5, hourly=67, daily=67),
    ]

    selected = signals._select_kalshi_event_signals(candidates, strategy_params)

    assert [signal["bucket_question"] for signal in selected] == ["66-67"]
    assert selected[0]["forecast_context"]["event_selection"] == "hedged"
    assert selected[0]["forecast_context"]["event_role"] == "primary"


def test_select_kalshi_event_signals_adds_adjacent_spill_near_boundary():
    strategy_params = signals.get_effective_strategy_params("kalshi")
    candidates = [
        _kalshi_signal("66-67", edge=0.22, selected_prob=0.42, trade_size=30.0, temp_low=66, temp_high=68, anchor=67.6, hourly=67, daily=68),
        _kalshi_signal("68-69", edge=0.18, selected_prob=0.28, trade_size=24.0, temp_low=68, temp_high=70, anchor=67.6, hourly=67, daily=68),
        _kalshi_signal("70-71", edge=0.11, selected_prob=0.14, trade_size=18.0, temp_low=70, temp_high=72, anchor=67.6, hourly=67, daily=68),
    ]

    selected = signals._select_kalshi_event_signals(candidates, strategy_params)

    assert [signal["bucket_question"] for signal in selected] == ["66-67", "68-69"]
    assert selected[0]["forecast_context"]["event_selection"] == "hedged"
    assert selected[0]["forecast_context"]["event_role"] == "primary"
    assert selected[1]["forecast_context"]["event_selection"] == "hedged"
    assert selected[1]["forecast_context"]["event_role"] == "spill"


def test_select_kalshi_event_signals_rejects_same_day_single_bucket_buy():
    strategy_params = signals.get_effective_strategy_params("kalshi")
    candidates = [
        _kalshi_signal(
            "66-67",
            edge=0.24,
            selected_prob=0.45,
            temp_low=66,
            temp_high=68,
            anchor=66.5,
            hourly=67,
            daily=67,
        ),
    ]
    candidates[0]["forecast_context"]["shadow_only"] = True

    selected = signals._select_kalshi_event_signals(candidates, strategy_params)

    assert selected == []


def test_select_kalshi_event_signals_keeps_same_day_two_leg_buy_hedge():
    strategy_params = signals.get_effective_strategy_params("kalshi")
    candidates = [
        _kalshi_signal("66-67", edge=0.22, selected_prob=0.42, trade_size=30.0, temp_low=66, temp_high=68, anchor=67.6, hourly=67, daily=68),
        _kalshi_signal("68-69", edge=0.18, selected_prob=0.28, trade_size=24.0, temp_low=68, temp_high=70, anchor=67.6, hourly=67, daily=68),
    ]
    for candidate in candidates:
        candidate["forecast_context"]["shadow_only"] = True

    selected = signals._select_kalshi_event_signals(candidates, strategy_params)

    assert [signal["bucket_question"] for signal in selected] == ["66-67", "68-69"]


def test_select_kalshi_event_signals_builds_same_day_sell_ladder(monkeypatch):
    monkeypatch.setattr(signals, "WEATHER_STRATEGY_VERSION", "weather_v2_same_day_sell_ladder")
    strategy_params = signals.get_effective_strategy_params("kalshi")
    strategy_params["kalshi_local_ladder_max_distance_f"] = 10.0
    candidates = [
        _kalshi_signal("73-74", side="SELL", edge=-0.18, selected_prob=0.82, trade_size=18.0, temp_low=73, temp_high=75, anchor=70.0, hourly=70, daily=70),
        _kalshi_signal("75-76", side="SELL", edge=-0.14, selected_prob=0.88, trade_size=15.0, temp_low=75, temp_high=77, anchor=70.0, hourly=70, daily=70),
        _kalshi_signal("77-78", side="SELL", edge=-0.12, selected_prob=0.91, trade_size=12.0, temp_low=77, temp_high=79, anchor=70.0, hourly=70, daily=70),
        _kalshi_signal("69-70", side="BUY", edge=0.10, selected_prob=0.33, trade_size=12.0, temp_low=69, temp_high=71, anchor=70.0, hourly=70, daily=70),
    ]
    for candidate in candidates:
        candidate["forecast_context"]["shadow_only"] = True

    selected = signals._select_kalshi_event_signals(candidates, strategy_params)

    assert [signal["bucket_question"] for signal in selected] == ["73-74", "75-76", "77-78"]
    assert all(signal["side"] == "SELL" for signal in selected)
    assert all(
        signal["forecast_context"]["event_selection"] == "same_day_sell_ladder"
        for signal in selected
    )


def test_select_kalshi_event_signals_builds_same_day_live_sell_ladder(monkeypatch):
    monkeypatch.setattr(signals, "WEATHER_STRATEGY_VERSION", "weather_v2_same_day_sell_ladder")
    strategy_params = signals.get_effective_strategy_params("kalshi")
    strategy_params["kalshi_local_ladder_max_distance_f"] = 10.0
    candidates = [
        _kalshi_signal("73-74", side="SELL", edge=-0.18, selected_prob=0.82, trade_size=18.0, temp_low=73, temp_high=75, anchor=70.0, hourly=70, daily=70),
        _kalshi_signal("75-76", side="SELL", edge=-0.14, selected_prob=0.88, trade_size=15.0, temp_low=75, temp_high=77, anchor=70.0, hourly=70, daily=70),
        _kalshi_signal("77-78", side="SELL", edge=-0.12, selected_prob=0.91, trade_size=12.0, temp_low=77, temp_high=79, anchor=70.0, hourly=70, daily=70),
    ]
    for candidate in candidates:
        candidate["forecast_context"]["same_day_live"] = True

    selected = signals._select_kalshi_event_signals(candidates, strategy_params)

    assert [signal["bucket_question"] for signal in selected] == ["73-74", "75-76", "77-78"]
    assert all(signal["side"] == "SELL" for signal in selected)


def test_select_kalshi_event_signals_can_choose_sell_as_primary():
    strategy_params = signals.get_effective_strategy_params("kalshi")
    candidates = [
        _kalshi_signal("66-67", side="SELL", edge=-0.22, selected_prob=0.92, temp_low=66, temp_high=68, anchor=64.0, hourly=64, daily=64),
        _kalshi_signal("64-65", side="BUY", edge=0.24, selected_prob=0.30, temp_low=64, temp_high=66, anchor=64.0, hourly=67, daily=67),
    ]

    selected = signals._select_kalshi_event_signals(candidates, strategy_params)

    assert [signal["bucket_question"] for signal in selected] == ["66-67"]
    assert selected[0]["side"] == "SELL"
    assert selected[0]["forecast_context"]["event_role"] == "primary"


def test_select_kalshi_event_signals_only_hedges_one_adjacent_bucket():
    strategy_params = signals.get_effective_strategy_params("kalshi")
    candidates = [
        _kalshi_signal("66-67", edge=0.25, selected_prob=0.44, trade_size=30.0, temp_low=66, temp_high=68, anchor=67.6, hourly=67, daily=68),
        _kalshi_signal("68-69", edge=0.21, selected_prob=0.31, trade_size=24.0, temp_low=68, temp_high=70, anchor=67.6, hourly=67, daily=68),
        _kalshi_signal("70-71", edge=0.18, selected_prob=0.23, trade_size=18.0, temp_low=70, temp_high=72, anchor=67.6, hourly=67, daily=68),
    ]

    selected = signals._select_kalshi_event_signals(candidates, strategy_params)

    assert [signal["bucket_question"] for signal in selected] == ["66-67", "68-69"]
    assert len(selected) == 2


def test_signal_midpoint_uses_inclusive_bucket_center():
    signal = _kalshi_signal("79-80", temp_low=79, temp_high=81, anchor=79.5, hourly=79, daily=79)

    midpoint = signals._signal_midpoint(signal)

    assert midpoint == 79.5


def test_select_kalshi_event_signals_filters_off_center_buy_even_if_edge_is_high():
    strategy_params = signals.get_effective_strategy_params("kalshi")
    candidates = [
        _kalshi_signal("65-66", edge=0.30, selected_prob=0.35, temp_low=65, temp_high=67, anchor=67.0, hourly=67, daily=67),
        _kalshi_signal("67-68", edge=0.08, selected_prob=0.20, temp_low=67, temp_high=69, anchor=67.0, hourly=67, daily=67),
    ]

    selected = signals._select_kalshi_event_signals(candidates, strategy_params)

    assert selected[0]["bucket_question"] == "67-68"
    assert selected[0]["forecast_context"]["event_role"] == "primary"
    assert [signal["bucket_question"] for signal in selected] == ["67-68", "65-66"]


def test_select_kalshi_event_signals_rejects_sell_inside_margin():
    strategy_params = signals.get_effective_strategy_params("kalshi")
    candidates = [
        _kalshi_signal("85-86", side="SELL", edge=-0.18, selected_prob=0.75, temp_low=85, temp_high=87, anchor=84.0, hourly=84, daily=85),
    ]

    selected = signals._select_kalshi_event_signals(candidates, strategy_params)

    assert selected == []


def test_select_kalshi_event_signals_skips_market_when_anchor_diverges_from_strip():
    strategy_params = signals.get_effective_strategy_params("kalshi")
    candidates = [
        _kalshi_signal("66-67", edge=0.20, selected_prob=0.40, temp_low=66, temp_high=68, anchor=66.5, hourly=67, daily=67, venue_implied_high=70.0),
    ]

    selected = signals._select_kalshi_event_signals(candidates, strategy_params)

    assert selected == []


def test_rebalance_selected_event_signals_applies_convex_split():
    selected = [
        _kalshi_signal("66-67", trade_size=30.0, edge=0.22, selected_prob=0.42),
        _kalshi_signal("68-69", trade_size=24.0, edge=0.18, selected_prob=0.28, temp_low=68, temp_high=70),
    ]
    selected[0]["forecast_context"]["event_role"] = "primary"
    selected[1]["forecast_context"]["event_role"] = "spill"

    rebalanced = signals._rebalance_selected_event_signals(selected)

    assert len(rebalanced) == 2
    assert round(sum(signal["trade_size"] for signal in rebalanced), 2) == 30.0
    assert rebalanced[0]["trade_size"] == 21.0
    assert rebalanced[1]["trade_size"] == 9.0
    assert rebalanced[0]["capped_by"] == "event_bucket_hedge"
    assert rebalanced[1]["capped_by"] == "event_bucket_hedge"
    assert rebalanced[0]["forecast_context"]["event_bucket_hedge"] is True


def test_rebalance_selected_event_signals_clears_split_metadata_when_spill_too_small():
    selected = [
        _kalshi_signal("47-48", trade_size=3.0, edge=0.2, selected_prob=0.6, temp_low=47, temp_high=49),
        _kalshi_signal("45-46", trade_size=1.2, edge=0.14, selected_prob=0.25, temp_low=45, temp_high=47),
    ]
    selected[0]["forecast_context"]["event_role"] = "primary"
    selected[0]["forecast_context"]["event_split_partner"] = "45-46"
    selected[0]["forecast_context"]["event_split_boundary_f"] = 47.0
    selected[1]["forecast_context"]["event_role"] = "spill"

    rebalanced = signals._rebalance_selected_event_signals(selected)

    assert len(rebalanced) == 1
    assert rebalanced[0]["forecast_context"]["event_role"] == "primary"
    assert "event_split_partner" not in rebalanced[0]["forecast_context"]
    assert "event_split_boundary_f" not in rebalanced[0]["forecast_context"]


def test_rebalance_selected_event_signals_drops_same_day_buy_when_spill_too_small():
    selected = [
        _kalshi_signal("47-48", trade_size=3.0, edge=0.2, selected_prob=0.6, temp_low=47, temp_high=49),
        _kalshi_signal("45-46", trade_size=1.2, edge=0.14, selected_prob=0.25, temp_low=45, temp_high=47),
    ]
    selected[0]["forecast_context"]["event_role"] = "primary"
    selected[0]["forecast_context"]["shadow_only"] = True
    selected[1]["forecast_context"]["event_role"] = "spill"
    selected[1]["forecast_context"]["shadow_only"] = True

    rebalanced = signals._rebalance_selected_event_signals(selected)

    assert rebalanced == []


def test_rebalance_selected_event_signals_scales_same_day_sell_ladder():
    selected = [
        _kalshi_signal("73-74", side="SELL", trade_size=18.0, edge=-0.18, selected_prob=0.82, temp_low=73, temp_high=75, anchor=70.0, hourly=70, daily=70),
        _kalshi_signal("75-76", side="SELL", trade_size=15.0, edge=-0.14, selected_prob=0.88, temp_low=75, temp_high=77, anchor=70.0, hourly=70, daily=70),
        _kalshi_signal("77-78", side="SELL", trade_size=12.0, edge=-0.12, selected_prob=0.91, temp_low=77, temp_high=79, anchor=70.0, hourly=70, daily=70),
    ]
    for idx, signal in enumerate(selected, start=1):
        signal["forecast_context"]["shadow_only"] = True
        signal["forecast_context"]["event_selection"] = "same_day_sell_ladder"
        signal["forecast_context"]["event_role"] = f"ladder_{idx}"

    rebalanced = signals._rebalance_selected_event_signals(selected)

    assert len(rebalanced) == 3
    assert round(sum(signal["trade_size"] for signal in rebalanced), 2) == 18.0
    assert [signal["capped_by"] for signal in rebalanced] == [
        "event_sell_ladder",
        "event_sell_ladder",
        "event_sell_ladder",
    ]
    assert rebalanced[0]["forecast_context"]["event_bucket_split_style"] == "proportional_sell_ladder"
