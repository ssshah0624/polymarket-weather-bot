from core import learning
from core import tuning


def _trade(
    *,
    venue: str,
    city: str,
    side: str,
    edge: float,
    outcome: str,
    pnl: float,
    actual_temp: float,
    model_expected_high: float,
    is_contrarian: bool,
) -> dict:
    return {
        "id": 1,
        "venue": venue,
        "city": city,
        "target_date": "2026-04-09",
        "side": side,
        "size_usd": 50.0,
        "price": 0.5,
        "ensemble_prob": 0.62,
        "edge": edge,
        "outcome": outcome,
        "pnl": pnl,
        "actual_temp": actual_temp,
        "model_expected_high": model_expected_high,
        "is_contrarian": is_contrarian,
        "strategy_version": "weather_v2",
    }


def test_generate_recommendations_emits_rules_based_changes():
    trades = []

    for idx in range(12):
        trades.append(_trade(
            venue="kalshi" if idx < 8 else "polymarket",
            city="miami",
            side="SELL",
            edge=0.09,
            outcome="win" if idx < 2 or 8 <= idx < 10 else "loss",
            pnl=12.0 if idx < 2 or 8 <= idx < 10 else -10.0,
            actual_temp=83.5,
            model_expected_high=81.0,
            is_contrarian=idx < 8,
        ))

    for idx in range(8):
        trades.append(_trade(
            venue="polymarket",
            city="nyc",
            side="BUY",
            edge=0.22,
            outcome="win" if idx < 6 else "loss",
            pnl=14.0 if idx < 6 else -8.0,
            actual_temp=58.0,
            model_expected_high=58.2,
            is_contrarian=False,
        ))

    analysis = learning.analyze_patterns(trades)
    recommendations = learning.generate_recommendations(analysis)
    rules = {item["rule"] for item in recommendations}

    assert "low_edge_underperformance" in rules
    assert "venue_underperformance" in rules
    assert "city_bias_detected" in rules
    assert "contrarian_underperformance" in rules


def test_send_weekly_digest_reports_active_params_and_applied_changes(monkeypatch, tmp_path):
    sent = {"blocks": None}

    monkeypatch.setattr(tuning, "KALSHI_TUNING_OVERRIDES_PATH", tmp_path / "kalshi_tuning.json")
    monkeypatch.setattr(tuning, "KALSHI_TUNING_HISTORY_PATH", tmp_path / "kalshi_tuning_history.jsonl")

    trades = [
        _trade(
            venue="kalshi",
            city="miami",
            side="SELL",
            edge=0.09,
            outcome="win" if idx < 4 else "loss",
            pnl=12.0 if idx < 4 else -10.0,
            actual_temp=83.5,
            model_expected_high=81.0,
            is_contrarian=idx % 2 == 0,
        )
        for idx in range(12)
    ]

    monkeypatch.setattr(learning, "_get_resolved_trades", lambda days=14, mode="paper", venue=None: trades)
    monkeypatch.setattr(learning, "send_slack_message", lambda text: True)

    def fake_blocks(blocks, text=""):
        sent["blocks"] = blocks
        return True

    monkeypatch.setattr(learning, "send_slack_blocks", fake_blocks)

    learning.send_weekly_digest(mode="paper")

    joined = "\n".join(block["text"]["text"] for block in sent["blocks"] if block.get("type") == "section")
    assert "Weekly Learning Digest (PAPER) — Kalshi" == sent["blocks"][0]["text"]["text"]
    assert "Active params:" in joined
    assert "Applied This Week:" in joined
    assert "edge_threshold" in joined


def test_analyze_shadow_layer_experiment_recommends_replace(monkeypatch):
    trades = [
        {
            "id": 1,
            "venue": "kalshi",
            "city": "miami",
            "target_date": "2026-04-15",
            "side": "BUY",
            "size_usd": 5.0,
            "price": 0.2,
            "ensemble_prob": 0.4,
            "edge": 0.2,
            "outcome": "loss",
            "pnl": -5.0,
            "actual_temp": 80.0,
            "settlement_station": "MIAMI",
            "resolution_source": "nws_cli_daily",
            "model_expected_high": 78.0,
            "forecast_context": {},
            "is_contrarian": False,
            "strategy_version": "weather_v2",
        },
        {
            "id": 2,
            "venue": "kalshi",
            "city": "nyc",
            "target_date": "2026-04-15",
            "side": "BUY",
            "size_usd": 5.0,
            "price": 0.2,
            "ensemble_prob": 0.4,
            "edge": 0.2,
            "outcome": "loss",
            "pnl": -5.0,
            "actual_temp": 86.0,
            "settlement_station": "CENTRAL PARK NY",
            "resolution_source": "nws_cli_daily",
            "model_expected_high": 84.0,
            "forecast_context": {},
            "is_contrarian": False,
            "strategy_version": "weather_v2",
        },
        {
            "id": 3,
            "venue": "kalshi",
            "city": "austin",
            "target_date": "2026-04-15",
            "side": "BUY",
            "size_usd": 5.0,
            "price": 0.2,
            "ensemble_prob": 0.4,
            "edge": 0.2,
            "outcome": "loss",
            "pnl": -5.0,
            "actual_temp": 84.0,
            "settlement_station": "AUSTIN BERGSTROM",
            "resolution_source": "nws_cli_daily",
            "model_expected_high": 82.0,
            "forecast_context": {},
            "is_contrarian": False,
            "strategy_version": "weather_v2",
        },
        {
            "id": 4,
            "venue": "kalshi",
            "city": "denver",
            "target_date": "2026-04-15",
            "side": "BUY",
            "size_usd": 5.0,
            "price": 0.2,
            "ensemble_prob": 0.4,
            "edge": 0.2,
            "outcome": "loss",
            "pnl": -5.0,
            "actual_temp": 67.0,
            "settlement_station": "DENVER",
            "resolution_source": "nws_cli_daily",
            "model_expected_high": 65.0,
            "forecast_context": {},
            "is_contrarian": False,
            "strategy_version": "weather_v2",
        },
    ]
    snapshots = [
        {
            "timestamp": None,
            "mode": "live",
            "strategy_version": "weather_v2",
            "city": "miami",
            "target_date": "2026-04-15",
            "model_summary": {"shadow_only": True, "selected_bets_source": "proposed_shadow"},
            "candidate_bets": [],
            "selected_bets": [{"bucket_question": "79° to 80°", "side": "BUY", "trade_size": 5.0, "entry_price": 0.2}],
            "skip_reasons": [],
        },
        {
            "timestamp": None,
            "mode": "live",
            "strategy_version": "weather_v2",
            "city": "nyc",
            "target_date": "2026-04-15",
            "model_summary": {"shadow_only": True, "selected_bets_source": "proposed_shadow"},
            "candidate_bets": [],
            "selected_bets": [{"bucket_question": "85° to 86°", "side": "BUY", "trade_size": 5.0, "entry_price": 0.2}],
            "skip_reasons": [],
        },
        {
            "timestamp": None,
            "mode": "live",
            "strategy_version": "weather_v2",
            "city": "austin",
            "target_date": "2026-04-15",
            "model_summary": {"shadow_only": True, "selected_bets_source": "proposed_shadow"},
            "candidate_bets": [],
            "selected_bets": [{"bucket_question": "83° to 84°", "side": "BUY", "trade_size": 5.0, "entry_price": 0.2}],
            "skip_reasons": [],
        },
        {
            "timestamp": None,
            "mode": "live",
            "strategy_version": "weather_v2",
            "city": "denver",
            "target_date": "2026-04-15",
            "model_summary": {"shadow_only": True, "selected_bets_source": "proposed_shadow"},
            "candidate_bets": [],
            "selected_bets": [{"bucket_question": "67° to 68°", "side": "BUY", "trade_size": 5.0, "entry_price": 0.2}],
            "skip_reasons": [],
        },
    ]

    monkeypatch.setattr(learning, "_get_resolved_trades", lambda days=5, mode="live", venue="kalshi": trades)
    monkeypatch.setattr(learning, "_get_weather_comparison_snapshots", lambda days=7, mode="live": snapshots)

    result = learning.analyze_shadow_layer_experiment(days=5, mode="live", venue="kalshi")

    assert result["summary"]["sample_size"] == 4
    assert result["summary"]["shadow_replace_pnl"] > result["summary"]["hold_pnl"]
    assert result["summary"]["recommendation"]["layer"] == "same_day_replace"


def test_send_shadow_experiment_digest_reports_recommendation(monkeypatch):
    sent = {"blocks": None}
    monkeypatch.setattr(
        learning,
        "analyze_shadow_layer_experiment",
        lambda days=5, mode="live", venue="kalshi": {
            "cases": [{
                "city": "miami",
                "target_date": "2026-04-15",
                "held_pnl": -5.0,
                "shadow_pnl_estimate": 20.0,
                "replace_benefit": 25.0,
                "shadow_changed": True,
            }],
            "summary": {
                "sample_size": 5,
                "changed_cases": 3,
                "hold_pnl": -12.0,
                "shadow_replace_pnl": 8.0,
                "veto_pnl": -2.0,
                "recommendation": {
                    "layer": "same_day_replace",
                    "reason": "Shadow replace beat hold.",
                },
            },
        },
    )
    monkeypatch.setattr(learning, "send_slack_message", lambda text: True)

    def fake_blocks(blocks, text=""):
        sent["blocks"] = blocks
        sent["text"] = text
        return True

    monkeypatch.setattr(learning, "send_slack_blocks", fake_blocks)

    learning.send_shadow_experiment_digest(days=5, mode="live")

    assert sent["blocks"][0]["text"]["text"] == "Shadow Layer Experiment (LIVE) — Kalshi"
    joined = "\n".join(block["text"]["text"] for block in sent["blocks"] if block.get("type") == "section")
    assert "same_day_replace" in joined
    assert "Hold P&L" in joined
