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
