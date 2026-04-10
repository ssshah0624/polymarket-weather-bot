from core import tuning


def test_evaluate_kalshi_tuning_tightens_two_risk_controls():
    base = tuning.get_base_strategy_params("kalshi")
    current = dict(base)
    analysis = {
        "total": 24,
        "total_pnl": -40.0,
        "win_rate": 0.42,
        "edge_stats": {
            "5-10%": {"trades": 12, "wins": 4, "pnl": -60.0},
            "10-20%": {"trades": 12, "wins": 6, "pnl": 20.0},
        },
        "stance_stats": {
            "contrarian": {"trades": 12, "wins": 4, "pnl": -50.0},
            "consensus": {"trades": 12, "wins": 8, "pnl": 50.0},
        },
        "calibration_mean_abs_error": 0.08,
    }

    decision = tuning.evaluate_kalshi_tuning(analysis, base, current, state={"overrides": {}, "positive_streaks": {}})
    changes = decision["applied_changes"]

    assert len(changes) == 2
    assert changes[0]["parameter"] == "edge_threshold"
    assert changes[0]["to"] == 0.09
    assert changes[1]["parameter"] == "contrarian_discount"
    assert changes[1]["to"] == 0.5


def test_evaluate_kalshi_tuning_relaxes_after_positive_streak():
    base = tuning.get_base_strategy_params("kalshi")
    current = dict(base)
    current["edge_threshold"] = 0.09
    analysis = {
        "total": 15,
        "total_pnl": 120.0,
        "win_rate": 0.6,
        "edge_stats": {
            "10-20%": {"trades": 15, "wins": 9, "pnl": 120.0},
            "5-10%": {"trades": 0, "wins": 0, "pnl": 0.0},
        },
        "stance_stats": {},
        "calibration_mean_abs_error": 0.04,
    }

    decision = tuning.evaluate_kalshi_tuning(
        analysis,
        base,
        current,
        state={"overrides": {"edge_threshold": 0.09}, "positive_streaks": {"edge_threshold": 1}},
    )

    assert decision["applied_changes"][0]["parameter"] == "edge_threshold"
    assert decision["applied_changes"][0]["to"] == base["edge_threshold"]


def test_apply_kalshi_tuning_persists_state_and_history(tmp_path, monkeypatch):
    overrides_path = tmp_path / "kalshi_tuning.json"
    history_path = tmp_path / "kalshi_tuning_history.jsonl"
    monkeypatch.setattr(tuning, "KALSHI_TUNING_OVERRIDES_PATH", overrides_path)
    monkeypatch.setattr(tuning, "KALSHI_TUNING_HISTORY_PATH", history_path)

    decision = {
        "evaluated_at": "2026-04-09T00:00:00+00:00",
        "analysis_total": 18,
        "applied_changes": [{"parameter": "edge_threshold", "from": 0.08, "to": 0.09}],
        "held_notes": [],
        "effective_params": {**tuning.get_base_strategy_params("kalshi"), "edge_threshold": 0.09},
        "next_state": {
            "overrides": {"edge_threshold": 0.09},
            "positive_streaks": {},
            "updated_at": "2026-04-09T00:00:00+00:00",
        },
    }

    tuning.apply_kalshi_tuning(decision)
    params = tuning.get_effective_strategy_params("kalshi")

    assert overrides_path.exists()
    assert history_path.exists()
    assert params["edge_threshold"] == 0.09
