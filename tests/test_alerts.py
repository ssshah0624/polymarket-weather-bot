from core import alerts


def _detail(index: int, won: bool = True, venue: str = "kalshi") -> dict:
    return {
        "city": f"City {index}",
        "bucket": f"Bucket range {index} with extra words to force length",
        "actual_temp_f": 70 + index,
        "size": 50.0,
        "market_price": 0.4,
        "ensemble_prob": 0.62,
        "side": "YES",
        "pnl": 10.0 if won else -10.0,
        "won": won,
        "venue": venue,
    }


def _trade(**overrides) -> dict:
    trade = {
        "venue": "polymarket",
        "city": "los_angeles",
        "target_date": "2026-04-11",
        "temp_low": 70,
        "temp_high": 71,
        "side": "SELL",
        "trade_size": 50.0,
        "edge": -0.14,
        "selected_prob": 0.74,
        "entry_price": 0.60,
        "model_expected_high": 64.9,
        "is_contrarian": False,
        "market_prob": 0.40,
        "fee_pct": 0.01,
    }
    trade.update(overrides)
    return trade


def test_build_daily_result_lines_truncates_and_adds_omitted_note():
    details = [_detail(i, won=(i % 2 == 0)) for i in range(20)]

    results_text, omitted = alerts._build_daily_result_lines(details)

    assert omitted > 0
    assert "...and" in results_text
    assert len(results_text) <= alerts.SLACK_DAILY_RESULTS_CHAR_BUDGET


def test_build_daily_result_lines_respects_max_lines_for_small_rows():
    details = [_detail(i) for i in range(12)]

    results_text, omitted = alerts._build_daily_result_lines(details)
    entries = [entry for entry in results_text.split("\n\n") if entry]

    assert len(entries) == alerts.SLACK_DAILY_RESULTS_MAX_LINES + 1
    assert omitted == len(details) - alerts.SLACK_DAILY_RESULTS_MAX_LINES
    assert entries[-1] == f"...and {omitted} more results in report"


def test_alert_daily_summary_falls_back_to_plain_text(monkeypatch):
    sent = {"blocks": None, "text": None}

    def fake_report(details, stats):
        return "/tmp/resolution_2026-04-05.md"

    def fake_blocks(blocks, text=""):
        sent["blocks"] = {"blocks": blocks, "text": text}
        return False

    def fake_message(text):
        sent["text"] = text
        return True

    monkeypatch.setattr(alerts, "save_resolution_report", fake_report)
    monkeypatch.setattr(alerts, "send_slack_blocks", fake_blocks)
    monkeypatch.setattr(alerts, "send_slack_message", fake_message)

    alerts.alert_daily_summary({
        "mode": "paper",
        "date": "2026-04-05",
        "trades_resolved": 20,
        "daily_pnl": -31.72,
        "wins": 12,
        "losses": 8,
        "details": [_detail(i, won=(i % 2 == 0)) for i in range(20)],
        "total_pnl": 123.45,
        "all_time_win_rate": 0.55,
        "all_time_trades": 90,
        "pending_trades": 7,
    })

    assert sent["blocks"] is not None
    results_block = sent["blocks"]["blocks"][3]["text"]["text"]
    assert len(results_block) <= alerts.SLACK_DAILY_RESULTS_CHAR_BUDGET
    assert "...and" in results_block
    assert sent["text"] is not None
    assert "Daily Pulse (PAPER) - 2026-04-05" in sent["text"]
    assert "/tmp/resolution_2026-04-05.md" not in sent["text"]


def test_alert_daily_summary_live_falls_back_to_plain_text(monkeypatch):
    sent = {"blocks": None, "text": None}

    def fake_report(details, stats):
        return "/tmp/resolution_2026-04-05.md"

    def fake_blocks(blocks, text=""):
        sent["blocks"] = {"blocks": blocks, "text": text}
        return False

    def fake_message(text):
        sent["text"] = text
        return True

    monkeypatch.setattr(alerts, "save_resolution_report", fake_report)
    monkeypatch.setattr(alerts, "send_slack_blocks", fake_blocks)
    monkeypatch.setattr(alerts, "send_slack_message", fake_message)
    monkeypatch.setattr(alerts, "_get_live_portfolio_value", lambda: 1003.89)

    alerts.alert_daily_summary({
        "mode": "live",
        "date": "2026-04-05",
        "details": [
            _detail(1, won=True, venue="kalshi"),
            _detail(2, won=False, venue="kalshi") | {"side": "NO"},
        ],
    })

    assert sent["blocks"] is not None
    assert sent["text"] is not None
    assert "Daily Live W/L — Sun Apr 5" in sent["text"]
    assert "Previous day P&L: $+0.00" in sent["text"]
    assert "Wins: 1 (1 buys, 0 sells)" in sent["text"]
    assert "Losses: 1 (0 buys, 1 sells)" in sent["text"]
    assert "Portfolio value: $1,003.89" in sent["text"]


def test_build_trade_reason_uses_temp_gap_when_available():
    reason = alerts._build_trade_reason(
        _trade(side="SELL", model_expected_high=64.9),
        {("los_angeles", "2026-04-11"): {"polymarket_implied_high": 70.8}},
    )

    assert "model 64.9F vs market-implied 70.8F" in reason
    assert "Kalshi looks too hot on this bucket" in reason


def test_build_trade_reason_falls_back_to_probability_gap():
    reason = alerts._build_trade_reason(
        _trade(model_expected_high=None, selected_prob=0.74, entry_price=0.60, side="BUY"),
        {},
    )

    assert "model 74% vs market 60%" in reason
    assert "underpriced" in reason


def test_build_trade_reason_uses_probability_gap_for_open_ended_bucket():
    reason = alerts._build_trade_reason(
        _trade(
            venue="kalshi",
            side="BUY",
            temp_low=-999.0,
            temp_high=60.0,
            model_expected_high=59.2,
            selected_prob=0.57,
            entry_price=0.08,
            market_prob=0.08,
        ),
        {("los_angeles", "2026-04-11"): {"kalshi_implied_high": 63.8}},
    )

    assert "model 57% vs market 8%" in reason
    assert "market-implied" not in reason


def test_build_trade_context_line_for_contrarian_bucket():
    context = alerts._build_trade_context_line(
        _trade(
            venue="kalshi",
            side="BUY",
            temp_low=-999.0,
            temp_high=60.0,
            edge=0.24,
            selected_prob=0.57,
            ensemble_prob=0.57,
            model_expected_high=59.2,
            venue_implied_high=63.9,
            nws_forecast={"temp": 60},
            ensemble_meta={"member_count": 30},
            is_contrarian=True,
        ),
        {},
    )

    assert context == (
        "Context: NWS 60F | Ensemble mean 59.2F | Market center 63.9F | "
        "17/30 members <= 60F"
    )


def test_alert_scan_summary_renders_bet_first_layout(monkeypatch):
    sent = {"blocks": None}

    def fake_report(executed, total_signals, bankroll, mode="paper", comparison_rows=None):
        return "/tmp/scan_2026-04-08_0100.md"

    def fake_blocks(blocks, text=""):
        sent["blocks"] = blocks
        return True

    monkeypatch.setattr(alerts, "save_scan_report", fake_report)
    monkeypatch.setattr(alerts, "send_slack_blocks", fake_blocks)

    alerts.alert_scan_summary(
        executed=[_trade(
            venue="kalshi",
            city="los_angeles",
            target_date="2026-04-09",
            model_expected_high=69.8,
        )],
        total_signals=1,
        bankroll={"kalshi": 950.0, "polymarket": 1000.0},
        mode="paper",
        comparison_rows=[{
            "city": "los_angeles",
            "target_date": "2026-04-09",
            "model_expected_high": 69.8,
            "polymarket_implied_high": 71.2,
            "kalshi_implied_high": 70.9,
            "selected_bets": [{
                "venue": "kalshi",
                "bucket_question": "70-71F",
                "side": "SELL",
            }],
        }],
    )

    bets_view = sent["blocks"][3]["text"]["text"]

    assert "*Bets placed*" in bets_view
    assert "Thu Apr 9 | Los Angeles | $50 -> $82 | Edge 14%" in bets_view
    assert "Betting NO on 70-71°F" in bets_view
    assert "$50 -> $82" in bets_view
    assert "Edge 14%" in bets_view
    assert "Why: model 69.8F vs market-implied 70.9F, so Kalshi looks too hot" in bets_view
    assert "City/date view" not in bets_view
    assert "Venue comparison" not in bets_view


def test_build_trade_alert_entry_includes_context_for_high_edge_bucket():
    entry = alerts._build_trade_alert_entry(
        _trade(
            venue="kalshi",
            city="nyc",
            side="BUY",
            target_date="2026-04-11",
            temp_low=-999.0,
            temp_high=60.0,
            trade_size=30.0,
            edge=0.24,
            selected_prob=0.57,
            ensemble_prob=0.57,
            entry_price=0.08,
            market_prob=0.08,
            fee_pct=0.0,
            model_expected_high=59.2,
            venue_implied_high=63.9,
            nws_forecast={"temp": 60},
            ensemble_meta={"member_count": 30},
            is_contrarian=True,
        ),
        {},
    )

    assert "Betting YES on 60°F or below" in entry
    assert "Why: model 57% vs market 8%" in entry
    assert "Context: NWS 60F | Ensemble mean 59.2F | Market center 63.9F | 17/30 members <= 60F" in entry


def test_alert_scan_summary_orders_by_date_then_edge_and_omits_extra(monkeypatch):
    sent = {"blocks": None}

    def fake_report(executed, total_signals, bankroll, mode="paper", comparison_rows=None):
        return "/tmp/scan_2026-04-08_0100.md"

    def fake_blocks(blocks, text=""):
        sent["blocks"] = blocks
        return True

    monkeypatch.setattr(alerts, "save_scan_report", fake_report)
    monkeypatch.setattr(alerts, "send_slack_blocks", fake_blocks)

    executed = [
        _trade(venue="kalshi", city=f"city_{idx}", target_date="2026-04-11", edge=-(0.20 - idx * 0.01))
        for idx in range(9)
    ]
    executed.append(_trade(venue="kalshi", city="earlier_city", target_date="2026-04-10", edge=-0.05))

    alerts.alert_scan_summary(
        executed=executed,
        total_signals=len(executed),
        bankroll={"kalshi": 1000.0},
        mode="paper",
        comparison_rows=[],
    )

    bets_view = sent["blocks"][3]["text"]["text"]
    earlier_index = bets_view.index("Fri Apr 10 | Earlier City")
    stronger_same_day_index = bets_view.index("Sat Apr 11 | City 0")
    weaker_same_day_index = bets_view.index("Sat Apr 11 | City 6")

    assert earlier_index < stronger_same_day_index < weaker_same_day_index
    assert "...and 2 more bets in report" in bets_view
    assert "(top 8 shown)" in bets_view


def test_alert_scan_summary_filters_non_kalshi_from_slack(monkeypatch):
    sent = {"blocks": None}

    def fake_report(executed, total_signals, bankroll, mode="paper", comparison_rows=None):
        return "/tmp/scan_2026-04-08_0100.md"

    def fake_blocks(blocks, text=""):
        sent["blocks"] = blocks
        return True

    monkeypatch.setattr(alerts, "save_scan_report", fake_report)
    monkeypatch.setattr(alerts, "send_slack_blocks", fake_blocks)

    alerts.alert_scan_summary(
        executed=[
            _trade(venue="polymarket", city="miami"),
            _trade(venue="kalshi", city="nyc"),
        ],
        total_signals=2,
        bankroll={"kalshi": 950.0, "polymarket": 1000.0},
        mode="paper",
        comparison_rows=[],
    )

    summary_view = sent["blocks"][1]["text"]["text"]
    bets_view = sent["blocks"][3]["text"]["text"]

    assert "*1 trades*" in summary_view
    assert "potential payout" in summary_view
    assert "NYC | $50 -> $82" in bets_view
    assert "Miami" not in bets_view


def test_alert_daily_summary_skips_polymarket_only_days(monkeypatch):
    sent = {"blocks": None, "text": None}

    def fake_report(details, stats):
        return "/tmp/resolution_2026-04-05.md"

    def fake_blocks(blocks, text=""):
        sent["blocks"] = blocks
        return True

    def fake_message(text):
        sent["text"] = text
        return True

    monkeypatch.setattr(alerts, "save_resolution_report", fake_report)
    monkeypatch.setattr(alerts, "send_slack_blocks", fake_blocks)
    monkeypatch.setattr(alerts, "send_slack_message", fake_message)

    alerts.alert_daily_summary({
        "date": "2026-04-05",
        "details": [_detail(1, venue="polymarket")],
        "total_pnl": 123.45,
        "all_time_win_rate": 0.55,
        "all_time_trades": 90,
        "pending_trades": 7,
    })

    assert sent["blocks"] is None
    assert sent["text"] is None


def test_alert_daily_summary_uses_live_win_loss_format(monkeypatch):
    sent = {"blocks": None}

    def fake_report(details, stats):
        return "/tmp/resolution_2026-04-05.md"

    def fake_blocks(blocks, text=""):
        sent["blocks"] = blocks
        return True

    monkeypatch.setattr(alerts, "save_resolution_report", fake_report)
    monkeypatch.setattr(alerts, "send_slack_blocks", fake_blocks)
    monkeypatch.setattr(alerts, "_get_live_portfolio_value", lambda: 1003.89)

    alerts.alert_daily_summary({
        "mode": "live",
        "date": "2026-04-05",
        "details": [
            _detail(1, won=True, venue="kalshi"),
            _detail(2, won=True, venue="kalshi") | {"side": "NO", "pnl": 12.0},
            _detail(3, won=False, venue="kalshi") | {"side": "YES", "pnl": -7.0},
        ],
    })

    header_text = sent["blocks"][0]["text"]["text"]
    summary_view = sent["blocks"][1]["text"]["text"]

    assert "Daily Live W/L — Sun Apr 5" == header_text
    assert "Previous day P&L: $+15.00" in summary_view
    assert "Wins: 2 (1 buys, 1 sells)" in summary_view
    assert "Losses: 1 (1 buys, 0 sells)" in summary_view
    assert "Portfolio value: $1,003.89" in summary_view
    assert len(sent["blocks"]) == 2


def test_build_comparison_table_filters_empty_rows():
    table = alerts._build_comparison_table([
        {
            "city": "atlanta",
            "target_date": "2026-04-08",
            "model_expected_high": None,
            "polymarket_implied_high": None,
            "kalshi_implied_high": None,
            "selected_bets": [],
        },
        {
            "city": "miami",
            "target_date": "2026-04-09",
            "model_expected_high": 81.2,
            "polymarket_implied_high": 82.3,
            "kalshi_implied_high": 83.1,
            "selected_bets": [{"venue": "kalshi", "bucket_question": "82-83F", "side": "SELL"}],
        },
    ])

    assert "Atlanta" not in table
    assert "Miami" in table
    assert "81.2F" in table
