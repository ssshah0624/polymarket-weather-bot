from core import alerts


def _detail(index: int, won: bool = True) -> dict:
    return {
        "city": f"City {index}",
        "bucket": f"Bucket range {index} with extra words to force length",
        "actual_temp_f": 70 + index,
        "pnl": 10.0 if won else -10.0,
        "won": won,
    }


def test_build_daily_result_lines_truncates_and_adds_omitted_note():
    details = [_detail(i, won=(i % 2 == 0)) for i in range(20)]

    results_text, omitted = alerts._build_daily_result_lines(details)

    assert omitted > 0
    assert "...and" in results_text
    assert len(results_text) <= alerts.SLACK_DAILY_RESULTS_CHAR_BUDGET


def test_build_daily_result_lines_respects_max_lines_for_small_rows():
    details = [_detail(i) for i in range(12)]

    results_text, omitted = alerts._build_daily_result_lines(details)
    lines = results_text.splitlines()

    assert len(lines) == alerts.SLACK_DAILY_RESULTS_MAX_LINES + 1
    assert omitted == len(details) - alerts.SLACK_DAILY_RESULTS_MAX_LINES
    assert lines[-1] == f"...and {omitted} more results in report"


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
    assert "Daily Pulse - 2026-04-05" in sent["text"]
    assert "/tmp/resolution_2026-04-05.md" in sent["text"]
