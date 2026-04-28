from core import database, reconciliation
from core.database import Trade, session_scope


REPORT_TEXT = """# PAPER Scan Report — Wed Apr 8, 2026 at 03:15 UTC

**2 trades placed** out of 2 signals found

## Kalshi — Wed Apr 8 (2026-04-08)

### Kalshi | Nyc (Wed Apr 8) | 70-71°F

Kalshi prices this at 44%, but our models say 57%. The market is undervaluing this outcome by 13%.

**YES $42 to win $95 (+$53) | Edge 13% | With crowd**

---

## Polymarket — Wed Apr 8 (2026-04-08)

### Polymarket | Miami (Wed Apr 8) | 84-85°F

Polymarket prices this at 62%, but our models say only 27%. The market is overpricing this outcome by 11%.

**NO $28 to win $73 (+$45) | Edge 11% | Against crowd (60% size)**

---
"""


def _reset_test_db(tmp_path, monkeypatch):
    monkeypatch.setattr(database, "DB_PATH", tmp_path / "trades.db")
    database._engine = None
    database._Session = None


def test_parse_scan_report_extracts_kalshi_trade(tmp_path):
    report_path = tmp_path / "scan_2026-04-08_0315.md"
    report_path.write_text(REPORT_TEXT)

    trades = reconciliation.parse_scan_report(report_path, venue="kalshi")

    assert len(trades) == 1
    trade = trades[0]
    assert trade["venue"] == "kalshi"
    assert trade["city"] == "nyc"
    assert trade["target_date"] == "2026-04-08"
    assert trade["side"] == "BUY"
    assert trade["trade_size"] == 42.0
    assert trade["market_prob"] == 0.44
    assert trade["ensemble_prob"] == 0.57
    assert trade["edge"] == 0.13
    assert trade["bucket_question"] == "Will the highest temperature in New York City be 70-71°F on April 8?"


def test_backfill_scan_reports_is_idempotent(tmp_path, monkeypatch):
    _reset_test_db(tmp_path, monkeypatch)

    reports_dir = tmp_path / "reports"
    reports_dir.mkdir()
    (reports_dir / "scan_2026-04-08_0315.md").write_text(REPORT_TEXT)
    (reports_dir / "scan_2026-04-09_0157.md").write_text(REPORT_TEXT)

    first = reconciliation.backfill_scan_reports(report_dir=reports_dir, venue="kalshi", mode="paper")
    second = reconciliation.backfill_scan_reports(report_dir=reports_dir, venue="kalshi", mode="paper")

    assert first["parsed"] == 2
    assert first["inserted"] == 1
    assert first["skipped"] == 1
    assert second["inserted"] == 0
    assert second["skipped"] == 2

    with session_scope() as session:
        rows = [(trade.venue, trade.city, str(trade.timestamp)) for trade in session.query(Trade).all()]

    assert len(rows) == 1
    assert rows[0][0] == "kalshi"
    assert rows[0][1] == "nyc"
    assert rows[0][2].startswith("2026-04-08 03:15:00")

    database._engine = None
    database._Session = None


class _FakeKalshiClient:
    def get_fills(self, *, limit=200):
        return [
            {
                "order_id": "order-1",
                "ticker": "KXHIGHAUS-26APR13-B82.5",
                "side": "yes",
                "count": 5,
                "yes_price_dollars": 0.16,
                "no_price_dollars": 0.84,
                "fee_cost": 0.05,
                "created_time": "2026-04-12T20:00:00Z",
            },
            {
                "order_id": "order-1",
                "ticker": "KXHIGHAUS-26APR13-B82.5",
                "side": "yes",
                "count": 3,
                "yes_price_dollars": 0.16,
                "no_price_dollars": 0.84,
                "fee_cost": 0.03,
                "created_time": "2026-04-12T20:00:03Z",
            },
        ]

    def get_market(self, ticker):
        assert ticker == "KXHIGHAUS-26APR13-B82.5"
        return {
            "ticker": ticker,
            "event_ticker": "KXHIGHAUS-26APR13",
            "series_ticker": "KXHIGHAUS",
            "title": "Highest temperature in Austin on 2026-04-13",
            "subtitle": "82° to 83°",
            "yes_ask_dollars": "0.16",
            "no_ask_dollars": "0.84",
        }


def test_backfill_kalshi_live_fills_is_idempotent(tmp_path, monkeypatch):
    _reset_test_db(tmp_path, monkeypatch)

    summary = reconciliation.backfill_kalshi_live_fills(client=_FakeKalshiClient())
    assert summary["parsed"] == 1
    assert summary["inserted"] == 1
    assert summary["skipped"] == 0

    second = reconciliation.backfill_kalshi_live_fills(client=_FakeKalshiClient())
    assert second["parsed"] == 1
    assert second["inserted"] == 0
    assert second["skipped"] == 1

    with session_scope() as session:
        trade = session.query(Trade).one()
        assert trade.mode == "live"
        assert trade.venue == "kalshi"
        assert trade.city == "austin"
        assert trade.target_date == "2026-04-13"
        assert trade.venue_order_id == "order-1"
        assert trade.filled_contracts == 8
        assert trade.filled_size_usd == 1.28
        assert trade.fee_usd == 0.08
        assert trade.entry_price == 0.16
        assert trade.fill_price == 0.16
        assert trade.bucket_question == "82° to 83°"

    database._engine = None
    database._Session = None
