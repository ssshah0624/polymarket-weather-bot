import pytest

from core import database
from core.database import Trade, log_trade, session_scope
from core.execution.kalshi_client import (
    BalanceSnapshot,
    KalshiClient,
    KalshiClientError,
    PortfolioExposureSnapshot,
)
from core.execution import live


@pytest.fixture(autouse=True)
def _baseline_live_flags(monkeypatch):
    monkeypatch.setattr(live, "KALSHI_LIVE_SCALE_TO_AVAILABLE_CASH", False)
    monkeypatch.setattr(live, "KALSHI_LIVE_EMPIRICAL_RANKING_ENABLED", False)
    monkeypatch.setattr(live, "KALSHI_LIVE_SAME_DAY_TOP_UP_ENABLED", False)
    monkeypatch.setattr(live, "KALSHI_LIVE_NEXT_DAY_CAPITAL_PCT", 0.65)
    monkeypatch.setattr(live, "KALSHI_LIVE_MIN_CASH_BUFFER_USD", 5.0)
    monkeypatch.setattr(live, "KALSHI_LIVE_TARGET_TOLERANCE_USD", 10.0)
    monkeypatch.setattr(live, "KALSHI_LIVE_CONTRARIAN_MAX_TRADE_SIZE_USD", 5.0)
    monkeypatch.setattr(live, "KALSHI_LIVE_MAX_BUY_TRADE_SIZE_USD", live.KALSHI_LIVE_MAX_TRADE_SIZE_USD)
    monkeypatch.setattr(live, "KALSHI_LIVE_MAX_SELL_TRADE_SIZE_USD", live.KALSHI_LIVE_MAX_TRADE_SIZE_USD)
    monkeypatch.setattr(live, "KALSHI_LIVE_BUDGET_ALLOCATION_USD", live.KALSHI_LIVE_BANKROLL_SLICE_USD)


@pytest.fixture(autouse=True)
def _isolated_test_db(monkeypatch, tmp_path):
    monkeypatch.setattr(database, "DB_PATH", tmp_path / "trades.db")
    database._engine = None
    database._Session = None
    yield
    database._engine = None
    database._Session = None


class _FakeSession:
    def __init__(self):
        self.calls = []

    def request(self, method, url, headers=None, params=None, json=None, timeout=None):
        self.calls.append({
            "method": method,
            "url": url,
            "headers": headers or {},
            "params": params,
            "json": json,
            "timeout": timeout,
        })

        class _Response:
            status_code = 200
            content = b"{}"
            text = "{}"

            @staticmethod
            def json():
                return {}

        return _Response()


class _FakeClient:
    def __init__(self, market_yes=0.10, market_no=0.70):
        self.market_yes = market_yes
        self.market_no = market_no
        self.order_calls = []

    def get_balance(self):
        return BalanceSnapshot(available_cash_usd=500.0, balance_usd=500.0, raw={})

    def get_portfolio_exposure(self):
        return PortfolioExposureSnapshot(
            total_cost_usd=0.0,
            market_value_usd=0.0,
            open_positions=0,
            raw={},
        )

    def get_market(self, ticker: str):
        return {
            "ticker": ticker,
            "yes_ask_dollars": f"{self.market_yes:.2f}",
            "no_ask_dollars": f"{self.market_no:.2f}",
            "close_time": "2026-12-13T23:00:00Z",
        }

    def place_marketable_buy(self, **kwargs):
        self.order_calls.append(kwargs)
        return {
            "latest_order": {
                "order_id": "ord-1",
                "status": "executed",
                "created_time": "2026-04-12T01:00:00Z",
                "last_update_time": "2026-04-12T01:00:01Z",
            },
            "fills": [{
                "count_fp": f"{kwargs['count']:.2f}",
                "yes_price_dollars": f"{kwargs['limit_price']:.4f}",
                "fee_cost": "0.50",
                "created_time": "2026-04-12T01:00:01Z",
            }],
        }

    def summarize_fill(self, *, order, fills, side, expected_entry_price):
        fill_count = int(float(fills[0]["count_fp"]))
        fill_price = float(fills[0]["yes_price_dollars"])
        return {
            "order_status": order["status"],
            "filled_contracts": fill_count,
            "filled_size_usd": round(fill_count * fill_price, 2),
            "fill_price": fill_price,
            "fee_usd": 0.50,
            "submitted_at": order["created_time"],
            "filled_at": order["last_update_time"],
            "adverse_drift_cents": round((fill_price - expected_entry_price) * 100, 2),
        }


class _FoKConflictClient(_FakeClient):
    def place_marketable_buy(self, **kwargs):
        raise KalshiClientError(
            'Kalshi POST /portfolio/orders failed: 409 {"error":{"code":"fill_or_kill_insufficient_resting_volume","message":"fill or kill insufficient resting volume"}}'
        )


class _ClientErrorAfterOrderClient(_FakeClient):
    def place_marketable_buy(self, **kwargs):
        raise KalshiClientError(
            'Kalshi GET /portfolio/orders/ord-1 failed: 404 {"error":{"code":"not_found","message":"not found"}}'
        )


class _ExistingExposureClient(_FakeClient):
    def get_portfolio_exposure(self):
        return PortfolioExposureSnapshot(
            total_cost_usd=40.0,
            market_value_usd=22.0,
            open_positions=2,
            raw={},
        )


class _LargeOpenBookClient(_FakeClient):
    def get_portfolio_exposure(self):
        return PortfolioExposureSnapshot(
            total_cost_usd=40.0,
            market_value_usd=22.0,
            open_positions=9,
            raw={},
        )


class _TrackedExposureMismatchClient(_FakeClient):
    def get_portfolio_exposure(self):
        return PortfolioExposureSnapshot(
            total_cost_usd=24.53,
            market_value_usd=19.0,
            open_positions=20,
            raw={},
        )


def _signal():
    return {
        "venue": "kalshi",
        "venue_market_id": "KXHIGHNY-26APR13",
        "event_id": "KXHIGHNY-26APR13",
        "city": "nyc",
        "target_date": "2026-04-13",
        "bucket_question": "60° or below",
        "side": "BUY",
        "trade_size": 30.0,
        "entry_price": 0.08,
        "selected_prob": 0.57,
        "ensemble_prob": 0.57,
        "edge": 0.24,
        "is_contrarian": True,
        "nws_forecast": {"temp": 60, "unit": "F"},
        "ensemble_meta": {"member_count": 30, "mean": 59.2},
    }


def test_kalshi_client_request_adds_auth_headers(monkeypatch):
    session = _FakeSession()
    client = KalshiClient(
        api_key_id="key-id",
        private_key_path="unused",
        session=session,
        private_key=object(),
    )
    monkeypatch.setattr(client, "_create_signature", lambda timestamp, method, path: "sig")

    client._request("GET", "/portfolio/balance", auth_required=True)

    call = session.calls[0]
    assert call["headers"]["KALSHI-ACCESS-KEY"] == "key-id"
    assert call["headers"]["KALSHI-ACCESS-SIGNATURE"] == "sig"
    assert "KALSHI-ACCESS-TIMESTAMP" in call["headers"]


def test_live_trader_skips_when_quote_drifts_too_far(monkeypatch):
    monkeypatch.setattr(live, "KALSHI_LIVE_ENABLED", True)
    monkeypatch.setattr(live, "KALSHI_LIVE_MAX_TRADE_SIZE_USD", 30.0)
    monkeypatch.setattr(live, "scan_all_markets", lambda **_: {"signals": [_signal()], "comparisons": []})
    monkeypatch.setattr(live, "get_realized_pnl_for_trading_day", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(live, "get_trade_cost_for_trading_day", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(live, "get_trade_count_for_trading_day", lambda *args, **kwargs: 0)
    monkeypatch.setattr(live, "get_open_exposure_usd", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(live, "get_unresolved_trades", lambda *args, **kwargs: [])
    monkeypatch.setattr(live, "get_traded_buckets", lambda *args, **kwargs: set())
    monkeypatch.setattr(live, "log_weather_comparison_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(live, "alert_scan_summary", lambda *args, **kwargs: None)

    client = _FakeClient(market_yes=0.12)
    trader = live.LiveTrader(client=client)

    executed = trader.run_scan_cycle()

    assert executed == []
    assert client.order_calls == []
    assert trader.last_decision_summary["kalshi_candidates"] == 1
    assert trader.last_decision_summary["skip_counts"]["quote_drift"] == 1


def test_live_trader_logs_filled_trade(monkeypatch):
    logged = []
    monkeypatch.setattr(live, "KALSHI_LIVE_ENABLED", True)
    monkeypatch.setattr(live, "KALSHI_LIVE_MAX_TRADE_SIZE_USD", 30.0)
    monkeypatch.setattr(live, "KALSHI_LIVE_ALLOWED_DRIFT_CENTS", 2)
    monkeypatch.setattr(live, "scan_all_markets", lambda **_: {"signals": [_signal()], "comparisons": []})
    monkeypatch.setattr(live, "get_realized_pnl_for_trading_day", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(live, "get_trade_cost_for_trading_day", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(live, "get_trade_count_for_trading_day", lambda *args, **kwargs: 0)
    monkeypatch.setattr(live, "get_open_exposure_usd", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(live, "get_unresolved_trades", lambda *args, **kwargs: [])
    monkeypatch.setattr(live, "get_traded_buckets", lambda *args, **kwargs: set())
    monkeypatch.setattr(live, "log_weather_comparison_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(live, "alert_scan_summary", lambda *args, **kwargs: None)
    monkeypatch.setattr(live, "log_trade", lambda signal, mode="live": logged.append((signal, mode)))

    client = _FakeClient(market_yes=0.10)
    trader = live.LiveTrader(client=client)

    executed = trader.run_scan_cycle()

    assert len(executed) == 1
    signal = executed[0]
    assert signal["filled_contracts"] == 50
    assert signal["filled_size_usd"] == 5.0
    assert signal["fill_price"] == 0.10
    assert signal["expected_entry_price"] == 0.08
    assert signal["client_order_id"]
    assert logged[0][1] == "live"
    assert logged[0][0]["forecast_context"]["fill_price"] == 0.10
    assert trader.last_decision_summary["filled"] == 1


def test_live_trader_uses_side_specific_trade_caps(monkeypatch):
    monkeypatch.setattr(live, "KALSHI_LIVE_ENABLED", True)
    monkeypatch.setattr(live, "KALSHI_LIVE_MAX_BUY_TRADE_SIZE_USD", 10.0)
    monkeypatch.setattr(live, "KALSHI_LIVE_MAX_SELL_TRADE_SIZE_USD", 30.0)
    monkeypatch.setattr(live, "KALSHI_LIVE_CONTRARIAN_MAX_TRADE_SIZE_USD", 6.0)

    trader = live.LiveTrader(client=_FakeClient())

    assert trader._max_signal_size({**_signal(), "side": "BUY", "is_contrarian": False}) == 10.0
    assert trader._max_signal_size({**_signal(), "side": "SELL", "is_contrarian": False}) == 30.0
    assert trader._max_signal_size({**_signal(), "side": "SELL", "is_contrarian": True}) == 6.0


def test_live_trader_scales_planned_package_toward_remaining_budget(monkeypatch):
    logged = []
    signals = [
        {
            **_signal(),
            "trade_size": 5.0,
            "entry_price": 0.10,
            "is_contrarian": False,
            "edge": 0.18,
        },
        {
            **_signal(),
            "city": "austin",
            "bucket_question": "82° to 83°",
            "event_id": "KXHIGHAUS-26APR13",
            "venue_market_id": "KXHIGHAUS-26APR13",
            "is_contrarian": False,
            "trade_size": 5.0,
            "entry_price": 0.10,
            "edge": 0.18,
        },
    ]
    monkeypatch.setattr(live, "KALSHI_LIVE_ENABLED", True)
    monkeypatch.setattr(live, "KALSHI_LIVE_MAX_TRADE_SIZE_USD", 30.0)
    monkeypatch.setattr(live, "KALSHI_LIVE_MAX_BUY_TRADE_SIZE_USD", 30.0)
    monkeypatch.setattr(live, "KALSHI_LIVE_MAX_SELL_TRADE_SIZE_USD", 30.0)
    monkeypatch.setattr(live, "KALSHI_LIVE_CONTRARIAN_MAX_TRADE_SIZE_USD", 30.0)
    monkeypatch.setattr(live, "KALSHI_LIVE_ALLOWED_DRIFT_CENTS", 2)
    monkeypatch.setattr(live, "KALSHI_LIVE_BANKROLL_SLICE_USD", 30.0)
    monkeypatch.setattr(live, "KALSHI_LIVE_MAX_DAILY_LOSS_USD", 30.0)
    monkeypatch.setattr(live, "KALSHI_LIVE_MAX_OPEN_EXPOSURE_USD", 30.0)
    monkeypatch.setattr(live, "scan_all_markets", lambda **_: {"signals": signals, "comparisons": []})
    monkeypatch.setattr(live, "get_realized_pnl_for_trading_day", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(live, "get_trade_cost_for_trading_day", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(live, "get_trade_count_for_trading_day", lambda *args, **kwargs: 0)
    monkeypatch.setattr(live, "get_open_exposure_usd", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(live, "get_unresolved_trades", lambda *args, **kwargs: [])
    monkeypatch.setattr(live, "get_traded_buckets", lambda *args, **kwargs: set())
    monkeypatch.setattr(live, "log_weather_comparison_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(live, "alert_scan_summary", lambda *args, **kwargs: None)
    monkeypatch.setattr(live, "log_trade", lambda signal, mode="live": logged.append((signal, mode)))

    trader = live.LiveTrader(client=_FakeClient(market_yes=0.10, market_no=0.70))
    executed = trader.run_scan_cycle()

    assert len(executed) == 2
    assert round(sum(signal["intended_size_usd"] for signal in executed), 2) == 30.0
    assert executed[0]["forecast_context"]["base_trade_size"] == 5.0
    assert executed[0]["forecast_context"]["target_trade_size"] == 15.0
    assert executed[1]["forecast_context"]["target_trade_size"] == 15.0
    assert trader.last_decision_summary["planned_trade_size_usd"] == 30.0
    assert trader.last_decision_summary["planned_target_budget_usd"] == 30.0
    assert logged[0][0]["intended_size_usd"] == 15.0


def test_live_trader_daily_loss_limit_does_not_cap_spend_target(monkeypatch):
    logged = []
    signals = [
        {
            **_signal(),
            "trade_size": 5.0,
            "entry_price": 0.10,
            "is_contrarian": False,
            "edge": 0.18,
        },
        {
            **_signal(),
            "city": "austin",
            "bucket_question": "82° to 83°",
            "event_id": "KXHIGHAUS-26APR13",
            "venue_market_id": "KXHIGHAUS-26APR13",
            "is_contrarian": False,
            "trade_size": 5.0,
            "entry_price": 0.10,
            "edge": 0.18,
        },
    ]
    monkeypatch.setattr(live, "KALSHI_LIVE_ENABLED", True)
    monkeypatch.setattr(live, "KALSHI_LIVE_MAX_TRADE_SIZE_USD", 30.0)
    monkeypatch.setattr(live, "KALSHI_LIVE_MAX_BUY_TRADE_SIZE_USD", 30.0)
    monkeypatch.setattr(live, "KALSHI_LIVE_MAX_SELL_TRADE_SIZE_USD", 30.0)
    monkeypatch.setattr(live, "KALSHI_LIVE_CONTRARIAN_MAX_TRADE_SIZE_USD", 30.0)
    monkeypatch.setattr(live, "KALSHI_LIVE_ALLOWED_DRIFT_CENTS", 2)
    monkeypatch.setattr(live, "KALSHI_LIVE_BANKROLL_SLICE_USD", 50.0)
    monkeypatch.setattr(live, "KALSHI_LIVE_MAX_DAILY_LOSS_USD", 20.0)
    monkeypatch.setattr(live, "KALSHI_LIVE_MAX_OPEN_EXPOSURE_USD", 50.0)
    monkeypatch.setattr(live, "scan_all_markets", lambda **_: {"signals": signals, "comparisons": []})
    monkeypatch.setattr(live, "get_realized_pnl_for_trading_day", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(live, "get_trade_cost_for_trading_day", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(live, "get_trade_count_for_trading_day", lambda *args, **kwargs: 0)
    monkeypatch.setattr(live, "get_open_exposure_usd", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(live, "get_unresolved_trades", lambda *args, **kwargs: [])
    monkeypatch.setattr(live, "get_traded_buckets", lambda *args, **kwargs: set())
    monkeypatch.setattr(live, "log_weather_comparison_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(live, "alert_scan_summary", lambda *args, **kwargs: None)
    monkeypatch.setattr(live, "log_trade", lambda signal, mode="live": logged.append((signal, mode)))

    trader = live.LiveTrader(client=_FakeClient(market_yes=0.10, market_no=0.70))
    executed = trader.run_scan_cycle()

    assert len(executed) == 2
    assert round(sum(signal["intended_size_usd"] for signal in executed), 2) == 50.0
    assert trader.last_decision_summary["planned_target_budget_usd"] == 50.0
    assert trader.last_decision_summary["planned_trade_size_usd"] == 50.0


def test_live_trader_scales_target_to_available_cash_when_enabled(monkeypatch):
    logged = []
    signals = [
        {
            **_signal(),
            "trade_size": 5.0,
            "entry_price": 0.10,
            "is_contrarian": False,
            "edge": 0.18,
        },
        {
            **_signal(),
            "city": "austin",
            "bucket_question": "82° to 83°",
            "event_id": "KXHIGHAUS-26APR13",
            "venue_market_id": "KXHIGHAUS-26APR13",
            "is_contrarian": False,
            "trade_size": 5.0,
            "entry_price": 0.10,
            "edge": 0.18,
        },
    ]
    monkeypatch.setattr(live, "KALSHI_LIVE_ENABLED", True)
    monkeypatch.setattr(live, "KALSHI_LIVE_SCALE_TO_AVAILABLE_CASH", True)
    monkeypatch.setattr(live, "KALSHI_LIVE_BUDGET_ALLOCATION_USD", 100.0)
    monkeypatch.setattr(live, "KALSHI_LIVE_BANKROLL_SLICE_USD", 100.0)
    monkeypatch.setattr(live, "KALSHI_LIVE_MAX_TRADE_SIZE_USD", 300.0)
    monkeypatch.setattr(live, "KALSHI_LIVE_MAX_BUY_TRADE_SIZE_USD", 300.0)
    monkeypatch.setattr(live, "KALSHI_LIVE_MAX_SELL_TRADE_SIZE_USD", 300.0)
    monkeypatch.setattr(live, "KALSHI_LIVE_CONTRARIAN_MAX_TRADE_SIZE_USD", 300.0)
    monkeypatch.setattr(live, "KALSHI_LIVE_ALLOWED_DRIFT_CENTS", 2)
    monkeypatch.setattr(live, "KALSHI_LIVE_MIN_CASH_BUFFER_USD", 5.0)
    monkeypatch.setattr(live, "KALSHI_LIVE_MAX_OPEN_EXPOSURE_USD", 500.0)
    monkeypatch.setattr(live, "scan_all_markets", lambda **_: {"signals": signals, "comparisons": []})
    monkeypatch.setattr(live, "get_realized_pnl_for_trading_day", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(live, "get_trade_cost_for_trading_day", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(live, "get_trade_count_for_trading_day", lambda *args, **kwargs: 0)
    monkeypatch.setattr(live, "get_open_exposure_usd", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(live, "get_unresolved_trades", lambda *args, **kwargs: [])
    monkeypatch.setattr(live, "get_traded_buckets", lambda *args, **kwargs: set())
    monkeypatch.setattr(live, "log_weather_comparison_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(live, "alert_scan_summary", lambda *args, **kwargs: None)
    monkeypatch.setattr(live, "log_trade", lambda signal, mode="live": logged.append((signal, mode)))

    trader = live.LiveTrader(client=_FakeClient(market_yes=0.10, market_no=0.70))
    executed = trader.run_scan_cycle()

    assert len(executed) == 2
    assert round(sum(signal["intended_size_usd"] for signal in executed), 2) == 100.0
    assert trader.last_decision_summary["planned_target_budget_usd"] == 100.0
    assert trader.last_decision_summary["planned_trade_size_usd"] == 100.0
    assert trader.last_decision_summary["scale_to_available_cash"] is True


def test_live_trader_enables_same_day_top_up_below_target_band(monkeypatch):
    captured = {}

    def _scan(**kwargs):
        captured.update(kwargs)
        return {"signals": [], "comparisons": []}

    monkeypatch.setattr(live, "KALSHI_LIVE_ENABLED", True)
    monkeypatch.setattr(live, "KALSHI_LIVE_BANKROLL_SLICE_USD", 50.0)
    monkeypatch.setattr(live, "KALSHI_LIVE_TARGET_TOLERANCE_USD", 10.0)
    monkeypatch.setattr(live, "KALSHI_LIVE_SAME_DAY_TOP_UP_ENABLED", True)
    monkeypatch.setattr(live, "scan_all_markets", _scan)
    monkeypatch.setattr(live, "get_realized_pnl_for_trading_day", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(live, "get_trade_cost_for_trading_day", lambda *args, **kwargs: 20.0)
    monkeypatch.setattr(live, "get_trade_count_for_trading_day", lambda *args, **kwargs: 2)
    monkeypatch.setattr(live, "get_open_exposure_usd", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(live, "get_unresolved_trades", lambda *args, **kwargs: [])
    monkeypatch.setattr(live, "get_traded_buckets", lambda *args, **kwargs: set())
    monkeypatch.setattr(live, "log_weather_comparison_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(live, "alert_scan_summary", lambda *args, **kwargs: None)

    trader = live.LiveTrader(client=_FakeClient())
    trader.run_scan_cycle()

    assert captured["allow_same_day_live"] is True
    assert trader.last_decision_summary["same_day_top_up_enabled"] is True
    assert trader.last_decision_summary["target_lower_bound_usd"] == 40.0


def test_live_trader_keeps_same_day_shadow_at_target_band(monkeypatch):
    captured = {}

    def _scan(**kwargs):
        captured.update(kwargs)
        return {"signals": [], "comparisons": []}

    monkeypatch.setattr(live, "KALSHI_LIVE_ENABLED", True)
    monkeypatch.setattr(live, "KALSHI_LIVE_BANKROLL_SLICE_USD", 50.0)
    monkeypatch.setattr(live, "KALSHI_LIVE_TARGET_TOLERANCE_USD", 10.0)
    monkeypatch.setattr(live, "KALSHI_LIVE_SAME_DAY_TOP_UP_ENABLED", True)
    monkeypatch.setattr(live, "scan_all_markets", _scan)
    monkeypatch.setattr(live, "get_realized_pnl_for_trading_day", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(live, "get_trade_cost_for_trading_day", lambda *args, **kwargs: 40.0)
    monkeypatch.setattr(live, "get_trade_count_for_trading_day", lambda *args, **kwargs: 4)
    monkeypatch.setattr(live, "get_open_exposure_usd", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(live, "get_unresolved_trades", lambda *args, **kwargs: [])
    monkeypatch.setattr(live, "get_traded_buckets", lambda *args, **kwargs: set())
    monkeypatch.setattr(live, "log_weather_comparison_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(live, "alert_scan_summary", lambda *args, **kwargs: None)

    trader = live.LiveTrader(client=_FakeClient())
    trader.run_scan_cycle()

    assert captured["allow_same_day_live"] is False
    assert trader.last_decision_summary["same_day_top_up_enabled"] is False


def test_live_trader_scale_to_available_cash_keeps_same_day_top_up_on(monkeypatch):
    captured = {}

    def _scan(**kwargs):
        captured.update(kwargs)
        return {"signals": [], "comparisons": []}

    monkeypatch.setattr(live, "KALSHI_LIVE_ENABLED", True)
    monkeypatch.setattr(live, "KALSHI_LIVE_SCALE_TO_AVAILABLE_CASH", True)
    monkeypatch.setattr(live, "KALSHI_LIVE_SAME_DAY_TOP_UP_ENABLED", True)
    monkeypatch.setattr(live, "scan_all_markets", _scan)
    monkeypatch.setattr(live, "get_realized_pnl_for_trading_day", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(live, "get_trade_cost_for_trading_day", lambda *args, **kwargs: 60.0)
    monkeypatch.setattr(live, "get_trade_count_for_trading_day", lambda *args, **kwargs: 6)
    monkeypatch.setattr(live, "get_open_exposure_usd", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(live, "get_unresolved_trades", lambda *args, **kwargs: [])
    monkeypatch.setattr(live, "get_traded_buckets", lambda *args, **kwargs: set())
    monkeypatch.setattr(live, "log_weather_comparison_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(live, "alert_scan_summary", lambda *args, **kwargs: None)

    trader = live.LiveTrader(client=_FakeClient())
    trader.run_scan_cycle()

    assert captured["allow_same_day_live"] is True
    assert trader.last_decision_summary["same_day_top_up_enabled"] is True
    assert trader.last_decision_summary["scale_to_available_cash"] is True


def test_live_trader_pools_budget_between_next_day_and_same_day(monkeypatch):
    next_day_signal = {
        **_signal(),
        "trade_size": 10.0,
        "entry_price": 0.10,
        "event_id": "next-day",
        "forecast_context": {"forecast_lead_bucket": "24-36h"},
    }
    same_day_signal = {
        **_signal(),
        "city": "austin",
        "bucket_question": "82° to 83°",
        "event_id": "same-day",
        "venue_market_id": "same-day-market",
        "trade_size": 10.0,
        "entry_price": 0.10,
        "forecast_context": {"same_day_live": True, "forecast_lead_bucket": "12-24h"},
    }
    monkeypatch.setattr(live, "KALSHI_LIVE_ENABLED", True)
    monkeypatch.setattr(live, "KALSHI_LIVE_SCALE_TO_AVAILABLE_CASH", True)
    monkeypatch.setattr(live, "KALSHI_LIVE_SAME_DAY_TOP_UP_ENABLED", True)
    monkeypatch.setattr(live, "KALSHI_LIVE_BUDGET_ALLOCATION_USD", 100.0)
    monkeypatch.setattr(live, "KALSHI_LIVE_BANKROLL_SLICE_USD", 100.0)
    monkeypatch.setattr(live, "KALSHI_LIVE_NEXT_DAY_CAPITAL_PCT", 0.65)
    monkeypatch.setattr(live, "KALSHI_LIVE_MAX_TRADE_SIZE_USD", 100.0)
    monkeypatch.setattr(live, "KALSHI_LIVE_MAX_BUY_TRADE_SIZE_USD", 100.0)
    monkeypatch.setattr(live, "KALSHI_LIVE_MAX_SELL_TRADE_SIZE_USD", 100.0)
    monkeypatch.setattr(live, "KALSHI_LIVE_CONTRARIAN_MAX_TRADE_SIZE_USD", 100.0)
    monkeypatch.setattr(live, "KALSHI_LIVE_MAX_OPEN_EXPOSURE_USD", 500.0)
    monkeypatch.setattr(live, "KALSHI_LIVE_EMPIRICAL_RANKING_ENABLED", False)
    monkeypatch.setattr(
        live,
        "scan_all_markets",
        lambda **_: {"signals": [next_day_signal, same_day_signal], "comparisons": []},
    )
    monkeypatch.setattr(live, "get_realized_pnl_for_trading_day", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(live, "get_trade_cost_for_trading_day", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(live, "get_trade_count_for_trading_day", lambda *args, **kwargs: 0)
    monkeypatch.setattr(live, "get_open_exposure_usd", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(live, "get_unresolved_trades", lambda *args, **kwargs: [])
    monkeypatch.setattr(live, "get_traded_buckets", lambda *args, **kwargs: set())
    monkeypatch.setattr(live, "log_weather_comparison_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(live, "alert_scan_summary", lambda *args, **kwargs: None)

    trader = live.LiveTrader(client=_FakeClient(market_yes=0.10, market_no=0.70))
    executed = trader.run_scan_cycle()

    assert len(executed) == 2
    assert trader.last_decision_summary["next_day_pool_reserve_usd"] == 65.0
    assert trader.last_decision_summary["same_day_pool_reserve_usd"] == 35.0
    assert trader.last_decision_summary["next_day_pool_target_usd"] == 65.0
    assert trader.last_decision_summary["same_day_pool_target_usd"] == 35.0
    assert trader.last_decision_summary["next_day_planned_usd"] == 65.0
    assert trader.last_decision_summary["same_day_planned_usd"] == 35.0
    assert round(sum(signal["intended_size_usd"] for signal in executed), 2) == 100.0


def test_live_trader_empirical_weighting_reorders_candidates(monkeypatch):
    signals = [
        {
            **_signal(),
            "city": "nyc",
            "bucket_question": "78° to 79°",
            "edge": 0.30,
            "side": "BUY",
            "trade_size": 10.0,
            "forecast_context": {"event_selection": "hedged", "forecast_lead_bucket": "24-36h"},
        },
        {
            **_signal(),
            "city": "chicago",
            "bucket_question": "80° to 81°",
            "event_id": "KXHIGHCHI-26APR13",
            "venue_market_id": "KXHIGHCHI-26APR13",
            "edge": 0.15,
            "side": "SELL",
            "trade_size": 10.0,
            "forecast_context": {"event_selection": "hedged", "forecast_lead_bucket": "12-24h"},
        },
    ]
    empirical_stats = {
        "baseline": {"trades": 20, "stake": 100.0, "pnl": 10.0},
        "event_selection": {"hedged": {"trades": 20, "stake": 100.0, "pnl": 10.0}},
        "edge_bucket": {
            "30%+": {"trades": 10, "stake": 40.0, "pnl": 0.0},
            "10-20%": {"trades": 10, "stake": 40.0, "pnl": 12.0},
        },
        "side": {
            "BUY": {"trades": 10, "stake": 40.0, "pnl": -8.0},
            "SELL": {"trades": 10, "stake": 40.0, "pnl": 12.0},
        },
        "city": {
            "nyc": {"trades": 6, "stake": 24.0, "pnl": -6.0},
            "chicago": {"trades": 6, "stake": 24.0, "pnl": 8.0},
        },
        "lead_bucket": {
            "24-36h": {"trades": 12, "stake": 50.0, "pnl": 6.0},
            "12-24h": {"trades": 8, "stake": 30.0, "pnl": 10.0},
        },
        "stance": {"consensus": {"trades": 20, "stake": 100.0, "pnl": 10.0}},
    }
    monkeypatch.setattr(live, "KALSHI_LIVE_ENABLED", True)
    monkeypatch.setattr(live, "KALSHI_LIVE_EMPIRICAL_RANKING_ENABLED", True)
    monkeypatch.setattr(live.LiveTrader, "_load_empirical_live_segment_stats", lambda self: empirical_stats)

    trader = live.LiveTrader(client=_FakeClient())
    decision_summary = {}
    weighted = trader._apply_empirical_live_weights(signals, decision_summary)

    assert weighted[0]["city"] == "chicago"
    assert weighted[0]["trade_size"] > weighted[1]["trade_size"]
    assert decision_summary["empirical_ranking_reference_trades"] == 20


def test_live_trader_skips_duplicate_unresolved_buckets(monkeypatch):
    monkeypatch.setattr(live, "KALSHI_LIVE_ENABLED", True)
    monkeypatch.setattr(live, "scan_all_markets", lambda **_: {"signals": [_signal()], "comparisons": []})
    monkeypatch.setattr(live, "get_realized_pnl_for_trading_day", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(live, "get_trade_cost_for_trading_day", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(live, "get_trade_count_for_trading_day", lambda *args, **kwargs: 0)
    monkeypatch.setattr(live, "get_open_exposure_usd", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(live, "get_unresolved_trades", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        live,
        "get_traded_buckets",
        lambda *args, **kwargs: {("kalshi", "nyc", "2026-04-13", "60° or below")},
    )
    monkeypatch.setattr(live, "log_weather_comparison_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(live, "alert_scan_summary", lambda *args, **kwargs: None)

    trader = live.LiveTrader(client=_FakeClient())
    executed = trader.run_scan_cycle()

    assert executed == []
    assert trader.last_decision_summary["skip_counts"]["duplicate_unresolved"] == 1


def test_live_trader_classifies_fill_or_kill_conflict_as_no_fill(monkeypatch):
    monkeypatch.setattr(live, "KALSHI_LIVE_ENABLED", True)
    monkeypatch.setattr(live, "scan_all_markets", lambda **_: {"signals": [_signal()], "comparisons": []})
    monkeypatch.setattr(live, "get_realized_pnl_for_trading_day", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(live, "get_trade_cost_for_trading_day", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(live, "get_trade_count_for_trading_day", lambda *args, **kwargs: 0)
    monkeypatch.setattr(live, "get_open_exposure_usd", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(live, "get_unresolved_trades", lambda *args, **kwargs: [])
    monkeypatch.setattr(live, "get_traded_buckets", lambda *args, **kwargs: set())
    monkeypatch.setattr(live, "log_weather_comparison_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(live, "alert_scan_summary", lambda *args, **kwargs: None)

    trader = live.LiveTrader(client=_FoKConflictClient())
    executed = trader.run_scan_cycle()

    assert executed == []
    assert trader.last_decision_summary["skip_counts"]["no_fill"] == 1


def test_live_trader_aborts_cycle_after_client_error(monkeypatch):
    signals = [_signal(), {**_signal(), "city": "austin", "bucket_question": "82° to 83°"}]
    monkeypatch.setattr(live, "KALSHI_LIVE_ENABLED", True)
    monkeypatch.setattr(live, "scan_all_markets", lambda **_: {"signals": signals, "comparisons": []})
    monkeypatch.setattr(live, "get_realized_pnl_for_trading_day", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(live, "get_trade_cost_for_trading_day", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(live, "get_trade_count_for_trading_day", lambda *args, **kwargs: 0)
    monkeypatch.setattr(live, "get_open_exposure_usd", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(live, "get_unresolved_trades", lambda *args, **kwargs: [])
    monkeypatch.setattr(live, "get_traded_buckets", lambda *args, **kwargs: set())
    monkeypatch.setattr(live, "log_weather_comparison_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(live, "alert_scan_summary", lambda *args, **kwargs: None)
    monkeypatch.setattr(live, "alert_error", lambda *args, **kwargs: None)

    trader = live.LiveTrader(client=_ClientErrorAfterOrderClient())
    executed = trader.run_scan_cycle()

    assert executed == []
    assert trader.last_decision_summary["skip_counts"]["client_error"] == 1
    assert trader.last_decision_summary["abort_reason"] == "Client error after order attempt; aborting remaining live cycle for safety"


def test_live_trader_uses_trading_day_exposure_gate(monkeypatch):
    monkeypatch.setattr(live, "KALSHI_LIVE_ENABLED", True)
    monkeypatch.setattr(live, "KALSHI_LIVE_MAX_OPEN_EXPOSURE_USD", 25.0)
    monkeypatch.setattr(live, "scan_all_markets", lambda **_: {"signals": [_signal()], "comparisons": []})
    monkeypatch.setattr(live, "get_realized_pnl_for_trading_day", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(live, "get_trade_cost_for_trading_day", lambda *args, **kwargs: 25.0)
    monkeypatch.setattr(live, "get_trade_count_for_trading_day", lambda *args, **kwargs: 0)
    monkeypatch.setattr(live, "get_open_exposure_usd", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(live, "get_unresolved_trades", lambda *args, **kwargs: [])
    monkeypatch.setattr(live, "get_traded_buckets", lambda *args, **kwargs: set())
    monkeypatch.setattr(live, "log_weather_comparison_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(live, "alert_scan_summary", lambda *args, **kwargs: None)

    trader = live.LiveTrader(client=_ExistingExposureClient())
    executed = trader.run_scan_cycle()

    assert executed == []
    assert trader.last_decision_summary["skip_counts"]["exposure_gate"] == 1


def test_live_bankroll_state_uses_trading_day_budget_instead_of_total_open_exposure(monkeypatch):
    monkeypatch.setattr(live, "KALSHI_LIVE_ENABLED", True)
    monkeypatch.setattr(live, "KALSHI_LIVE_BUDGET_ALLOCATION_USD", 50.0)
    monkeypatch.setattr(live, "KALSHI_LIVE_BANKROLL_SLICE_USD", 50.0)
    monkeypatch.setattr(live, "KALSHI_LIVE_MAX_OPEN_EXPOSURE_USD", 50.0)
    monkeypatch.setattr(live, "KALSHI_LIVE_MAX_DAILY_LOSS_USD", 20.0)
    monkeypatch.setattr(live, "get_realized_pnl_for_trading_day", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(live, "get_trade_cost_for_trading_day", lambda *args, **kwargs: 12.0)
    monkeypatch.setattr(live, "get_trade_count_for_trading_day", lambda *args, **kwargs: 2)
    monkeypatch.setattr(live, "get_open_exposure_usd", lambda *args, **kwargs: 40.0)
    monkeypatch.setattr(
        live,
        "get_unresolved_trades",
        lambda *args, **kwargs: [{"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}],
    )

    trader = live.LiveTrader(client=_LargeOpenBookClient())
    state = trader._live_bankroll_state(_LargeOpenBookClient().get_balance())

    assert state["trading_day_cost"] == 12.0
    assert state["trading_day_positions"] == 2
    assert state["open_exposure"] == 40.0
    assert state["open_positions"] == 9
    assert state["account_open_positions"] == 9
    assert state["total_open_positions"] == 9
    assert state["remaining_slice"] == 38.0
    assert state["remaining_open_exposure"] == 10.0


def test_live_bankroll_state_scales_remaining_budget_to_available_cash(monkeypatch):
    monkeypatch.setattr(live, "KALSHI_LIVE_ENABLED", True)
    monkeypatch.setattr(live, "KALSHI_LIVE_SCALE_TO_AVAILABLE_CASH", True)
    monkeypatch.setattr(live, "KALSHI_LIVE_BUDGET_ALLOCATION_USD", 100.0)
    monkeypatch.setattr(live, "KALSHI_LIVE_BANKROLL_SLICE_USD", 100.0)
    monkeypatch.setattr(live, "KALSHI_LIVE_MIN_CASH_BUFFER_USD", 5.0)
    monkeypatch.setattr(live, "KALSHI_LIVE_MAX_OPEN_EXPOSURE_USD", 50.0)
    monkeypatch.setattr(live, "KALSHI_LIVE_MAX_DAILY_LOSS_USD", 20.0)
    monkeypatch.setattr(live, "get_realized_pnl_for_trading_day", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(live, "get_trade_cost_for_trading_day", lambda *args, **kwargs: 12.0)
    monkeypatch.setattr(live, "get_trade_count_for_trading_day", lambda *args, **kwargs: 2)
    monkeypatch.setattr(live, "get_open_exposure_usd", lambda *args, **kwargs: 40.0)
    monkeypatch.setattr(
        live,
        "get_unresolved_trades",
        lambda *args, **kwargs: [{"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}],
    )

    trader = live.LiveTrader(client=_LargeOpenBookClient())
    state = trader._live_bankroll_state(_LargeOpenBookClient().get_balance())

    assert state["available_cash"] == 495.0
    assert state["target_budget_cap_usd"] == 100.0
    assert state["remaining_slice"] == 88.0
    assert state["remaining_open_exposure"] == 10.0


def test_live_bankroll_state_prefers_account_exposure_over_stale_tracked_exposure(monkeypatch):
    monkeypatch.setattr(live, "KALSHI_LIVE_ENABLED", True)
    monkeypatch.setattr(live, "KALSHI_LIVE_SCALE_TO_AVAILABLE_CASH", True)
    monkeypatch.setattr(live, "KALSHI_LIVE_MIN_CASH_BUFFER_USD", 5.0)
    monkeypatch.setattr(live, "KALSHI_LIVE_MAX_OPEN_EXPOSURE_USD", 200.0)
    monkeypatch.setattr(live, "get_realized_pnl_for_trading_day", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(live, "get_trade_cost_for_trading_day", lambda *args, **kwargs: 12.0)
    monkeypatch.setattr(live, "get_trade_count_for_trading_day", lambda *args, **kwargs: 2)
    monkeypatch.setattr(live, "get_open_exposure_usd", lambda *args, **kwargs: 1268.93)
    monkeypatch.setattr(
        live,
        "get_unresolved_trades",
        lambda *args, **kwargs: [{"id": i} for i in range(20)],
    )

    trader = live.LiveTrader(client=_TrackedExposureMismatchClient())
    state = trader._live_bankroll_state(_TrackedExposureMismatchClient().get_balance())

    assert state["tracked_open_exposure"] == 1268.93
    assert state["account_total_cost"] == 24.53
    assert state["open_exposure"] == 24.53
    assert state["remaining_open_exposure"] == 175.47


def test_live_trader_ignores_prior_day_position_count_for_todays_cap(monkeypatch):
    monkeypatch.setattr(live, "KALSHI_LIVE_ENABLED", True)
    monkeypatch.setattr(live, "KALSHI_LIVE_MAX_POSITIONS", 3)
    monkeypatch.setattr(live, "KALSHI_LIVE_MAX_TRADE_SIZE_USD", 30.0)
    monkeypatch.setattr(live, "KALSHI_LIVE_ALLOWED_DRIFT_CENTS", 2)
    monkeypatch.setattr(live, "scan_all_markets", lambda **_: {"signals": [_signal()], "comparisons": []})
    monkeypatch.setattr(live, "get_realized_pnl_for_trading_day", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(live, "get_trade_cost_for_trading_day", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(live, "get_trade_count_for_trading_day", lambda *args, **kwargs: 15)
    monkeypatch.setattr(live, "get_open_exposure_usd", lambda *args, **kwargs: 40.0)
    monkeypatch.setattr(
        live,
        "get_unresolved_trades",
        lambda *args, **kwargs: [{"id": i} for i in range(9)],
    )
    monkeypatch.setattr(live, "get_traded_buckets", lambda *args, **kwargs: set())
    monkeypatch.setattr(live, "log_weather_comparison_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(live, "alert_scan_summary", lambda *args, **kwargs: None)

    trader = live.LiveTrader(client=_ExistingExposureClient())
    executed = trader.run_scan_cycle()

    assert len(executed) == 1
    assert trader.last_decision_summary["filled"] == 1
    assert trader.last_decision_summary["skip_counts"].get("max_positions_gate", 0) == 0


def test_log_trade_persists_live_fill_fields(tmp_path, monkeypatch):
    monkeypatch.setattr(database, "DB_PATH", tmp_path / "trades.db")
    database._engine = None
    database._Session = None

    log_trade({
        "timestamp": "2026-04-12T01:00:00+00:00",
        "submitted_at": "2026-04-12T01:00:00+00:00",
        "filled_at": "2026-04-12T01:00:01+00:00",
        "venue": "kalshi",
        "city": "nyc",
        "target_date": "2026-04-13",
        "bucket_question": "60° or below",
        "side": "BUY",
        "trade_size": 30.0,
        "intended_size_usd": 32.0,
        "filled_size_usd": 30.0,
        "filled_contracts": 300,
        "market_prob": 0.10,
        "entry_price": 0.10,
        "expected_entry_price": 0.08,
        "fill_price": 0.10,
        "ensemble_prob": 0.57,
        "edge": 0.24,
        "order_status": "executed",
        "wallet_balance_snapshot": 500.0,
        "fee_usd": 0.50,
    }, mode="live")

    with session_scope() as session:
        trade = session.query(Trade).one()
        assert trade.mode == "live"
        assert trade.intended_size_usd == 32.0
        assert trade.filled_size_usd == 30.0
        assert trade.filled_contracts == 300
        assert trade.fill_price == 0.10
        assert trade.expected_entry_price == 0.08
        assert trade.order_status == "executed"
        assert trade.wallet_balance_snapshot == 500.0

    database._engine = None
    database._Session = None
