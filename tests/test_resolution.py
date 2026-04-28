from datetime import date

import core.resolution as resolution
from core.data import nws_climate
import core.strategy.signals as signals


CLI_TEXT = """
880
CDUS41 KOKX 140659
CLINYC

CLIMATE REPORT
NATIONAL WEATHER SERVICE NEW YORK, NY
259 AM EDT TUE APR 14 2026

...THE CENTRAL PARK NY CLIMATE SUMMARY FOR APRIL 13 2026...

TEMPERATURE (F)
 YESTERDAY
  MAXIMUM         79    118 PM
  MINIMUM         48    557 AM
"""


def test_extract_station_from_rules_primary_handles_in_and_at_variants():
    in_rules = (
        "If the highest temperature recorded in Central Park, New York for April 14, 2026 "
        "as reported by the National Weather Service's Climatological Report (Daily), "
        "is between 79-80°, then the market resolves to Yes."
    )
    at_rules = (
        "If the maximum temperature recorded at Miami International Airport for Apr 14, 2026, "
        "is less than 77° fahrenheit according to the National Weather Service's "
        "Climatological Report (Daily), then the market resolves to Yes."
    )

    assert nws_climate.extract_station_from_rules_primary(in_rules) == "Central Park, New York"
    assert nws_climate.extract_station_from_rules_primary(at_rules) == "Miami International Airport"


def test_get_daily_climate_high_parses_cli_report(monkeypatch):
    monkeypatch.setattr(
        nws_climate,
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
        nws_climate,
        "_fetch_product_text",
        lambda product, issuedby, version=1: CLI_TEXT if product == "CLI" and version == 1 else None,
    )

    result = nws_climate.get_daily_climate_high("nyc", "2026-04-13")

    assert result == {
        "actual_temp_f": 79.0,
        "station_name": "CENTRAL PARK NY",
        "issuedby": "NYC",
        "report_date": "2026-04-13",
        "source": "nws_cli_daily",
        "report_version": 1,
    }


def test_get_daily_climate_high_tries_alternate_station_codes(monkeypatch):
    monkeypatch.setattr(
        nws_climate,
        "CITIES",
        {
            "chicago": {
                "name": "Chicago",
                "lat": 41.8781,
                "lon": -87.6298,
                "kalshi_cli_code": "ORD",
                "kalshi_cli_codes": ["ORD", "CHI"],
            }
        },
    )
    monkeypatch.setattr(
        nws_climate,
        "get_climate_station_metadata",
        lambda city_key: {
            "station_name": "CHICAGO-OHARE",
            "issuedby": "ORD",
            "lat": 41.995,
            "lon": -87.9336,
            "source": "nws_cf6",
        },
    )

    def fake_fetch(product, issuedby, version=1):
        if product == "CLI" and issuedby == "ORD" and version == 1:
            return """
CLIMATE REPORT
...THE CHICAGO-OHARE CLIMATE SUMMARY FOR APRIL 13 2026...
TEMPERATURE (F)
 MAXIMUM         79
"""
        return None

    monkeypatch.setattr(nws_climate, "_fetch_product_text", fake_fetch)

    result = nws_climate.get_daily_climate_high("chicago", "2026-04-13")

    assert result["actual_temp_f"] == 79.0
    assert result["issuedby"] == "ORD"


def test_resolve_pending_trades_uses_nws_cli_for_kalshi(monkeypatch):
    resolved = {}
    monkeypatch.setattr(
        resolution,
        "get_unresolved_trades",
        lambda mode="paper", venue=None: [{
            "id": 42,
            "venue": "kalshi",
            "city": "nyc",
            "target_date": "2026-04-12",
            "bucket_question": "78° to 79°",
            "side": "BUY",
            "size_usd": 2.0,
            "price": 0.25,
            "entry_price": 0.25,
            "fill_price": 0.25,
            "filled_size_usd": 2.0,
            "fee_usd": 0.0,
            "ensemble_prob": 0.6,
            "edge": 0.2,
        }],
    )
    monkeypatch.setattr(
        resolution,
        "get_daily_climate_high",
        lambda city_key, target_date: {
            "actual_temp_f": 78.0,
            "station_name": "Central Park NY",
            "issuedby": "NYC",
            "report_date": "2026-04-12",
            "source": "nws_cli_daily",
            "report_version": 1,
        },
    )
    monkeypatch.setattr(resolution, "date", type("FakeDate", (), {"today": staticmethod(lambda: date(2026, 4, 13))}))

    def fake_resolve_trade(**kwargs):
        resolved.update(kwargs)

    monkeypatch.setattr(resolution, "resolve_trade", fake_resolve_trade)

    summary = resolution.resolve_pending_trades(mode="live", venue="kalshi")

    assert summary["resolved"] == 1
    assert summary["wins"] == 1
    assert resolved["trade_id"] == 42
    assert resolved["actual_temp"] == 78.0
    assert resolved["settlement_station"] == "Central Park NY"
    assert resolved["resolution_source"] == "nws_cli_daily"


def test_scan_event_uses_climate_station_coordinates_for_kalshi(monkeypatch):
    recorded = {}

    class _FakeDateTime:
        @classmethod
        def now(cls, tz=None):
            import datetime as _dt

            current = _dt.datetime(2026, 4, 13, 12, 0, 0, tzinfo=_dt.timezone.utc)
            return current if tz is None else current.astimezone(tz)

        @classmethod
        def strptime(cls, *args, **kwargs):
            import datetime as _dt

            return _dt.datetime.strptime(*args, **kwargs)

    monkeypatch.setattr(signals, "datetime", _FakeDateTime)
    monkeypatch.setattr(signals, "MIN_VOLUME", 1000)
    monkeypatch.setattr(signals, "MAX_FORECAST_DAYS", 2)
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

    def fake_get_forecast_high(city_key, target_date, lat=None, lon=None):
        recorded["forecast"] = (city_key, target_date, lat, lon)
        return None

    def fake_get_hourly_forecast_high(city_key, target_date, lat=None, lon=None):
        recorded["hourly"] = (city_key, target_date, lat, lon)
        return None

    def fake_get_full_distribution(city_key, target_date, buckets, unit="fahrenheit", lat=None, lon=None, **kwargs):
        recorded["distribution"] = (city_key, target_date, lat, lon, kwargs)
        return None

    monkeypatch.setattr(signals, "get_forecast_high", fake_get_forecast_high)
    monkeypatch.setattr(signals, "get_hourly_forecast_high", fake_get_hourly_forecast_high)
    monkeypatch.setattr(signals, "get_full_distribution", fake_get_full_distribution)

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

    assert result["event_summary"]["reason_code"] == "ensemble_unavailable"
    assert recorded["forecast"] == ("nyc", "2026-04-14", 40.7783, -73.9667)
    assert recorded["hourly"] == ("nyc", "2026-04-14", 40.7783, -73.9667)
    assert recorded["distribution"] == (
        "nyc",
        "2026-04-14",
        40.7783,
        -73.9667,
        {"anchor_temp": None, "blend_weight": 0.65, "blend_source": None},
    )
