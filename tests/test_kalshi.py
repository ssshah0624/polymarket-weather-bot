from core.data.kalshi import (
    extract_market_city,
    extract_market_date,
    get_active_temperature_events,
    parse_market_bucket,
)
from core.resolution import check_bucket_hit
from core.strategy.edge import analyze_event_buckets


def test_kalshi_market_parsing_extracts_city_date_and_prices():
    market = {
        "ticker": "KXHIGHNY-26APR06-T70",
        "event_ticker": "KXHIGHNY-26APR06",
        "title": "70° to 71°",
        "subtitle": "Highest temperature in NYC on Apr 6, 2026?",
        "yes_bid_dollars": "0.42",
        "no_bid_dollars": "0.56",
        "volume_fp": "1200",
    }

    assert extract_market_city(market) == "nyc"
    assert extract_market_date(market) == "2026-04-06"

    bucket = parse_market_bucket(market)
    assert bucket["temp_low"] == 70
    assert bucket["temp_high"] == 71
    assert bucket["yes_price"] == 0.44
    assert bucket["no_price"] == 0.58


def test_kalshi_range_resolves_with_to_syntax():
    assert check_bucket_hit(70.4, "70° to 71°", "nyc") is True
    assert check_bucket_hit(71.0, "70° to 71°", "nyc") is False


def test_extract_market_city_falls_back_to_series_ticker_when_title_is_generic():
    market = {
        "title": "Will the maximum temperature be  >84° on Apr 10, 2026?",
        "subtitle": None,
        "yes_sub_title": "85° or above",
        "event_ticker": "KXHIGHTHOU-26APR10",
        "series_ticker": "KXHIGHTHOU",
    }

    assert extract_market_city(market) == "houston"


def test_analyze_event_buckets_prefers_no_side_when_no_contract_is_cheaper():
    analyzed = analyze_event_buckets([
        {
            "question": "70° to 71°",
            "ensemble_prob": 0.20,
            "yes_price": 0.35,
            "no_price": 0.55,
            "market_prob": 0.35,
        }
    ], venue="kalshi")

    result = analyzed[0]
    assert result["preferred_side"] == "SELL"
    assert result["selected_price"] == 0.55
    assert result["edge"] < 0
    assert result["is_tradeable"] is True


def test_get_active_temperature_events_uses_series_tickers(monkeypatch):
    market = {
        "ticker": "KXHIGHLAX-26APR08-B75.5",
        "event_ticker": "KXHIGHLAX-26APR08",
        "title": "Will the **high temp in LA** be 75-76° on Apr 8, 2026?",
        "yes_sub_title": "75° to 76°",
        "no_sub_title": "75° to 76°",
        "yes_bid_dollars": "0.07",
        "yes_ask_dollars": "0.08",
        "no_bid_dollars": "0.92",
        "no_ask_dollars": "0.93",
        "volume_fp": "1500",
    }

    seen = []

    def fake_fetch_series_markets(series_ticker: str, status: str = "open", limit: int = 200):
        seen.append((series_ticker, status, limit))
        if series_ticker == "KXHIGHLAX":
            return [market]
        return []

    monkeypatch.setattr("core.data.kalshi.fetch_series_markets", fake_fetch_series_markets)
    monkeypatch.setattr(
        "core.data.kalshi.CITIES",
        {
            "los_angeles": {
                "name": "Los Angeles",
                "kalshi_series_ticker": "KXHIGHLAX",
                "kalshi_names": ["los angeles", "la"],
            },
            "nyc": {
                "name": "New York City",
                "kalshi_series_ticker": "KXHIGHNY",
                "kalshi_names": ["new york city", "nyc", "new york"],
            },
        },
    )

    events = get_active_temperature_events()

    assert ("KXHIGHLAX", "open", 200) in seen
    assert ("KXHIGHNY", "open", 200) in seen
    assert len(events) == 1
    assert events[0]["venue"] == "kalshi"
    assert events[0]["city"] == "los_angeles"
    assert events[0]["target_date"] == "2026-04-08"
    assert events[0]["buckets"][0]["yes_price"] == 0.08


def test_parse_market_bucket_does_not_misread_chicago_as_celsius():
    market = {
        "ticker": "KXHIGHCHI-26APR09-B67.5",
        "event_ticker": "KXHIGHCHI-26APR09",
        "title": "Will the high temp in Chicago be 67-68° on Apr 9, 2026?",
        "yes_bid_dollars": "0.17",
        "no_bid_dollars": "0.82",
    }

    bucket = parse_market_bucket(market)

    assert bucket["is_fahrenheit"] is True
    assert bucket["temp_low"] == 67
    assert bucket["temp_high"] == 68
