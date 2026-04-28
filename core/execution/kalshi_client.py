"""
Lightweight Kalshi REST client for authenticated order placement.
"""

from __future__ import annotations

import base64
import math
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import requests

from config.settings import KALSHI_API_BASE, KALSHI_DEMO_API_BASE


REQUEST_TIMEOUT = 30


class KalshiClientError(RuntimeError):
    """Raised when the Kalshi API returns an unexpected response."""


def _to_float(value) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


@dataclass
class BalanceSnapshot:
    available_cash_usd: float
    balance_usd: float
    raw: dict


@dataclass
class PortfolioExposureSnapshot:
    total_cost_usd: float
    market_value_usd: float
    open_positions: int
    raw: dict


class KalshiClient:
    """Thin authenticated client for the Kalshi REST API."""

    def __init__(
        self,
        api_key_id: str,
        private_key_path: str,
        *,
        use_demo: bool = True,
        session: Optional[requests.Session] = None,
        private_key=None,
    ):
        if not api_key_id:
            raise KalshiClientError("Missing KALSHI_API_KEY_ID")
        if private_key is None and not private_key_path:
            raise KalshiClientError("Missing KALSHI_PRIVATE_KEY_PATH")

        self.api_key_id = api_key_id
        self.private_key_path = private_key_path
        self.base_url = KALSHI_DEMO_API_BASE if use_demo else KALSHI_API_BASE
        self.session = session or requests.Session()
        self.private_key = private_key or self._load_private_key(private_key_path)

    def _load_private_key(self, key_path: str):
        try:
            from cryptography.hazmat.backends import default_backend
            from cryptography.hazmat.primitives import serialization
        except ImportError as exc:
            raise KalshiClientError(
                "cryptography is required for Kalshi live trading"
            ) from exc

        path = Path(key_path)
        if not path.exists():
            raise KalshiClientError(f"Kalshi private key file not found: {path}")
        with path.open("rb") as handle:
            return serialization.load_pem_private_key(
                handle.read(),
                password=None,
                backend=default_backend(),
            )

    def _timestamp_ms(self) -> str:
        return str(int(datetime.now(timezone.utc).timestamp() * 1000))

    def _create_signature(self, timestamp: str, method: str, path: str) -> str:
        try:
            from cryptography.hazmat.primitives import hashes
            from cryptography.hazmat.primitives.asymmetric import padding
        except ImportError as exc:
            raise KalshiClientError(
                "cryptography is required for Kalshi live trading"
            ) from exc

        path_without_query = path.split("?")[0]
        message = f"{timestamp}{method.upper()}{path_without_query}".encode("utf-8")
        signature = self.private_key.sign(
            message,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )
        return base64.b64encode(signature).decode("utf-8")

    def _headers(self, method: str, path: str, auth_required: bool) -> dict:
        headers = {}
        if auth_required:
            sign_path = urlparse(self.base_url + path).path
            timestamp = self._timestamp_ms()
            headers.update(
                {
                    "KALSHI-ACCESS-KEY": self.api_key_id,
                    "KALSHI-ACCESS-SIGNATURE": self._create_signature(timestamp, method, sign_path),
                    "KALSHI-ACCESS-TIMESTAMP": timestamp,
                }
            )
        return headers

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict | None = None,
        json_data: dict | None = None,
        auth_required: bool = True,
    ) -> dict:
        headers = self._headers(method, path, auth_required)
        if json_data is not None:
            headers["Content-Type"] = "application/json"

        response = self.session.request(
            method.upper(),
            self.base_url + path,
            headers=headers,
            params=params,
            json=json_data,
            timeout=REQUEST_TIMEOUT,
        )
        if response.status_code >= 400:
            raise KalshiClientError(
                f"Kalshi {method.upper()} {path} failed: {response.status_code} {response.text}"
            )
        if not response.content:
            return {}
        return response.json()

    def get_balance(self) -> BalanceSnapshot:
        payload = self._request("GET", "/portfolio/balance")
        balance_cents = payload.get("balance")
        if balance_cents is None:
            balance_cents = payload.get("balance_cents")
        available_cents = payload.get("available_balance")
        if available_cents is None:
            available_cents = payload.get("available_cash")
        if available_cents is None:
            available_cents = balance_cents
        return BalanceSnapshot(
            available_cash_usd=_to_float(available_cents) / 100.0,
            balance_usd=_to_float(balance_cents) / 100.0,
            raw=payload,
        )

    def get_market(self, ticker: str) -> dict:
        payload = self._request("GET", f"/markets/{ticker}", auth_required=False)
        return payload.get("market", payload)

    def get_order(self, order_id: str) -> dict:
        payload = self._request("GET", f"/portfolio/orders/{order_id}")
        return payload.get("order", payload)

    def get_fills(self, *, order_id: str | None = None, ticker: str | None = None, limit: int = 100) -> list[dict]:
        params = {"limit": limit}
        if order_id:
            params["order_id"] = order_id
        if ticker:
            params["ticker"] = ticker
        payload = self._request("GET", "/portfolio/fills", params=params)
        return payload.get("fills", [])

    def get_positions(
        self,
        *,
        settlement_status: str = "unsettled",
        count_filter: str = "position,total_traded",
        limit: int = 200,
    ) -> dict:
        params = {
            "settlement_status": settlement_status,
            "count_filter": count_filter,
            "limit": limit,
        }
        market_positions: list[dict] = []
        event_positions: list[dict] = []
        cursor = None
        while True:
            if cursor:
                params["cursor"] = cursor
            payload = self._request("GET", "/portfolio/positions", params=params)
            market_positions.extend(payload.get("market_positions", []))
            event_positions.extend(payload.get("event_positions", []))
            cursor = payload.get("cursor")
            if not cursor:
                break
        return {
            "market_positions": market_positions,
            "event_positions": event_positions,
        }

    def get_portfolio_exposure(self) -> PortfolioExposureSnapshot:
        payload = self.get_positions()
        event_positions = payload.get("event_positions", [])
        market_positions = payload.get("market_positions", [])

        total_cost_usd = sum(_to_float(p.get("total_cost_dollars")) for p in event_positions)
        market_value_usd = sum(_to_float(p.get("event_exposure_dollars")) for p in event_positions)
        open_positions = sum(
            1 for p in market_positions
            if abs(_to_float(p.get("position_fp"))) > 0 or _to_float(p.get("total_traded_dollars")) > 0
        )

        return PortfolioExposureSnapshot(
            total_cost_usd=round(total_cost_usd, 2),
            market_value_usd=round(market_value_usd, 2),
            open_positions=open_positions,
            raw=payload,
        )

    def cancel_order(self, order_id: str) -> dict:
        return self._request("DELETE", f"/portfolio/orders/{order_id}")

    def create_buy_order(
        self,
        *,
        ticker: str,
        side: str,
        count: int,
        limit_price: float,
        client_order_id: str,
        buy_max_cost_cents: int | None = None,
    ) -> dict:
        price_cents = max(1, min(99, int(round(limit_price * 100))))
        payload = {
            "ticker": ticker,
            "action": "buy",
            "side": side,
            "count": int(count),
            "type": "limit",
            "client_order_id": client_order_id,
            "time_in_force": "fill_or_kill",
            "buy_max_cost": buy_max_cost_cents if buy_max_cost_cents is not None else price_cents * int(count),
            "cancel_order_on_pause": True,
        }
        if side == "yes":
            payload["yes_price"] = price_cents
        elif side == "no":
            payload["no_price"] = price_cents
        else:
            raise KalshiClientError(f"Unsupported Kalshi side: {side}")

        response = self._request("POST", "/portfolio/orders", json_data=payload)
        return response.get("order", response)

    def summarize_fill(
        self,
        *,
        order: dict,
        fills: list[dict],
        side: str,
        expected_entry_price: float,
    ) -> dict:
        fill_contracts = 0.0
        total_cost = 0.0
        total_fees = 0.0
        filled_at = None

        for fill in fills:
            count = _to_float(fill.get("count_fp") or fill.get("count"))
            price = _to_float(
                fill.get("yes_price_dollars") if side == "yes" else fill.get("no_price_dollars")
            )
            fee = _to_float(fill.get("fee_cost"))
            fill_contracts += count
            total_cost += count * price
            total_fees += fee
            filled_at = fill.get("created_time") or filled_at

        if fill_contracts <= 0:
            fill_contracts = _to_float(order.get("fill_count_fp") or order.get("fill_count"))
            total_cost = _to_float(order.get("taker_fill_cost_dollars") or order.get("maker_fill_cost_dollars"))
            total_fees = _to_float(order.get("taker_fees_dollars") or order.get("maker_fees_dollars"))

        fill_price = total_cost / fill_contracts if fill_contracts > 0 else 0.0
        adverse_drift_cents = max(0.0, (fill_price - expected_entry_price) * 100.0)

        return {
            "order_status": order.get("status", "unknown"),
            "filled_contracts": int(math.floor(fill_contracts)),
            "filled_size_usd": round(total_cost, 2),
            "fill_price": round(fill_price, 4),
            "fee_usd": round(total_fees, 2),
            "submitted_at": order.get("created_time"),
            "filled_at": filled_at or order.get("last_update_time"),
            "adverse_drift_cents": round(adverse_drift_cents, 2),
        }

    def place_marketable_buy(
        self,
        *,
        ticker: str,
        side: str,
        count: int,
        limit_price: float,
        client_order_id: str,
        max_cost_buffer_cents: int = 0,
    ) -> dict:
        buy_max_cost = int(math.ceil(count * limit_price * 100)) + max(0, max_cost_buffer_cents)
        order = self.create_buy_order(
            ticker=ticker,
            side=side,
            count=count,
            limit_price=limit_price,
            client_order_id=client_order_id,
            buy_max_cost_cents=buy_max_cost,
        )

        order_id = order.get("order_id")
        latest_order = order
        fills = []

        # FoK orders are often terminal in the create response already. Prefer that response
        # and only poll when the exchange reports a non-terminal status.
        if order_id and latest_order.get("status") not in {"executed", "canceled"}:
            for _ in range(3):
                try:
                    latest_order = self.get_order(order_id)
                except KalshiClientError as exc:
                    if " 404 " in f" {exc} " or '"code":"not_found"' in str(exc):
                        break
                    raise
                if latest_order.get("status") in {"executed", "canceled"}:
                    break
                time.sleep(0.25)

        if order_id:
            try:
                fills = self.get_fills(order_id=order_id, ticker=ticker)
            except KalshiClientError:
                fills = []

        order["latest_order"] = latest_order
        order["fills"] = fills
        return order
