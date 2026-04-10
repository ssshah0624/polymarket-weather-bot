"""
Edge calculation engine.
Compares ensemble-derived probabilities against venue prices
to identify mispriced temperature buckets.
"""

import logging
from typing import Optional
from config.settings import EDGE_THRESHOLD
from core.tuning import get_effective_strategy_params

logger = logging.getLogger(__name__)

# Weather category fee parameters (effective March 30, 2026)
# fee = C * p * feeRate * (p * (1 - p))^exponent
WEATHER_FEE_RATE = 0.025
WEATHER_FEE_EXPONENT = 0.5


def calc_fee_pct(price: float, venue: str = "polymarket",
                 strategy_params: dict | None = None) -> float:
    """Calculate the effective fee percentage for a venue quote."""
    if venue == "kalshi":
        params = strategy_params or get_effective_strategy_params(venue)
        return max(params.get("kalshi_fee_buffer_pct", 0.0), 0.0)
    if price <= 0 or price >= 1:
        return 0.0
    return WEATHER_FEE_RATE * (price * (1 - price)) ** WEATHER_FEE_EXPONENT


def calculate_edge(true_prob: float, entry_price: float, fee_pct: float = 0.0) -> float:
    """
    Calculate the fee-adjusted edge between the model probability and the entry price.
    
    Positive edge means the estimated probability exceeds the all-in contract cost.
    """
    raw_edge = true_prob - entry_price
    if raw_edge > 0:
        return raw_edge - fee_pct
    elif raw_edge < 0:
        return raw_edge + fee_pct
    return 0.0


def is_tradeable(edge: float, threshold: Optional[float] = None) -> bool:
    """Check if the absolute edge exceeds the trading threshold."""
    t = threshold if threshold is not None else EDGE_THRESHOLD
    return abs(edge) >= t


def classify_signal(edge: float, threshold: Optional[float] = None) -> str:
    """
    Classify the edge into a trading signal.
    
    Returns one of: 'strong_buy', 'buy', 'hold', 'sell', 'strong_sell'
    """
    t = threshold if threshold is not None else EDGE_THRESHOLD
    if edge >= t * 2:
        return "strong_buy"
    elif edge >= t:
        return "buy"
    elif edge <= -t * 2:
        return "strong_sell"
    elif edge <= -t:
        return "sell"
    else:
        return "hold"


def analyze_event_buckets(enriched_buckets: list[dict],
                          threshold: Optional[float] = None,
                          venue: str = "polymarket",
                          strategy_params: dict | None = None) -> list[dict]:
    """
    Analyze all buckets for an event and attach edge/signal data.
    
    Args:
        enriched_buckets: Buckets from ensemble.get_full_distribution()
                          (must have 'ensemble_prob' and 'market_prob' fields)
        threshold: Override edge threshold
    
    Returns:
        List of bucket dicts enriched with 'edge', 'signal', 'is_tradeable' fields.
    """
    results = []
    params = strategy_params or get_effective_strategy_params(venue)
    effective_threshold = threshold if threshold is not None else params.get("edge_threshold", EDGE_THRESHOLD)

    for bucket in enriched_buckets:
        ens_prob = bucket.get("ensemble_prob")
        yes_price = bucket.get("yes_price", bucket.get("market_prob", 0.0))
        no_price = bucket.get("no_price")
        if no_price is None:
            no_price = max(1.0 - yes_price, 0.0)

        if ens_prob is None:
            results.append({
                **bucket,
                "edge": None,
                "signal": "no_data",
                "is_tradeable": False,
            })
            continue

        yes_fee = calc_fee_pct(yes_price, venue=venue, strategy_params=params)
        no_fee = calc_fee_pct(no_price, venue=venue, strategy_params=params)
        yes_edge = calculate_edge(ens_prob, yes_price, fee_pct=yes_fee)
        no_edge = calculate_edge(1 - ens_prob, no_price, fee_pct=no_fee)

        if yes_edge >= no_edge:
            signed_edge = yes_edge
            preferred_side = "BUY"
            selected_price = yes_price
            selected_prob = ens_prob
            selected_fee_pct = yes_fee
        else:
            signed_edge = -no_edge
            preferred_side = "SELL"
            selected_price = no_price
            selected_prob = 1 - ens_prob
            selected_fee_pct = no_fee

        signal = classify_signal(signed_edge, effective_threshold)
        tradeable = is_tradeable(signed_edge, effective_threshold)

        results.append({
            **bucket,
            "market_prob": yes_price,
            "yes_price": yes_price,
            "no_price": no_price,
            "yes_edge": yes_edge,
            "no_edge": no_edge,
            "edge": signed_edge,
            "signal": signal,
            "is_tradeable": tradeable,
            "preferred_side": preferred_side,
            "selected_price": selected_price,
            "selected_prob": selected_prob,
            "selected_fee_pct": selected_fee_pct,
        })

    # Log summary
    tradeable_count = sum(1 for r in results if r["is_tradeable"])
    if tradeable_count > 0:
        logger.info(f"Found {tradeable_count} tradeable buckets out of {len(results)}")
        for r in results:
            if r["is_tradeable"]:
                logger.info(f"  {r['signal'].upper():>12} | {r['question'][:50]} | "
                           f"Edge: {r['edge']:+.1%} | YES: {r['yes_price']:.1%} | "
                           f"NO: {r['no_price']:.1%} | "
                           f"Ensemble: {r['ensemble_prob']:.1%}")

    return results


def rank_opportunities(analyzed_buckets: list[dict]) -> list[dict]:
    """
    Rank tradeable opportunities by absolute edge (highest first).
    Only returns buckets that are tradeable.
    """
    tradeable = [b for b in analyzed_buckets if b.get("is_tradeable")]
    return sorted(tradeable, key=lambda b: abs(b.get("edge", 0)), reverse=True)
