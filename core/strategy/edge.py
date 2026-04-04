"""
Edge calculation engine.
Compares ensemble-derived probabilities against Polymarket prices
to identify mispriced temperature buckets.
"""

import logging
from typing import Optional
from config.settings import EDGE_THRESHOLD

logger = logging.getLogger(__name__)

# Weather category fee parameters (effective March 30, 2026)
# fee = C * p * feeRate * (p * (1 - p))^exponent
WEATHER_FEE_RATE = 0.025
WEATHER_FEE_EXPONENT = 0.5


def calc_fee_pct(price: float) -> float:
    """Calculate the effective taker fee percentage for a weather market trade."""
    if price <= 0 or price >= 1:
        return 0.0
    return WEATHER_FEE_RATE * (price * (1 - price)) ** WEATHER_FEE_EXPONENT


def calculate_edge(ensemble_prob: float, market_prob: float) -> float:
    """
    Calculate the fee-adjusted edge between the model probability and the market price.
    
    Positive edge -> ensemble says more likely than market prices -> BUY YES
    Negative edge -> ensemble says less likely than market prices -> BUY NO / SELL YES
    
    The edge is reduced by the effective taker fee so we only trade when
    the edge exceeds fees.
    
    Args:
        ensemble_prob: Probability from GFS ensemble (0.0 to 1.0)
        market_prob: Implied probability from Polymarket price (0.0 to 1.0)
    
    Returns:
        Fee-adjusted edge as a float (e.g., 0.15 means 15% edge after fees)
    """
    raw_edge = ensemble_prob - market_prob
    fee = calc_fee_pct(market_prob)
    # Subtract fee from absolute edge (fees eat into our edge regardless of direction)
    if raw_edge > 0:
        return raw_edge - fee
    elif raw_edge < 0:
        return raw_edge + fee  # edge is negative, fee makes it less negative (closer to 0)
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
                          threshold: Optional[float] = None) -> list[dict]:
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

    for bucket in enriched_buckets:
        ens_prob = bucket.get("ensemble_prob")
        mkt_prob = bucket.get("market_prob", 0.0)

        if ens_prob is None:
            results.append({
                **bucket,
                "edge": None,
                "signal": "no_data",
                "is_tradeable": False,
            })
            continue

        edge = calculate_edge(ens_prob, mkt_prob)
        signal = classify_signal(edge, threshold)
        tradeable = is_tradeable(edge, threshold)

        results.append({
            **bucket,
            "edge": edge,
            "signal": signal,
            "is_tradeable": tradeable,
        })

    # Log summary
    tradeable_count = sum(1 for r in results if r["is_tradeable"])
    if tradeable_count > 0:
        logger.info(f"Found {tradeable_count} tradeable buckets out of {len(results)}")
        for r in results:
            if r["is_tradeable"]:
                logger.info(f"  {r['signal'].upper():>12} | {r['question'][:50]} | "
                           f"Edge: {r['edge']:+.1%} | Market: {r['market_prob']:.1%} | "
                           f"Ensemble: {r['ensemble_prob']:.1%}")

    return results


def rank_opportunities(analyzed_buckets: list[dict]) -> list[dict]:
    """
    Rank tradeable opportunities by absolute edge (highest first).
    Only returns buckets that are tradeable.
    """
    tradeable = [b for b in analyzed_buckets if b.get("is_tradeable")]
    return sorted(tradeable, key=lambda b: abs(b.get("edge", 0)), reverse=True)
