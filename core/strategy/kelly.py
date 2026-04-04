"""
Kelly Criterion position sizing.
Calculates optimal bet size based on edge and probability,
with fractional Kelly and hard caps for risk management.
"""

import logging
from config.settings import KELLY_FRACTION, MAX_TRADE_SIZE, MAX_DAILY_LOSS

logger = logging.getLogger(__name__)


def full_kelly(edge: float, probability: float) -> float:
    """
    Calculate the full Kelly Criterion fraction.
    
    Kelly % = edge / odds
    For binary markets: Kelly % = (p * b - q) / b
    where p = true prob, q = 1-p, b = payout odds
    
    Simplified for prediction markets where payout = 1/market_price:
    Kelly % = edge / (1 - market_price)
    
    Args:
        edge: The calculated edge (ensemble_prob - market_prob)
        probability: The ensemble probability (our estimate of true prob)
    
    Returns:
        Kelly fraction (can be > 1.0 for very large edges)
    """
    if edge <= 0:
        return 0.0

    # For a binary contract priced at market_prob:
    # If we buy YES at price p, we win (1-p) if correct, lose p if wrong
    # Kelly = (edge) / (1 - market_price)
    market_price = probability - edge
    if market_price <= 0 or market_price >= 1:
        return 0.0

    odds = (1 - market_price) / market_price  # decimal odds - 1
    kelly = (probability * odds - (1 - probability)) / odds

    return max(kelly, 0.0)


def fractional_kelly(edge: float, probability: float,
                     fraction: float = None) -> float:
    """
    Calculate fractional Kelly bet size.
    Using a fraction (default 15%) of full Kelly reduces variance
    while maintaining positive expected value.
    
    Returns:
        Fraction of bankroll to bet (0.0 to 1.0)
    """
    f = fraction if fraction is not None else KELLY_FRACTION
    fk = full_kelly(edge, probability)
    return fk * f


def calculate_trade_size(edge: float, probability: float,
                         bankroll: float,
                         daily_pnl: float = 0.0,
                         fraction: float = None,
                         max_size: float = None,
                         max_loss: float = None) -> dict:
    """
    Calculate the dollar amount to trade, applying all risk limits.
    
    Args:
        edge: Calculated edge (ensemble_prob - market_prob)
        probability: Ensemble probability (our true prob estimate)
        bankroll: Current available bankroll in USD
        daily_pnl: Today's running P&L (negative = losses)
        fraction: Kelly fraction override
        max_size: Max trade size override
        max_loss: Max daily loss override
    
    Returns:
        Dict with 'size', 'kelly_pct', 'capped_by', 'skip_reason'
    """
    ms = max_size if max_size is not None else MAX_TRADE_SIZE
    ml = max_loss if max_loss is not None else MAX_DAILY_LOSS

    # Check daily loss limit
    if daily_pnl <= -ml:
        return {
            "size": 0.0,
            "kelly_pct": 0.0,
            "capped_by": "daily_loss_limit",
            "skip_reason": f"Daily loss limit reached (${-daily_pnl:.2f} >= ${ml:.2f})",
        }

    # Calculate Kelly fraction
    kelly_pct = fractional_kelly(edge, probability, fraction)
    if kelly_pct <= 0:
        return {
            "size": 0.0,
            "kelly_pct": 0.0,
            "capped_by": "no_edge",
            "skip_reason": "Kelly fraction is zero or negative",
        }

    # Raw dollar amount
    raw_size = bankroll * kelly_pct

    # Apply caps
    capped_by = "kelly"
    final_size = raw_size

    if final_size > ms:
        final_size = ms
        capped_by = "max_trade_size"

    # Don't exceed remaining daily loss budget
    remaining_budget = ml + daily_pnl  # daily_pnl is negative for losses
    if final_size > remaining_budget:
        final_size = max(remaining_budget, 0)
        capped_by = "daily_loss_budget"

    # Minimum viable trade ($1)
    if final_size < 1.0:
        return {
            "size": 0.0,
            "kelly_pct": kelly_pct,
            "capped_by": "below_minimum",
            "skip_reason": f"Trade size ${final_size:.2f} below $1 minimum",
        }

    logger.info(f"Position size: ${final_size:.2f} (Kelly={kelly_pct:.2%}, "
                f"capped_by={capped_by}, bankroll=${bankroll:.2f})")

    return {
        "size": round(final_size, 2),
        "kelly_pct": kelly_pct,
        "capped_by": capped_by,
        "skip_reason": None,
    }
