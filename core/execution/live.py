"""
Live trading execution engine.
Executes real trades on Polymarket via the CLOB API using py-clob-client.

REQUIRES:
- POLYMARKET_PRIVATE_KEY in .env
- POLYMARKET_FUNDER_ADDRESS in .env
- py-clob-client installed (pip install py-clob-client)
- USDC.e funded on Polygon

This module is a STUB — it will be fully implemented when you're ready
to go live. For now, it mirrors the paper trading interface.
"""

import logging

logger = logging.getLogger(__name__)


class LiveTrader:
    """
    Live trading engine — STUB.
    Will be implemented in Phase 4 when credentials are provided.
    """

    def __init__(self):
        logger.warning(
            "LiveTrader initialized but NOT YET IMPLEMENTED. "
            "Use PaperTrader for now. To enable live trading:\n"
            "  1. Set POLYMARKET_PRIVATE_KEY in .env\n"
            "  2. Set POLYMARKET_FUNDER_ADDRESS in .env\n"
            "  3. Install py-clob-client: pip install py-clob-client\n"
            "  4. Set TRADING_MODE=live in .env\n"
        )

    def run_loop(self, interval: int = None):
        raise NotImplementedError(
            "Live trading is not yet implemented. "
            "Run in paper mode first to validate the strategy."
        )
