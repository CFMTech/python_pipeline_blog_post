import logging

LOGGER = logging.getLogger(__name__)


def get_signals(closes, volumes):
    """Return a collection of signals with the same resolution as past prices"""
    LOGGER.info("Computing signals")
    signals = {}

    shape_df = (~closes.isnull()) + (~volumes.isnull())

    # Buy AAPL
    buy_aapl = shape_df * 0.0
    buy_aapl["AAPL"] = 1.0
    signals["BUY_AAPL"] = buy_aapl

    # Buy AMZN
    buy_amzn = shape_df * 0.0
    buy_amzn["AMZN"] = 1.0
    signals["BUY_AMZN"] = buy_amzn

    return signals
