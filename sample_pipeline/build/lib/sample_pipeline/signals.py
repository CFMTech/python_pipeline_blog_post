import logging

import numpy as np
import pandas as pd

LOGGER = logging.getLogger(__name__)


def get_signals(closes, volumes):
    """Return a collection of signals
    with the same resolution as past prices"""
    LOGGER.info("Computing signals")
    signals = {}

    shape_df = (~closes.isnull()) + (~volumes.isnull())

    # Random signal
    rng = np.random.default_rng()
    random = pd.DataFrame(
        rng.normal(size=shape_df.values.shape),
        index=shape_df.index,
        columns=shape_df.columns,
    )
    random[shape_df > 0] = np.NaN
    signals["RANDOM"] = random

    # Buy AMZN
    buy_amzn = shape_df * 0.0
    buy_amzn["AMZN"] = 1.0
    signals["BUY_AMZN"] = buy_amzn

    return signals
