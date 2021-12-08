import logging

import pandas as pd
import pandas_datareader as wb

LOGGER = logging.getLogger(__name__)


def get_yahoo_data(tickers, start_date, end_date):
    """Return a dict ticker => yahoo data
    (data frame: index = dates, columns = Close prices, Volumes, etc)"""
    LOGGER.info("Loading price data from Yahoo finance")
    return {
        ticker: wb.DataReader(ticker, "yahoo", start_date, end_date)
        for ticker in tickers
    }


def _extract_field(yahoo_data, field):
    """Return a data frame with a single metric
    (columns = tickers, index = dates)"""
    return pd.concat(
        {ticker: ticker_data[field] for ticker, ticker_data in yahoo_data.items()},
        axis=1,
        # concat has a "sort" argument but it is not effective
        # sort=True,
    ).sort_index(axis=1)


def get_closes(yahoo_data):
    """Return a data frame with close prices
    (columns = tickers, index = dates)"""
    LOGGER.info("Loading close prices")
    return _extract_field(yahoo_data, "Close")


def get_volumes(yahoo_data):
    """Return a data frame with volumes
    (columns = tickers, index = dates)"""
    LOGGER.info("Loading volumes")
    return _extract_field(yahoo_data, "Volume")
