"""In this conftest we expose the pipeline nodes as fixtures the simulation pipeline starting with a small data set"""

import pytest

from sample_pipeline.data import get_closes, get_volumes, get_yahoo_data


@pytest.fixture(scope="session")
def start_date():
    """A sample start date for the pipeline"""
    return "2021-01-04"


@pytest.fixture(scope="session")
def end_date():
    """A sample end date for the pipeline"""
    return "2021-01-29"


@pytest.fixture(scope="session")
def tickers():
    """A sample list of tickers"""
    return {"AAPL", "MSFT", "AMZN", "GOOGL"}


@pytest.fixture(scope="session")
def yahoo_data(tickers, start_date, end_date):
    return get_yahoo_data(tickers, start_date, end_date)


@pytest.fixture(scope="session")
def closes(yahoo_data):
    return get_closes(yahoo_data)


@pytest.fixture(scope="session")
def volumes(yahoo_data):
    return get_volumes(yahoo_data)
