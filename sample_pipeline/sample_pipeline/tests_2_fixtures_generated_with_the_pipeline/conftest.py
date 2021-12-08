import dask
import pytest

from sample_pipeline.pipeline import get_full_pipeline


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
def evaluated_pipeline(tickers, start_date, end_date):
    # The pipeline
    full_pipeline = get_full_pipeline(tickers, start_date, end_date)

    # Evaluate all the tasks
    _compute = dask.compute(full_pipeline)

    # The value returned by dask.compute is a tuple of one element
    (_evaluated_pipeline,) = _compute

    return _evaluated_pipeline


@pytest.fixture(scope="session")
def yahoo_data(evaluated_pipeline):
    return evaluated_pipeline["yahoo_data"]


@pytest.fixture(scope="session")
def closes(evaluated_pipeline):
    return evaluated_pipeline["closes"]


@pytest.fixture(scope="session")
def volumes(evaluated_pipeline):
    return evaluated_pipeline["volumes"]
