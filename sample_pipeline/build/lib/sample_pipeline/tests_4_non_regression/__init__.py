import pickle
from functools import lru_cache
from pathlib import Path

import dask

from sample_pipeline.pipeline import get_full_pipeline

NON_REGRESSION_DATA_FILE = Path(__file__).parent / "non_regression_data.pickle"
NON_REGRESSION_TICKERS = {"AAPL", "MSFT", "AMZN", "GOOGL"}
NON_REGRESSION_START_DATE = "2021-01-04"
NON_REGRESSION_END_DATE = "2021-01-29"


def get_non_regression_pipeline():
    return get_full_pipeline(
        tickers=NON_REGRESSION_TICKERS,
        start_date=NON_REGRESSION_START_DATE,
        end_date=NON_REGRESSION_END_DATE,
    )


@lru_cache()
def load_non_regression_data():
    with open(NON_REGRESSION_DATA_FILE, "rb") as fp:
        return pickle.load(fp)


def generate_non_regression_data():
    # Dict of delayed operations
    full_pipeline = get_non_regression_pipeline()

    # Evaluate them
    _compute = dask.compute(full_pipeline)

    # The value returned by dask.compute is a tuple of one element
    (_evaluated_pipeline,) = _compute

    # Dump the values on disk
    with open(NON_REGRESSION_DATA_FILE, "wb") as fp:
        pickle.dump(_evaluated_pipeline, fp)
