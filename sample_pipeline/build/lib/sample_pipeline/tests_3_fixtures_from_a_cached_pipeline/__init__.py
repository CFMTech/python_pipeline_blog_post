import pickle
from pathlib import Path

import dask

from sample_pipeline.pipeline import get_full_pipeline

CACHED_PIPELINE_PATH = Path(__file__).parent / "cached_pipeline"


def load_from_cached_pipeline(task_name):
    with open((CACHED_PIPELINE_PATH / task_name).with_suffix(".pickle"), "rb") as fp:
        return pickle.load(fp)


def generate_cached_pipeline(tickers, start_date, end_date):
    # Dict of delayed operations
    full_pipeline = get_full_pipeline(tickers, start_date, end_date)

    # Evaluate them
    _compute = dask.compute(full_pipeline)

    # The value returned by dask.compute is a tuple of one element
    (_evaluated_pipeline,) = _compute

    # Dump the values on disk
    CACHED_PIPELINE_PATH.mkdir(exist_ok=True)
    for task_name, task_value in _evaluated_pipeline.items():
        with open(
            (CACHED_PIPELINE_PATH / task_name).with_suffix(".pickle"), "wb"
        ) as fp:
            pickle.dump(task_value, fp)
