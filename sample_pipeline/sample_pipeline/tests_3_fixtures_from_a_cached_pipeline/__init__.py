import logging
import os
import pickle
import shutil
from datetime import datetime, timedelta
from pathlib import Path

import dask

from sample_pipeline.pipeline import get_full_pipeline

LOGGER = logging.getLogger(__name__)

CACHED_PIPELINE_PATH = Path(os.environ.get("TMPDIR", "/tmp")) / "cached_pipeline"


def get_cached_pipeline_path(tickers, start_date, end_date, worker_id):
    cache_path = CACHED_PIPELINE_PATH / worker_id

    # Always regenerate on the CI
    if cache_path.exists() and os.environ.get("CI"):
        shutil.rmtree(cache_path)

    # Regenerate every day
    if cache_path.exists():
        creation_time = datetime.fromtimestamp(cache_path.stat().st_ctime)
        if creation_time < datetime.now() - timedelta(hours=10):
            shutil.rmtree(cache_path)

    if cache_path.exists():
        LOGGER.info(
            f"Loading the cached pipeline from {cache_path} generated at {creation_time}"
        )
    else:
        LOGGER.info(
            f"Regenerating the cached pipeline at {cache_path} at {datetime.now()}"
        )

        # Regenerate the cache on disk
        cache_path.mkdir(parents=True)

        # Dict of delayed operations
        full_pipeline = get_full_pipeline(tickers, start_date, end_date)

        # Evaluate them
        _compute = dask.compute(full_pipeline)

        # The value returned by dask.compute is a tuple of one element
        (_evaluated_pipeline,) = _compute

        # Dump the values on disk
        for task_name, task_value in _evaluated_pipeline.items():
            with open((cache_path / task_name).with_suffix(".pickle"), "wb") as fp:
                pickle.dump(task_value, fp)

    return cache_path


def load_from_cache(cached_pipeline_path, task_name):
    with open((cached_pipeline_path / task_name).with_suffix(".pickle"), "rb") as fp:
        return pickle.load(fp)
