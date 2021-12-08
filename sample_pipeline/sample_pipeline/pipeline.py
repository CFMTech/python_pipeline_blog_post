from dask.delayed import Delayed, delayed

from .data import get_closes, get_volumes, get_yahoo_data
from .signals import get_signals


def get_full_pipeline(tickers, start_date, end_date):
    """Return the full simulation pipeline"""
    yahoo_data = delayed(get_yahoo_data)(
        tickers, start_date, end_date, dask_key_name="yahoo_data"
    )
    volumes = delayed(get_volumes)(yahoo_data, dask_key_name="volumes")
    closes = delayed(get_closes)(yahoo_data, dask_key_name="closes")
    signals = delayed(get_signals)(closes, volumes, dask_key_name="signals")  # noqa

    # Return a dict with all the nodes
    return {name: task for name, task in locals().items() if isinstance(task, Delayed)}
