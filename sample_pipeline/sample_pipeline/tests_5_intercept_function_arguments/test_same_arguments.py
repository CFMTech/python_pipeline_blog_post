import pytest
from deepdiff import DeepDiff

from sample_pipeline.data import get_closes, get_volumes, get_yahoo_data
from sample_pipeline.intercept_function_arguments import intercept_function_arguments
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
def old_pipeline(tickers, start_date, end_date):
    """A basic implementation of the pipeline"""

    def compute():
        # We import the target function at the last moment
        # (after entering the 'intercept_function_arguments' context)
        from sample_pipeline.signals import get_signals

        yahoo_data = get_yahoo_data(tickers, start_date, end_date)
        return get_signals(get_closes(yahoo_data), get_volumes(yahoo_data))

    return compute


@pytest.fixture(scope="session")
def new_pipeline(tickers, start_date, end_date):
    """The pipeline implemented with Dask"""

    def compute():
        # The pipeline construction will store 'get_signals', so we need to
        # delay it until we enter the 'intercept_function_arguments' context
        signals = get_full_pipeline(tickers, start_date, end_date)["signals"]

        return signals.compute()

    return compute


def test_same_arguments(new_pipeline, old_pipeline):
    """We test that the two versions of the pipeline result in identical
    parameters passed to get_signals.

    Note that we need to pass the path to the get_signals function that is
    actually used by the pipelines (i.e. sample_pipeline.pipeline.get_signals
    for the Dask pipeline)
    """
    fun_path = "sample_pipeline.signals.get_signals"
    args_old = {}
    with intercept_function_arguments(fun_path, args_old):
        old_pipeline()

    fun_path = "sample_pipeline.pipeline.get_signals"
    args_new = {}
    with intercept_function_arguments(fun_path, args_new):
        new_pipeline()

    assert not DeepDiff(args_new, args_old)
