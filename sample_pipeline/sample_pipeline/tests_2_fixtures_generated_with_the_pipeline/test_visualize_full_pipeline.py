import pytest

from sample_pipeline.pipeline import get_full_pipeline

pytest.importorskip("graphviz")


def test_visualize_full_pipeline(tickers, start_date, end_date):
    get_full_pipeline(tickers, start_date, end_date)["signals"].visualize()
