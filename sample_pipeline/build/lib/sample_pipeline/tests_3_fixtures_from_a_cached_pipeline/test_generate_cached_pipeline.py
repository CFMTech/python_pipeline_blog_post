from . import generate_cached_pipeline


def test_generate_cached_pipeline(tickers, start_date, end_date):
    generate_cached_pipeline(tickers, start_date, end_date)
