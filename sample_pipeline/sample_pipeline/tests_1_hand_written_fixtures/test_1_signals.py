import pandas as pd

from sample_pipeline.signals import get_signals


def test_get_signals(closes, volumes, tickers):
    signals = get_signals(closes, volumes)

    assert isinstance(signals, dict)
    assert len(signals) >= 2
    for signal_name, signal in signals.items():
        assert isinstance(signal, pd.DataFrame), signal_name
        assert set(signal.columns) == tickers, signal_name
