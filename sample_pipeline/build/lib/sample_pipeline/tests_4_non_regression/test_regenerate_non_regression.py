import pytest

from . import NON_REGRESSION_DATA_FILE, generate_non_regression_data


def test_regenerate_non_regression_data():
    if NON_REGRESSION_DATA_FILE.is_file():
        pytest.skip("The non-regression data exists already")
        return

    generate_non_regression_data()
    raise RuntimeError("The non-regression data was re-generated - is this expected?")
