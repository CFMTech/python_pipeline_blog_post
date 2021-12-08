import shutil

import pytest

from . import NON_REGRESSION_DATA_FILE, generate_non_regression_data


def test_regenerate_non_regression_data(tmp_path):
    """This test regenerates the non-regression data if it does not exist already"""
    non_reg_file = tmp_path / "non_reg.pickle"

    # NB: in practice, if the re-generation takes a while,
    # you might want to move this AFTER the conditional skip
    generate_non_regression_data(non_reg_file)

    if NON_REGRESSION_DATA_FILE.is_file():
        pytest.skip("The non-regression data exists already")

    shutil.move(non_reg_file, NON_REGRESSION_DATA_FILE)
    raise RuntimeError("The non-regression data was re-generated - is this expected?")
