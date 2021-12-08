import pytest
from dask.delayed import Delayed
from deepdiff import DeepDiff

from . import get_non_regression_pipeline, load_non_regression_data


@pytest.fixture
def non_regression_data():
    return load_non_regression_data()


def node_iterator():
    """Iterate over the nodes in the pipeline"""
    for name, node in get_non_regression_pipeline().items():
        yield name, node


@pytest.mark.parametrize("name,node", node_iterator())
def test_non_regression(name, node, non_regression_data):
    """For each node in the data pipeline, load the inputs from a
    reference run, evaluate the node, and compare the new output with
    the output from the reference run"""
    expected = non_regression_data[name]

    # Load the inputs for the given node from the reference non-reg data
    inputs = {
        input_name: non_regression_data[input_name]
        for input_name in node.dask.dependencies[name]
    }

    # And evaluate the node given the inputs above
    node_with_inputs_from_non_reg_data = Delayed(name, dict(node.dask, **inputs))
    actual = node_with_inputs_from_non_reg_data.compute()

    # ######################################
    # ### There should be no difference! ###
    # ######################################

    diff = DeepDiff(actual, expected)
    if diff:
        raise ValueError(
            f"The value for {name} has changed. "
            f"You can either revert the change, or, if you understand the new values, "
            f"you can delete the non-regression file `non_regression_data.pickle` "
            f"and regenerate it by running `test_regenerate_non_regression_data`.\n"
            f"Differences: {diff}"
        )
