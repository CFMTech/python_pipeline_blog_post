import pytest
from dask.delayed import Delayed, delayed


def inc(x):
    return x + 1


@pytest.fixture
def b():
    a = delayed(inc)(1, dask_key_name="a")
    return delayed(inc)(a, dask_key_name="b")


def test_compute(b):
    assert b.compute() == 3


def test_compute_partial(b):
    assert b.dask.dependencies["b"] == {"a"}
    b_mod = Delayed("b", dict(b.dask, a=4))
    assert b_mod.compute() == 5
