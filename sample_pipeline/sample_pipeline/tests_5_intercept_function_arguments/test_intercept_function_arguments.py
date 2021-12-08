import pytest

from sample_pipeline.intercept_function_arguments import intercept_function_arguments
from sample_pipeline.sample_functions import g


def test_intercept_positional_args():
    fun_path = "sample_pipeline.sample_functions.f"
    fun_args = {}
    with intercept_function_arguments(fun_path, fun_args):
        from sample_pipeline.sample_functions import f

        f(1, 2, 3)
    assert fun_args == {"a": 1, "b": 2, "c": 3}


def test_intercept_named_args():
    fun_path = "sample_pipeline.sample_functions.f"
    fun_args = {}
    with intercept_function_arguments(fun_path, fun_args):
        from sample_pipeline.sample_functions import f

        f(c=3, b=2, a=1)
    assert fun_args == {"a": 1, "b": 2, "c": 3}


def test_intercept_mixed_args():
    fun_path = "sample_pipeline.sample_functions.f"
    fun_args = {}
    with intercept_function_arguments(fun_path, fun_args):
        from sample_pipeline.sample_functions import f

        f(1, c=3, b=2)
    assert fun_args == {"a": 1, "b": 2, "c": 3}


def test_not_intercepted():
    """In this test the function was imported before the context manager
    so it can't be patched"""
    from sample_pipeline.sample_functions import f

    fun_path = "sample_pipeline.sample_functions.f"
    fun_args = {}

    with pytest.raises(
        RuntimeError, match=r"sample_pipeline.sample_functions.f was not called"
    ):
        with intercept_function_arguments(fun_path, fun_args):
            assert f(1, -2, 3) == 2


def test_intercept_imported():
    """We can import a function that imports the target function"""
    # 'fun_path' must be the location at which the function is imported
    fun_path = "sample_pipeline.sample_functions.f"
    fun_args = {}

    with intercept_function_arguments(fun_path, fun_args):
        assert g(1, 2) != 3  # not executed

    assert fun_args == {"a": 1, "b": 2, "c": 0}


def test_missing_arguments_direct():
    with pytest.raises(
        TypeError, match=r"f\(\) missing 1 required positional argument: 'c'"
    ):
        from sample_pipeline.sample_functions import f

        f(1, 2)


def test_missing_arguments_intercept():
    fun_path = "sample_pipeline.sample_functions.f"
    fun_args = {}
    with pytest.raises(TypeError, match=r"missing a required argument: 'c'"):
        with intercept_function_arguments(fun_path, fun_args):
            from sample_pipeline.sample_functions import f

            f(1, 2)


def test_too_many_positional_arguments_direct():
    with pytest.raises(
        TypeError, match=r"f\(\) takes 3 positional arguments but 4 were given"
    ):
        from sample_pipeline.sample_functions import f

        f(1, 2, 3, 4)


def test_too_many_positional_arguments_intercept():
    fun_path = "sample_pipeline.sample_functions.f"
    fun_args = {}
    with pytest.raises(TypeError, match=r"too many positional arguments"):
        with intercept_function_arguments(fun_path, fun_args):
            from sample_pipeline.sample_functions import f

            f(1, 2, 3, 4)


def test_wrong_argument_direct():
    with pytest.raises(
        TypeError, match=r"f\(\) got an unexpected keyword argument 'd'"
    ):
        from sample_pipeline.sample_functions import f

        f(d=4)


def test_wrong_argument_intercept():
    fun_path = "sample_pipeline.sample_functions.f"
    fun_args = {}
    with pytest.raises(TypeError, match=r"missing a required argument: 'a'"):
        with intercept_function_arguments(fun_path, fun_args):
            from sample_pipeline.sample_functions import f

            f(d=4)


def test_wrong_path():
    fun_path = "sample_pipeline.sample_functions.h"
    fun_args = {}
    with pytest.raises(
        AttributeError,
        match=r"module 'sample_pipeline.sample_functions' has no attribute 'h'",
    ):
        with intercept_function_arguments(fun_path, fun_args):
            g()


def test_path_not_callable():
    fun_path = "sample_pipeline.sample_functions.CONSTANT"
    fun_args = {}
    with pytest.raises(
        AssertionError, match=r"'fun_path' must be a reference to a callable"
    ):
        with intercept_function_arguments(fun_path, fun_args):
            g()
