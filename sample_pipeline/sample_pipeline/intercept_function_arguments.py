from contextlib import contextmanager
from importlib import import_module
from inspect import signature
from unittest import mock


class StopComputation(Exception):
    """An exception that stops the current computation"""


@contextmanager
def intercept_function_arguments(fun_path, ret_kwargs):
    """
    Intercept the (first) call to the function with reference 'fun_path'
    and return the arguments passed to that function by reference in ret_kwargs

    For examples, see 'test_intercept_function_arguments' in the same repo

    fun_path: a reference to a Python function (to be mocked using mock.patch)
    ret_kwargs: a (reference) to an empty dictionary
    """
    assert (
        isinstance(ret_kwargs, dict) and not ret_kwargs
    ), "ret_kwargs must be an empty dictionary"

    assert isinstance(fun_path, str)
    assert "." in fun_path
    module, fun_name = fun_path.rsplit(".", maxsplit=1)
    fun = getattr(import_module(module), fun_name)
    assert callable(fun), "'fun_path' must be a reference to a callable"

    fun_signature = signature(fun)

    def get_args_and_stop(*args, **kwargs):
        # We bind the function arguments with the function signature
        # (and raise TypeErrors when arguments don't match)
        bound_args = fun_signature.bind(*args, **kwargs)

        # Positional arguments
        for key, value in zip(fun_signature.parameters, bound_args.args):
            ret_kwargs[key] = value

        # Named arguments
        ret_kwargs.update(bound_args.kwargs)

        raise StopComputation()

    with mock.patch(fun_path, get_args_and_stop):
        try:
            yield
        except StopComputation:
            pass
        else:
            raise RuntimeError(f"{fun_path} was not called")
