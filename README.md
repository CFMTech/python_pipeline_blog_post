Advanced Testing Techniques for your Python Data Pipeline with Dask and Pytest Fixtures
=======================================================================================
Achieve full coverage; Use fixtures and write tests easily; Refactor in confidence with non-regression tests
------------------------------------------------------------------------------------------------------------

![CI](https://github.com/mwouts/python_pipeline_blog_post/workflows/CI/badge.svg)
[![codecov.io](https://codecov.io/github/mwouts/python_pipeline_blog_post/coverage.svg?branch=main)](https://codecov.io/gh/mwouts/python_pipeline_blog_post/branch/main)

![](joshua-sortino-LqKhnDzSF-8-unsplash.jpg)
(Photo by <a href="https://unsplash.com/@sortino?utm_source=unsplash&utm_medium=referral&utm_content=creditCopyText">Joshua Sortino</a> on <a href="https://unsplash.com/?utm_source=unsplash&utm_medium=referral&utm_content=creditCopyText">Unsplash</a>)

[CFM](https://www.cfm.fr) is a Quantitative Hedge Fund with more than 30 years of experience in the domain of quantitative investing. Our daily production (and research) is done with a complex Python data pipeline.

In this post, we share our experience in writing tests for the pipeline. Our first objective was to improve the coverage of the pipeline. Then, we also wanted to improve the identification of the contributions that could break any production or research usage.

The techniques that we have used, and that we document in this article are:
- Test the functions using fixtures
- Generate fixtures for every node of the pipeline
- Identify unexpected code impacts with non-regression tests
- Put breakpoints programmatically and compare the arguments passed to a given function in two different versions of the pipeline, to refactor with confidence.

# Introducing our Python Pipeline

We use a pipeline to best combine the research contributions of the different teams. For instance, we have tasks dedicated to
- **data** streams (financial information like past prices, economic data, news, ...)
- **signals**, derived from the data, that express numerically views on future returns, for one or more financial instruments
- and many other tasks up to portfolio construction.

A convenient way to schedule this tools is to use a pipeline. At CFM, we use an in-house Python utility for this, but for this post, I'll assume that our pipeline is implemented with [Dask](https://dask.org/) (see this video: [Next Generation Big Data Pipelines with Prefect and Dask](https://www.youtube.com/watch?v=R6z77ZNJvho) for a review of Dask and a few other alternatives). Also, for the sake of simplicity our sample pipeline is made of only four nodes:
```python
from dask.delayed import delayed, Delayed

from .data import get_closes, get_volumes, get_yahoo_data
from .signals import get_signals


def get_full_pipeline(tickers, start_date, end_date):
    """Return the full simulation pipeline"""
    yahoo_data = delayed(get_yahoo_data)(
        tickers, start_date, end_date, dask_key_name="yahoo_data"
    )
    volumes = delayed(get_volumes)(yahoo_data, dask_key_name="volumes")
    closes = delayed(get_closes)(yahoo_data, dask_key_name="closes")
    signals = delayed(get_signals)(closes, volumes, dask_key_name="signals")  # noqa

    # Return a dict with all the nodes
    return {name: task for name, task in locals().items() if isinstance(task, Delayed)}
```

We can visualize the pipeline with `pipeline.visualize()`:

![](https://github.com/CFMTech/python_pipeline_blog_post/raw/main/sample_pipeline/sample_pipeline/tests_2_fixtures_generated_with_the_pipeline/mydask.png)

Combining the nodes into a pipeline is a great improvement compared to using a simple bash scheduler:
- With the pipeline, running the pipeline in full is accessible to every user, since the dependencies are taken care of by the pipeline. The users do not need any more to have experience with all the tasks in the pipeline.
- The nodes must be implemented with _[pure functions](https://en.wikipedia.org/wiki/Pure_function)_, that is, functions with an output that is deterministic given the inputs, and have no side effect. This is a very important requirement - not using pure functions will cause lots of complexity and extra maintenance work when a task must be regenerated.
- The pipeline, being more Python-oriented, is also notebook friendly - isn't it easier to call a function than run a script in a Jupyter notebook?

# Testing the pipeline and the nodes

Our next objective after building our pipeline is to improve coverage. While coverage is not enough to make sure that you will detect problems with a new contribution, it is necessary. Without tests, the pipeline will often break in research.

A simple way to improve coverage is to run the pipeline in full on the CI. Obviously, we don't use the full configuration - our test pipeline runs for just two portfolios, a handful of financial instruments, and extends over just a few days. Overall it runs in under one minute.

But we wanted to go a bit further. We have observed that many contributors find it difficult to write tests because preparing the inputs for the function to be tested is challenging. And many tests were long to write just because of the data preparation code:
```python
def test_complex_function():
    # complex and long code to prepare the sample inputs
    tickers = ...
    volumes = ...
    closes = ...

    # Call the function
    res = complex_function(tickers, volumes, closes)

    # Few asserts on the result
    assert isinstance(res, pd.DataFrame)
    assert set(res.columns) == set(tickers)
    ...
```

We first decided to separate the test input preparation from the test itself. We created `fixtures` (see below) that can be used in as many tests as we want. With fixtures at hand, writing a test becomes very easy - the example above becomes much shorter indeed:
```python

def test_complex_function(tickers, volumes, closes):
    # Call the function
    res = complex_function(tickers, volumes, closes)

    # Few asserts on the result
    assert isinstance(res, pd.DataFrame)
    assert set(res.columns) == set(tickers)
    ...
```

Another important point is that, within a pipeline, the inputs for a node are its parent nodes. So, we decided to generate the fixtures directly from the pipeline - but before discussing that, let us introduce the fixtures.

## Introducing pytest fixtures

A [pytest fixture](https://docs.pytest.org/en/6.2.x/fixture.html) is a function decorated with `pytest.fixture`. You can define the fixtures either directly in the test file, or in a [`conftest.py`](https://docs.pytest.org/en/6.2.x/fixture.html#conftest-py-sharing-fixtures-across-multiple-files) file in the same or in a parent directory.

For our first fixture example in the folder [tests_1_hand_written_fixtures](https://github.com/CFMTech/python_pipeline_blog_post/tree/main/sample_pipeline/sample_pipeline/tests_1_hand_written_fixtures), we create the fixtures by just calling the corresponding functions:

```python
import pytest

from sample_pipeline.data import get_closes, get_volumes, get_yahoo_data


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
def yahoo_data(tickers, start_date, end_date):
    return get_yahoo_data(tickers, start_date, end_date)


@pytest.fixture(scope="session")
def closes(yahoo_data):
    return get_closes(yahoo_data)


@pytest.fixture(scope="session")
def volumes(yahoo_data):
    return get_volumes(yahoo_data)
```

Now we can use these fixtures in the test just by putting them as arguments to the test:

```python
from sample_pipeline.signals import get_signals


def test_get_signals(tickers, closes, volumes):
    signals = get_signals(closes, volumes)
    assert isinstance(signals, dict)
    assert len(signals) >= 2
    for signal_name, signal in signals:
        assert isinstance(signals, pd.DataFrame), signal_name
        assert set(signal.columns) == tickers, signal_name
```

## Why use `scope="session"`?

Our test pipeline executes in full in under one minute, but we don't want to multiply this by the number of tests. With the `scope="session"` option, we save a lot of time as the fixtures are generated just once (per worker, so they will still be generated multiple times if you use `pytest-xdist`).

For instance, if we launch the tests in `test_1_data.py`, you see that the fixtures are generated on demand, and just once for each of them (cf. the INFO logs).

```
============================= test session starts ==============================
collecting ... collected 3 items

test_1_data.py::test_get_yahoo_data
test_1_data.py::test_get_closes
test_1_data.py::test_get_volumes

============================== 3 passed in 5.30s ===============================

Process finished with exit code 0

-------------------------------- live log call ---------------------------------
INFO     sample_pipeline.data:data.py:12 Loading price data from Yahoo finance
PASSED                                                                   [ 33%]
-------------------------------- live log setup --------------------------------
INFO     sample_pipeline.data:data.py:12 Loading price data from Yahoo finance
-------------------------------- live log call ---------------------------------
INFO     sample_pipeline.data:data.py:33 Loading close prices
PASSED                                                                   [ 66%]
-------------------------------- live log call ---------------------------------
INFO     sample_pipeline.data:data.py:40 Loading volumes
PASSED                                                                   [100%]
```

It is legitimate to use `scope="session"` as we work with pure functions - as required by the pipeline. If you are not so sure that your functions are pure, and want to double-check that they do not modify their input by reference, you can do so in the fixture teardown:
```python
from copy import deepcopy
from deepdiff import DeepDiff

@pytest.fixture(scope="session")
def closes(yahoo_data):
    # Compute the fixture
    value_org = get_closes(yahoo_data)

    # Make a copy
    value = deepcopy(value_org)

    # Yield the copy and run all the selected tests
    yield value

    # In the fixture teardown, make sure that no test modified the value
    assert not DeepDiff(value, value_org)
```

In this example we have used [DeepDiff](https://zepworks.com/deepdiff/current/index.html) to show the recursive differences between two Python objects - this sounds like a great library, but I have to mention that I have no extended experience with it.

## Do I need to duplicate the pipeline in the conftest.py?

In our first example ([tests_1_hand_written_fixtures](https://github.com/CFMTech/python_pipeline_blog_post/tree/main/sample_pipeline/sample_pipeline/tests_1_hand_written_fixtures)), we have actually re-implemented the pipeline in the `conftest.py` file. This causes duplication and will require specific maintenance when you change the pipeline, so we recommend this approach only for short and stable pipelines.

In the second folder [tests_2_fixtures_generated_with_the_pipeline](https://github.com/CFMTech/python_pipeline_blog_post/tree/main/sample_pipeline/sample_pipeline/tests_2_fixtures_generated_with_the_pipeline), we used another approach. We created a fixture for the evaluated pipeline - a dictionary with the value for every node - and then we exposed each node as a fixture.
```python
@pytest.fixture(scope="session")
def evaluated_pipeline(tickers, start_date, end_date):
    # The pipeline
    full_pipeline = get_full_pipeline(tickers, start_date, end_date)

    # Evaluate all the tasks
    _compute = dask.compute(full_pipeline)

    # The value returned by dask.compute is a tuple of one element
    (_evaluated_pipeline,) = _compute

    return _evaluated_pipeline


@pytest.fixture(scope="session")
def yahoo_data(evaluated_pipeline):
    return evaluated_pipeline["yahoo_data"]


@pytest.fixture(scope="session")
def closes(evaluated_pipeline):
    return evaluated_pipeline["closes"]


@pytest.fixture(scope="session")
def volumes(evaluated_pipeline):
    return evaluated_pipeline["volumes"]
```

The advantages of that approach are:
- Low maintenance: you just need to add or remove fixtures when a node is added to or removed from the pipeline. Changes on the node arguments are automatically replicated on the fixtures.
- The pipeline is covered in full - no matter if some nodes are not used in the tests, they are evaluated

But it also has a few drawbacks:
- The fixtures become _fragile_. Say you work on the `get_signals` function, start developing and introduce an error in the function... Because of this the pipeline cannot be evaluated anymore. So you cannot get a value for the fixtures, and you are not in a position to launch the test on `get_signals` anymore.
- The fact that the pipeline is evaluated in full makes the creation of the fixtures a bit slower. Now the logs look like this:
```
============================= test session starts ==============================
collecting ... collected 2 items

test_2_data.py::test_get_closes
test_2_data.py::test_get_volumes

============================== 2 passed in 3.16s ===============================

Process finished with exit code 0

-------------------------------- live log setup --------------------------------
INFO     sample_pipeline.data:data.py:12 Loading price data from Yahoo finance
INFO     sample_pipeline.data:data.py:33 Loading close prices
INFO     sample_pipeline.data:data.py:40 Loading volumes
INFO     sample_pipeline.signals:signals.py:8 Computing signals
-------------------------------- live log call ---------------------------------
INFO     sample_pipeline.data:data.py:33 Loading close prices
PASSED                                                                   [ 50%]
-------------------------------- live log call ---------------------------------
INFO     sample_pipeline.data:data.py:40 Loading volumes
PASSED                                                                   [100%]
```
In particular, we see that the signals are generated even if they are not used in the tests.

As a conclusion, this [tests_2_fixtures_generated_with_the_pipeline](https://github.com/CFMTech/python_pipeline_blog_post/tree/main/sample_pipeline/sample_pipeline/tests_2_fixtures_generated_with_the_pipeline) approach is good for the CI, but not for local development.

## Generating the fixtures from a cached run of the pipeline

This is the approach that we use in practice. The pipeline is run in full on the CI, and for local developments, we save the results to a cache.

Our sample implementation is available at [tests_3_fixtures_from_a_cached_pipeline](https://github.com/CFMTech/python_pipeline_blog_post/tree/main/sample_pipeline/sample_pipeline/tests_3_fixtures_from_a_cached_pipeline), and we cite a short extract here:
```python
@pytest.fixture(scope="session")
def cached_pipeline_path(tickers, start_date, end_date, worker_id):
    """This fixture returns the path to the cached pipeline and evaluates the
    pipeline if necessary.

    worker_id: the id of the worker in pytest-xdist
    (remove this argument if you don't use pytest-xdist)
    """
    return get_cached_pipeline_path(tickers, start_date, end_date, worker_id)


@pytest.fixture(scope="session")
def yahoo_data(cached_pipeline_path):
    return load_from_cache(cached_pipeline_path, "yahoo_data")
```

Note that the fixture `cached_pipeline_path` may not return instantly - it will evaluate and cache the full pipeline if necessary (e.g. if it executed on the CI, or if the user removed the local cache).

This approach has many advantages:
- The fixtures are available instantaneously (they are loaded from disk, not computed)
- We can develop freely and make breaking changes locally, that will not affect the fixtures generation (well, not until we decide to regenerate)
- And we get full coverage of the pipeline on the CI.

The only disadvantage of this method is that the developer must be aware of the cache, and will need to know when to remove and regenerate it.

The first time we run the test suite, we see a mention that the cache is being generated, and from the second time on the log will point out to the cache being reused:
```
============================= test session starts ==============================
collecting ... collected 2 items

test_3_data.py::test_get_closes
test_3_data.py::test_get_volumes

============================== 2 passed in 3.22s ===============================

Process finished with exit code 0

-------------------------------- live log setup --------------------------------
INFO     sample_pipeline.tests_3_fixtures_from_a_cached_pipeline:__init__.py:35 Regenerating the cached pipeline at /tmp/cached_pipeline/master at 2021-12-07 15:38:10.157451
INFO     sample_pipeline.data:data.py:12 Loading price data from Yahoo finance
INFO     sample_pipeline.data:data.py:33 Loading close prices
INFO     sample_pipeline.data:data.py:40 Loading volumes
INFO     sample_pipeline.signals:signals.py:8 Computing signals
-------------------------------- live log call ---------------------------------
INFO     sample_pipeline.data:data.py:33 Loading close prices
PASSED                                                                   [ 50%]
-------------------------------- live log call ---------------------------------
INFO     sample_pipeline.data:data.py:40 Loading volumes
PASSED                                                                   [100%]
```

## My pipeline has parameters. Should I write multiple conftests with different fixtures?

We recommend working with only **one** set of fixtures. Maintaining a pipeline of fixtures is an investment in code, in user training, so it is best if everyone knows what the sample pipeline is.

We do understand that some tests require specific inputs. When this is the case, we recommend to _derive_ custom fixtures from the reference ones.

Assume for instance that some signal generation requires that `"FB"` be among the tickers. In that case, we can simply create a new fixture
```python
@pytest.fixture(scope="session")
def tickers_including_fb(tickers):
    return tickers + {"FB"}
```

⚠️Pay attention to not change the original fixture by reference, i.e. do `tickers + {"FB"}` but not `tickers.add('FB')`!

## Non-regression tests

With the test fixtures documented above, we already get excellent coverage for the data pipeline. Still, this is not enough to ensure that the pipeline will work in practical applications.

So we added another kind of test to our platform, the non-regression tests. These tests are run with the complete portfolio configuration, for each portfolio that we have in production. We want a test that is not too slow (< 5 minutes), so we don't cover the full data history but just one month. Also, we might not cover the full cartesian product of portfolios times tasks, but only the selection that most matters to us.

Our sample non-regression test is coded at [tests_4_non_regression](https://github.com/CFMTech/python_pipeline_blog_post/tree/main/sample_pipeline/sample_pipeline/tests_4_non_regression), and the test itself is also reproduced below.

```python
def non_regression_nodes_iterator():
    for name, node in get_non_regression_pipeline().items():
        # Skip the nodes for which you don't want a non-regression test
        if "slow" in name:
            continue
        yield name, node


@pytest.mark.parametrize("name,node", non_regression_nodes_iterator())
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
```

The difference between the non-regression test and the previous test suite, are that
- The non-regression test has better coverage of the production use cases (since it uses the production configuration)
- It will detect any impact on the outputs of the nodes. Unlike the simple tests that we wrote before, we don't only check the shape of the outputs, but also their value.
- It also takes more time to run but gives much more confidence in the updated code.

In our example, we saved the non-regression data into a simple file. When a non-regression occurs and is expected, that file must be updated with the new outputs (i.e. deleted, the framework will regenerate it). It is possible to save the non-regression data outside the project repository (i.e. on disk/url) if it is too big. In that case, make sure the non-regression data sets are incremental (i.e. use a new file name or URL for each new non-regression run), otherwise the non-regression tests on existing branches will break.

## Refactor and test that the arguments passed to a certain function don't change

We will conclude this article with one last technique that we have found useful in the context of large refactorings. The objective is to guarantee that, after the refactoring, a given function is called with the exact same arguments as before (so, in particular, it will have the same outputs).

Our technique is a bit comparable to a breakpoint that we would set programmatically, and that would export the arguments at that point of the program. We have implemented this with a _context manager_. The context manager intercepts the (first) call to the target function, stops the computation, without evaluating the target function, and returns the arguments of the call.

With this `intercept_function_arguments` context manager we can write tests like [test_same_arguments.py](https://github.com/CFMTech/python_pipeline_blog_post/tree/main/sample_pipeline/sample_pipeline/tests_5_intercept_function_arguments/test_same_arguments.py):
```python
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
```

A subtlety in the above is that the target function is patched using `mock.patch`, so you will have to be careful with imports. If you import the target function before entering the `intercept_function_arguments`, then `fun_path` should be the path where the function is imported, see the section on [where to patch](https://docs.python.org/3/library/unittest.mock.html#where-to-patch) in the standard library.

# Conclusion

We hope this post will help you keep your data pipeline under control! As we have seen, creating a fixture for each task in the pipeline makes the writing of tests very easy. The non-regression tests are also super useful to identify unexpected impacts before a contribution gets accepted. And if you want to go further and guarantee that the inputs of a certain function don't change, then the `intercept_function_arguments` technique is all yours!

# Acknowledgments

This article was written by [Marc Wouts](https://github.com/mwouts), a researcher at CFM, and the author of [Jupytext](https://github.com/mwouts/jupytext). Marc would like to thank the Portfolio team for the collaboration on the pipeline, the Open Source Program Office at CFM for the support on this article, [Florent Zara](https://twitter.com/flzara) for the time spent reading the many draft versions of this post, [Emmanuel Serie](https://github.com/eserie) and the [Dask Discourse Group](https://dask.discourse.group) for advice on Dask.
