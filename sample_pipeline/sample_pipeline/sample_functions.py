CONSTANT = 1


def f(a, b, c):
    assert b < 0
    return a + b + c


def g(a, b):
    return f(a, b, 0)
