import inspect


def _assert(b: bool, msg: str = ''):
    s = inspect.stack()
    f = s[2]
    if not b:
        context_str = '(missing context)' if f.code_context is None else ''.join(list(f.code_context)).strip()
        raise AssertionError(f"{context_str} failed in {f.function=} at {f.lineno=}")


def assert_true(b: bool, msg: str = ''):
    _assert(b, msg)


def assert_false(b: bool, msg: str = ''):
    _assert(not b, msg)


def assert_eq(o1, o2, msg: str = ''):
    _assert(o1 == o2, msg)


def assert_not_eq(o1, o2, msg: str = ''):
    _assert(o1 != o2, msg)


def assert_lt(o1, o2, msg: str = ''):
    _assert(o1 < o2, msg)


def assert_le(o1, o2, msg: str = ''):
    _assert(o1 <= o2, msg)


def assert_gt(o1, o2, msg: str = ''):
    _assert(o1 > o2, msg)


def assert_ge(o1, o2, msg: str = ''):
    _assert(o1 >= o2, msg)


def assert_is(o1, o2, msg: str = ''):
    _assert(o1 is o2, msg)


def assert_is_not(o1, o2, msg: str = ''):
    _assert(o1 is not o2, msg)


def assert_in(o1, o2, msg: str = ''):
    _assert(o1 in o2, msg)






# def test_assert_true():
#    assert_true(1 < 0, "Expect 1 less than 0")
