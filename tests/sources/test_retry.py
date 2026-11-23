import urllib.error

import pytest

from issueflow.sources.http import HttpError
from issueflow.sources.retry import RetryPolicy


class Flaky:
    """Raises the scripted errors in order, then returns 'ok'."""

    def __init__(self, *errors: Exception) -> None:
        self.errors = list(errors)
        self.calls = 0

    def __call__(self) -> str:
        self.calls += 1
        if self.errors:
            raise self.errors.pop(0)
        return "ok"


def policy(sleeps: list[float], rng: float = 1.0, **kwargs: float) -> RetryPolicy:
    return RetryPolicy(sleep=sleeps.append, random=lambda: rng, **kwargs)


def err(status: int) -> HttpError:
    return HttpError(status, {})


def test_success_first_try_does_not_sleep() -> None:
    sleeps: list[float] = []
    fn = Flaky()
    assert policy(sleeps).call(fn) == "ok"
    assert fn.calls == 1
    assert sleeps == []


def test_retries_5xx_then_succeeds() -> None:
    sleeps: list[float] = []
    fn = Flaky(err(500), err(503))
    assert policy(sleeps).call(fn) == "ok"
    assert fn.calls == 3
    assert len(sleeps) == 2


@pytest.mark.parametrize("exc", [urllib.error.URLError("dns"), TimeoutError(), err(502)])
def test_retryable_errors(exc: Exception) -> None:
    fn = Flaky(exc)
    assert policy([]).call(fn) == "ok"
    assert fn.calls == 2


@pytest.mark.parametrize("exc", [err(400), err(401), err(403), err(404), err(422), err(429), ValueError("x")])
def test_other_errors_are_never_retried(exc: Exception) -> None:
    sleeps: list[float] = []
    fn = Flaky(exc)
    with pytest.raises(type(exc)):
        policy(sleeps).call(fn)
    assert fn.calls == 1
    assert sleeps == []


def test_gives_up_after_max_attempts_and_raises_last_error() -> None:
    sleeps: list[float] = []
    errors = [err(500), err(501), err(502)]
    fn = Flaky(*errors)
    with pytest.raises(HttpError) as excinfo:
        policy(sleeps, max_attempts=3).call(fn)
    assert excinfo.value is errors[-1]
    assert fn.calls == 3
    assert len(sleeps) == 2  # no sleep after the final attempt


def test_default_is_five_attempts() -> None:
    fn = Flaky(*[err(500) for _ in range(10)])
    with pytest.raises(HttpError):
        policy([]).call(fn)
    assert fn.calls == 5


def test_max_attempts_one_never_retries() -> None:
    fn = Flaky(err(500))
    with pytest.raises(HttpError):
        policy([], max_attempts=1).call(fn)
    assert fn.calls == 1


def test_exponential_ceiling_capped_at_max_delay() -> None:
    sleeps: list[float] = []
    fn = Flaky(*[err(500) for _ in range(7)])
    with pytest.raises(HttpError):
        policy(sleeps, rng=1.0, max_attempts=7, base_delay=1, max_delay=10).call(fn)
    assert sleeps == [1, 2, 4, 8, 10, 10]


def test_full_jitter_scales_ceiling_by_random() -> None:
    sleeps: list[float] = []
    fn = Flaky(err(500), err(500), err(500))
    policy(sleeps, rng=0.5).call(fn)
    assert sleeps == [0.5, 1.0, 2.0]


def test_zero_random_means_no_wait() -> None:
    sleeps: list[float] = []
    policy(sleeps, rng=0.0).call(Flaky(err(500)))
    assert sleeps == [0.0]


def test_invalid_max_attempts_rejected() -> None:
    with pytest.raises(ValueError):
        RetryPolicy(max_attempts=0)
