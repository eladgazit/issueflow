import pytest

from issueflow.sources.http import HttpError
from issueflow.sources.rate_limit import RateLimiter, RateLimitExceeded, is_rate_limit_error

NOW = 1_000.0


def limiter(sleeps: list[float], max_wait: float = 60.0) -> RateLimiter:
    return RateLimiter(max_wait=max_wait, clock=lambda: NOW, sleep=sleeps.append)


def test_no_headers_seen_means_no_wait() -> None:
    sleeps: list[float] = []
    limiter(sleeps).before_request()
    assert sleeps == []


def test_remaining_quota_means_no_wait() -> None:
    sleeps: list[float] = []
    rl = limiter(sleeps)
    rl.update({"x-ratelimit-remaining": "42", "x-ratelimit-reset": str(NOW + 30)})
    rl.before_request()
    assert sleeps == []


def test_exhausted_quota_sleeps_until_reset() -> None:
    sleeps: list[float] = []
    rl = limiter(sleeps)
    rl.update({"x-ratelimit-remaining": "0", "x-ratelimit-reset": str(NOW + 30)})
    rl.before_request()
    assert sleeps == [30.0]


def test_wait_exactly_at_cap_is_allowed() -> None:
    sleeps: list[float] = []
    rl = limiter(sleeps, max_wait=30)
    rl.update({"x-ratelimit-remaining": "0", "x-ratelimit-reset": str(NOW + 30)})
    rl.before_request()
    assert sleeps == [30.0]


def test_wait_beyond_cap_fails_fast_without_sleeping() -> None:
    sleeps: list[float] = []
    rl = limiter(sleeps, max_wait=60)
    rl.update({"x-ratelimit-remaining": "0", "x-ratelimit-reset": str(NOW + 3000)})
    with pytest.raises(RateLimitExceeded) as excinfo:
        rl.before_request()
    assert sleeps == []
    assert excinfo.value.wait == 3000
    assert "3000s" in str(excinfo.value)


def test_reset_already_passed_does_not_sleep() -> None:
    sleeps: list[float] = []
    rl = limiter(sleeps)
    rl.update({"x-ratelimit-remaining": "0", "x-ratelimit-reset": str(NOW - 5)})
    rl.before_request()
    assert sleeps == []


def test_state_clears_after_waiting() -> None:
    sleeps: list[float] = []
    rl = limiter(sleeps)
    rl.update({"x-ratelimit-remaining": "0", "x-ratelimit-reset": str(NOW + 10)})
    rl.before_request()
    rl.before_request()
    assert sleeps == [10.0]


def test_retry_after_header_is_honored() -> None:
    sleeps: list[float] = []
    rl = limiter(sleeps)
    rl.update({"retry-after": "7"})
    rl.before_request()
    assert sleeps == [7.0]


def test_later_response_with_quota_replaces_exhausted_state() -> None:
    sleeps: list[float] = []
    rl = limiter(sleeps)
    rl.update({"x-ratelimit-remaining": "0", "x-ratelimit-reset": str(NOW + 10)})
    rl.update({"x-ratelimit-remaining": "5000", "x-ratelimit-reset": str(NOW + 3600)})
    rl.before_request()
    assert sleeps == []


@pytest.mark.parametrize(
    ("status", "headers", "expected"),
    [
        (403, {"x-ratelimit-remaining": "0"}, True),
        (429, {"x-ratelimit-remaining": "0"}, True),
        (403, {"retry-after": "60"}, True),
        (429, {"retry-after": "60"}, True),
        (403, {"x-ratelimit-remaining": "10"}, False),  # plain permission error
        (404, {"x-ratelimit-remaining": "0"}, False),
        (500, {"retry-after": "5"}, False),
    ],
)
def test_is_rate_limit_error(status: int, headers: dict[str, str], expected: bool) -> None:
    assert is_rate_limit_error(HttpError(status, headers)) is expected


def test_non_http_error_is_not_rate_limit_error() -> None:
    assert is_rate_limit_error(ValueError("x")) is False
