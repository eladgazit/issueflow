import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass

from issueflow.sources.http import HttpError


class RateLimitExceeded(Exception):
    """The quota is exhausted and the reset is further away than we are willing to wait."""

    def __init__(self, wait: float, max_wait: float) -> None:
        super().__init__(
            f"GitHub rate limit exhausted; it resets in {wait:.0f}s, which exceeds the "
            f"{max_wait:.0f}s wait cap. Retry later or use --limit to fetch fewer issues."
        )
        self.wait = wait
        self.max_wait = max_wait


def is_rate_limit_error(exc: Exception) -> bool:
    """GitHub signals rate limiting with 403/429 plus an exhausted quota or Retry-After."""
    if not isinstance(exc, HttpError) or exc.status not in (403, 429):
        return False
    return exc.headers.get("x-ratelimit-remaining") == "0" or "retry-after" in exc.headers


@dataclass
class _State:
    remaining: int | None = None
    reset_at: float | None = None  # epoch seconds


class RateLimiter:
    """Tracks X-RateLimit-* / Retry-After and waits for the reset when it is short enough."""

    def __init__(
        self,
        max_wait: float = 60.0,
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._max_wait = max_wait
        self._clock = clock
        self._sleep = sleep
        self._state = _State()

    def update(self, headers: Mapping[str, str]) -> None:
        """Record quota info from (lower-cased) response headers."""
        if "retry-after" in headers:
            self._state = _State(0, self._clock() + float(headers["retry-after"]))
        elif "x-ratelimit-remaining" in headers:
            reset = headers.get("x-ratelimit-reset")
            self._state = _State(
                int(headers["x-ratelimit-remaining"]),
                float(reset) if reset is not None else None,
            )

    def before_request(self) -> None:
        """Sleep until the reset if the quota is exhausted, or raise if that is too long."""
        state = self._state
        if state.remaining != 0 or state.reset_at is None:
            return
        wait = state.reset_at - self._clock()
        if wait > self._max_wait:
            raise RateLimitExceeded(wait, self._max_wait)
        if wait > 0:
            self._sleep(wait)
        self._state = _State()
