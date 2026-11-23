import random
import time
import urllib.error
from collections.abc import Callable
from dataclasses import dataclass, field

from issueflow.sources.http import HttpError


def is_retryable(exc: Exception) -> bool:
    """5xx, network failures and timeouts are transient; other 4xx never are."""
    if isinstance(exc, HttpError):
        return exc.status >= 500
    return isinstance(exc, (urllib.error.URLError, TimeoutError))


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 5  # total calls, including the first
    base_delay: float = 1
    max_delay: float = 30
    sleep: Callable[[float], None] = field(default=time.sleep, repr=False)
    random: Callable[[], float] = field(default=random.random, repr=False)

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")

    def delay_for(self, attempt: int) -> float:
        """Full jitter: uniform in [0, min(max_delay, base_delay * 2**(attempt-1))]."""
        ceiling = min(self.max_delay, self.base_delay * 2 ** (attempt - 1))
        return self.random() * ceiling

    def call[T](self, fn: Callable[[], T]) -> T:
        for attempt in range(1, self.max_attempts + 1):
            try:
                return fn()
            except Exception as exc:
                if not is_retryable(exc) or attempt == self.max_attempts:
                    raise
                self.sleep(self.delay_for(attempt))
        raise AssertionError("unreachable")
