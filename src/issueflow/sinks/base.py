from typing import Protocol, Self

from issueflow.models import EnrichedIssue


class Sink(Protocol):
    """Receives enriched issues one at a time; usable as a context manager that closes on exit."""

    def write(self, item: EnrichedIssue) -> None: ...

    def close(self) -> None: ...

    def __enter__(self) -> Self: ...

    def __exit__(self, *exc: object) -> None: ...
