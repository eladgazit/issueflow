from collections.abc import Iterator
from typing import Any, Protocol


class Source(Protocol):
    """A lazy, single-pass producer of raw records."""

    def iter_records(self) -> Iterator[dict[str, Any]]: ...
