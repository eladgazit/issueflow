import threading
import time
from collections.abc import Iterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from issueflow.models import EnrichedIssue, Issue
from issueflow.pipeline import run_pipeline
from issueflow.transforms.derived import enrich

AS_OF = datetime(2025, 3, 1, tzinfo=UTC)
BASE = Issue(
    number=0,
    id=0,
    title="t",
    state="open",
    state_reason=None,
    author="a",
    created_at=AS_OF - timedelta(days=5),
    updated_at=AS_OF - timedelta(days=1),
    closed_at=None,
    comments=0,
    milestone=None,
    html_url="http://x",
)


def issue(n: int) -> Issue:
    return replace(BASE, number=n, id=n)


def transform(item: Issue) -> EnrichedIssue:
    return enrich(item, AS_OF)


class ListSink:
    def __init__(self) -> None:
        self.items: list[EnrichedIssue] = []

    def write(self, item: EnrichedIssue) -> None:
        self.items.append(item)

    def close(self) -> None: ...

    def __enter__(self) -> "ListSink":
        return self

    def __exit__(self, *exc: object) -> None: ...


def test_writes_every_issue_in_order_and_returns_count() -> None:
    sink = ListSink()
    count = run_pipeline((issue(n) for n in range(1, 51)), transform, sink, queue_size=4)
    assert count == 50
    assert [e.issue.number for e in sink.items] == list(range(1, 51))


def test_transform_is_applied() -> None:
    sink = ListSink()
    run_pipeline([issue(1)], transform, sink)
    assert sink.items[0].open_age_days == 5
    assert sink.items[0].as_of == AS_OF


def test_empty_source_writes_nothing() -> None:
    sink = ListSink()
    assert run_pipeline([], transform, sink) == 0
    assert sink.items == []


def test_producer_error_is_reraised_after_earlier_items_are_written() -> None:
    def failing() -> Iterator[Issue]:
        yield issue(1)
        yield issue(2)
        raise RuntimeError("fetch failed")

    sink = ListSink()
    with pytest.raises(RuntimeError, match="fetch failed"):
        run_pipeline(failing(), transform, sink)
    assert [e.issue.number for e in sink.items] == [1, 2]


def test_producer_error_on_first_item_is_reraised() -> None:
    def failing() -> Iterator[Issue]:
        raise ValueError("boom")
        yield  # pragma: no cover

    with pytest.raises(ValueError, match="boom"):
        run_pipeline(failing(), transform, ListSink())


def test_sink_error_stops_an_endless_producer() -> None:
    closed = threading.Event()

    def endless() -> Iterator[Issue]:
        try:
            n = 0
            while True:
                n += 1
                yield issue(n)
        finally:
            closed.set()

    class FailingSink(ListSink):
        def write(self, item: EnrichedIssue) -> None:
            if len(self.items) == 2:
                raise OSError("disk full")
            super().write(item)

    with pytest.raises(OSError, match="disk full"):
        run_pipeline(endless(), transform, FailingSink(), queue_size=2)
    assert closed.wait(timeout=2)


def test_transform_error_propagates_and_stops_producer() -> None:
    closed = threading.Event()

    def endless() -> Iterator[Issue]:
        try:
            n = 0
            while True:
                n += 1
                yield issue(n)
        finally:
            closed.set()

    def bad_transform(item: Issue) -> EnrichedIssue:
        raise KeyError("nope")

    with pytest.raises(KeyError):
        run_pipeline(endless(), bad_transform, ListSink(), queue_size=2)
    assert closed.wait(timeout=2)


def test_queue_is_bounded_so_producer_cannot_run_ahead() -> None:
    produced = 0
    release = threading.Event()

    def source() -> Iterator[Issue]:
        nonlocal produced
        for n in range(100):
            produced += 1
            yield issue(n)

    class BlockingSink(ListSink):
        def write(self, item: EnrichedIssue) -> None:
            release.wait(timeout=5)
            super().write(item)

    queue_size = 3
    result: list[int] = []
    thread = threading.Thread(target=lambda: result.append(run_pipeline(source(), transform, BlockingSink(), queue_size=queue_size)))
    thread.start()
    deadline = time.monotonic() + 2
    while produced < queue_size + 1 and time.monotonic() < deadline:
        time.sleep(0.01)
    time.sleep(0.2)  # give an unbounded producer time to race ahead
    # 1 item held by the blocked sink + queue_size queued + 1 waiting to be queued
    assert produced <= queue_size + 2
    release.set()
    thread.join(timeout=5)
    assert result == [100]


def test_invalid_queue_size_is_rejected() -> None:
    with pytest.raises(ValueError):
        run_pipeline([], transform, ListSink(), queue_size=0)
