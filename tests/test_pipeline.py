import logging
from collections.abc import Iterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from issueflow.messaging.messages import IssueMessage, Message, RunEnd, RunStart, decode
from issueflow.models import EnrichedIssue, Issue
from issueflow.pipeline import IncompleteRunError, RunCountMismatch, consume, produce
from issueflow.transforms.derived import enrich

from conftest import FakeBroker

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


class ListSink:
    def __init__(self, start: RunStart) -> None:
        self.start = start
        self.items: list[EnrichedIssue] = []
        self.closed = False

    def write(self, item: EnrichedIssue) -> None:
        self.items.append(item)

    def close(self) -> None:
        self.closed = True

    def __enter__(self) -> "ListSink":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class Sinks:
    """make_sink that remembers every sink it created."""

    def __init__(self) -> None:
        self.created: list[ListSink] = []

    def __call__(self, start: RunStart) -> ListSink:
        sink = ListSink(start)
        self.created.append(sink)
        return sink


class ListReader:
    """A reader over a fixed list of messages that records commits."""

    def __init__(self, *messages: Message) -> None:
        self.messages = messages
        self.commits = 0

    def __iter__(self) -> Iterator[Message]:
        return iter(self.messages)

    def commit(self) -> None:
        self.commits += 1


def run_messages(run_id: str, numbers: list[int], *, count: int | None = None) -> list[Message]:
    return [
        RunStart(run_id, AS_OF, "o/r"),
        *[IssueMessage(run_id, i, issue(n)) for i, n in enumerate(numbers, 1)],
        RunEnd(run_id, len(numbers) if count is None else count),
    ]


# ---------------------------------------------------------------- produce


def test_produce_publishes_start_issues_end_and_flushes(broker: FakeBroker) -> None:
    summary = produce((issue(n) for n in (1, 2, 3)), broker.publisher(), as_of=AS_OF, repo="o/r", run_id="r1")
    messages = [decode(r) for r in broker.records]
    assert messages[0] == RunStart("r1", AS_OF, "o/r")
    assert [m.issue.number for m in messages[1:-1] if isinstance(m, IssueMessage)] == [1, 2, 3]
    assert [m.seq for m in messages[1:-1] if isinstance(m, IssueMessage)] == [1, 2, 3]
    assert messages[-1] == RunEnd("r1", 3)
    assert (summary.run_id, summary.count) == ("r1", 3)
    assert broker.flushes == 1


def test_produce_generates_a_run_id_when_none_is_given(broker: FakeBroker) -> None:
    first = produce([], broker.publisher(), as_of=AS_OF, repo="o/r")
    second = produce([], broker.publisher(), as_of=AS_OF, repo="o/r")
    assert first.run_id and first.run_id != second.run_id


def test_produce_empty_source_sends_start_and_end_with_zero(broker: FakeBroker) -> None:
    produce([], broker.publisher(), as_of=AS_OF, repo="o/r", run_id="r1")
    assert [decode(r) for r in broker.records] == [RunStart("r1", AS_OF, "o/r"), RunEnd("r1", 0)]


def test_produce_never_sends_end_if_fetching_fails(broker: FakeBroker) -> None:
    def failing() -> Iterator[Issue]:
        yield issue(1)
        raise RuntimeError("fetch failed")

    with pytest.raises(RuntimeError, match="fetch failed"):
        produce(failing(), broker.publisher(), as_of=AS_OF, repo="o/r", run_id="r1")
    kinds = [type(decode(r)).__name__ for r in broker.records]
    assert kinds == ["RunStart", "IssueMessage"]


def test_produce_surfaces_a_failed_send(broker: FakeBroker) -> None:
    broker.send_error = OSError("broker down")
    with pytest.raises(OSError, match="broker down"):
        produce([issue(1)], broker.publisher(), as_of=AS_OF, repo="o/r")


# ---------------------------------------------------------------- consume


def test_consume_writes_the_run_in_order_and_commits() -> None:
    sinks, reader = Sinks(), ListReader(*run_messages("r1", [1, 2, 3]))
    result = consume(reader, sinks, enrich)
    sink = sinks.created[0]
    assert [e.issue.number for e in sink.items] == [1, 2, 3]
    assert (result.run_id, result.count, result.as_of) == ("r1", 3, AS_OF)
    assert result.sink is sink and sink.closed
    assert reader.commits == 1


def test_consume_applies_the_transform_with_the_run_as_of() -> None:
    sinks = Sinks()
    consume(ListReader(*run_messages("r1", [1])), sinks, enrich)
    assert sinks.created[0].items[0].as_of == AS_OF
    assert sinks.created[0].items[0].open_age_days == 5


def test_consume_gets_the_start_message_when_making_the_sink() -> None:
    sinks = Sinks()
    consume(ListReader(*run_messages("r1", [])), sinks, enrich)
    assert sinks.created[0].start == RunStart("r1", AS_OF, "o/r")


def test_consume_empty_run_is_fine() -> None:
    result = consume(ListReader(*run_messages("r1", [])), Sinks(), enrich)
    assert result.count == 0


def test_consume_stops_at_the_end_and_leaves_later_runs_unread() -> None:
    reader = ListReader(*run_messages("r1", [1]), *run_messages("r2", [5, 6]))
    sinks = Sinks()
    assert consume(reader, sinks, enrich).run_id == "r1"
    assert len(sinks.created) == 1


def test_consume_ignores_messages_before_a_start() -> None:
    reader = ListReader(IssueMessage("old", 1, issue(99)), RunEnd("old", 1), *run_messages("r1", [1]))
    sinks = Sinks()
    result = consume(reader, sinks, enrich)
    assert result.run_id == "r1" and result.count == 1
    assert [e.issue.number for e in sinks.created[0].items] == [1]


def test_consume_ignores_messages_of_other_runs_inside_a_run() -> None:
    reader = ListReader(
        RunStart("r1", AS_OF, "o/r"),
        IssueMessage("other", 1, issue(50)),
        IssueMessage("r1", 1, issue(1)),
        RunEnd("other", 1),
        RunEnd("r1", 1),
    )
    sinks = Sinks()
    assert consume(reader, sinks, enrich).count == 1
    assert [e.issue.number for e in sinks.created[0].items] == [1]


def test_consume_run_id_filter_skips_other_runs() -> None:
    reader = ListReader(*run_messages("r1", [1]), *run_messages("r2", [2, 3]))
    sinks = Sinks()
    result = consume(reader, sinks, enrich, run_id="r2")
    assert result.run_id == "r2" and result.count == 2
    assert len(sinks.created) == 1
    assert reader.commits == 1


def test_consume_skips_duplicate_issues_from_producer_retries() -> None:
    reader = ListReader(
        RunStart("r1", AS_OF, "o/r"),
        IssueMessage("r1", 1, issue(1)),
        IssueMessage("r1", 2, issue(2)),
        IssueMessage("r1", 2, issue(2)),  # retried send, same seq
        IssueMessage("r1", 3, issue(3)),
        RunEnd("r1", 3),
    )
    sinks = Sinks()
    assert consume(reader, sinks, enrich).count == 3
    assert [e.issue.number for e in sinks.created[0].items] == [1, 2, 3]


def test_issue_numbers_need_not_increase(broker: FakeBroker) -> None:
    # Real GitHub data: issue 64 was created after issue 65, so numbers are not ordered.
    produce((issue(n) for n in (63, 65, 64, 66)), broker.publisher(), as_of=AS_OF, repo="o/r", run_id="r1")
    sinks = Sinks()
    assert consume(broker.reader(), sinks, enrich).count == 4
    assert [e.issue.number for e in sinks.created[0].items] == [63, 65, 64, 66]


def test_consume_ignores_a_duplicate_start_of_the_same_run() -> None:
    start = RunStart("r1", AS_OF, "o/r")
    reader = ListReader(start, IssueMessage("r1", 1, issue(1)), start, IssueMessage("r1", 2, issue(2)), RunEnd("r1", 2))
    sinks = Sinks()
    assert consume(reader, sinks, enrich).count == 2
    assert len(sinks.created) == 1


def test_count_mismatch_raises_and_does_not_commit() -> None:
    reader = ListReader(*run_messages("r1", [1, 2], count=3))
    sinks = Sinks()
    with pytest.raises(RunCountMismatch, match="3 issues but 2"):
        consume(reader, sinks, enrich)
    assert reader.commits == 0
    assert sinks.created[0].closed


def test_missing_end_raises_incomplete_run_and_does_not_commit() -> None:
    reader = ListReader(*run_messages("r1", [1, 2])[:-1])
    sinks = Sinks()
    with pytest.raises(IncompleteRunError, match="no END message after 2 issues"):
        consume(reader, sinks, enrich)
    assert reader.commits == 0
    assert sinks.created[0].closed


def test_no_start_at_all_raises_incomplete_run() -> None:
    with pytest.raises(IncompleteRunError, match="no run found"):
        consume(ListReader(), Sinks(), enrich)


def test_an_unfinished_run_is_abandoned_when_a_new_one_starts(caplog: pytest.LogCaptureFixture) -> None:
    reader = ListReader(*run_messages("dead", [1, 2])[:-1], *run_messages("r2", [7]))
    sinks = Sinks()
    with caplog.at_level(logging.WARNING):
        result = consume(reader, sinks, enrich)
    assert result.run_id == "r2" and result.count == 1
    assert len(sinks.created) == 2 and sinks.created[0].closed
    assert "abandoning" in caplog.text


def test_a_sink_error_propagates_without_commit() -> None:
    class BrokenSink(ListSink):
        def write(self, item: EnrichedIssue) -> None:
            raise OSError("disk full")

    reader = ListReader(*run_messages("r1", [1]))
    with pytest.raises(OSError, match="disk full"):
        consume(reader, lambda start: BrokenSink(start), enrich)
    assert reader.commits == 0


def test_a_transform_error_propagates_and_closes_the_sink() -> None:
    sinks, reader = Sinks(), ListReader(*run_messages("r1", [1]))

    def bad(item: Issue, as_of: datetime) -> EnrichedIssue:
        raise KeyError("nope")

    with pytest.raises(KeyError):
        consume(reader, sinks, bad)
    assert sinks.created[0].closed and reader.commits == 0


def test_produce_then_consume_through_the_fake_broker(broker: FakeBroker) -> None:
    produce((issue(n) for n in range(1, 8)), broker.publisher(), as_of=AS_OF, repo="o/r", run_id="r1")
    sinks = Sinks()
    reader = broker.reader()
    result = consume(reader, sinks, enrich)
    assert result.count == 7
    assert [e.issue.number for e in sinks.created[0].items] == list(range(1, 8))
    assert broker.committed == len(broker.records)
