import logging
import uuid
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from issueflow.messaging.messages import IssueMessage, Message, RunEnd, RunStart
from issueflow.models import EnrichedIssue, Issue
from issueflow.sinks.base import Sink

log = logging.getLogger(__name__)


class PipelineError(Exception):
    """A run could not be completed."""


class IncompleteRunError(PipelineError):
    """The messages stopped before the run's END marker arrived."""


class RunCountMismatch(PipelineError):
    """The END marker's count differs from the issues received, so some were lost."""


class Publisher(Protocol):
    def publish(self, message: Message) -> None: ...

    def flush(self) -> None: ...


class Reader(Protocol):
    def __iter__(self) -> Iterator[Message]: ...

    def commit(self) -> None: ...


@dataclass(frozen=True)
class RunSummary:
    run_id: str
    count: int


@dataclass(frozen=True)
class ConsumeResult:
    run_id: str
    as_of: datetime
    count: int
    sink: Sink  # already closed; kept so callers can find the files it wrote


def produce(
    issues: Iterable[Issue],
    publisher: Publisher,
    *,
    as_of: datetime,
    repo: str,
    run_id: str | None = None,
) -> RunSummary:
    """Publish START, one message per issue, then END with the count; wait for the acks.

    If `issues` raises, END is never sent, so a consumer cannot mistake a partial run for a
    complete one.
    """
    run_id = run_id or uuid.uuid4().hex
    publisher.publish(RunStart(run_id, as_of, repo))
    count = 0
    for issue in issues:
        count += 1
        publisher.publish(IssueMessage(run_id, count, issue))
    publisher.publish(RunEnd(run_id, count))
    publisher.flush()
    return RunSummary(run_id, count)


def consume(
    reader: Reader,
    make_sink: Callable[[RunStart], Sink],
    transform: Callable[[Issue, datetime], EnrichedIssue],
    *,
    run_id: str | None = None,
) -> ConsumeResult:
    """Read one run, write it to a sink, and commit the reader's position.

    Reads the first run found, or only the run named by `run_id`. Messages before a START and
    messages of other runs are ignored. A new START while a run is open abandons the unfinished
    one. The reader is committed only after the sink is closed, so a crash means the run is read
    again rather than skipped.
    """
    run: RunStart | None = None
    sink: Sink | None = None
    count = 0
    last_seq = 0
    try:
        for message in reader:
            if isinstance(message, RunStart):
                if run_id is not None and message.run_id != run_id:
                    continue  # not the run we were asked for
                if run is not None and message.run_id == run.run_id:
                    continue  # duplicate START from a producer retry
                if sink is not None:
                    log.warning("run %s never ended; abandoning it for run %s", run.run_id, message.run_id)  # type: ignore[union-attr]
                    sink.close()
                run, sink, count, last_seq = message, make_sink(message), 0, 0
            elif run is None or sink is None or message.run_id != run.run_id:
                continue
            elif isinstance(message, IssueMessage):
                if message.seq <= last_seq:
                    continue  # duplicate from a producer retry
                sink.write(transform(message.issue, run.as_of))
                count += 1
                last_seq = message.seq
            elif isinstance(message, RunEnd):
                if message.count != count:
                    raise RunCountMismatch(
                        f"run {run.run_id}: the producer sent {message.count} issues but {count} arrived"
                    )
                sink.close()
                reader.commit()
                return ConsumeResult(run.run_id, run.as_of, count, sink)
        if run is None:
            raise IncompleteRunError("no run found: the topic had no START message before the reader timed out")
        raise IncompleteRunError(
            f"run {run.run_id} has no END message after {count} issues; the producer may have failed"
        )
    finally:
        if sink is not None:
            sink.close()
