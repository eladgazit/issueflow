import queue
import threading
from collections.abc import Callable, Iterable

from issueflow.models import EnrichedIssue, Issue
from issueflow.sinks.base import Sink

DEFAULT_QUEUE_SIZE = 500  # about five GitHub pages of issues
_POLL_SECONDS = 0.1  # how often a blocked producer re-checks the stop flag
_JOIN_SECONDS = 5.0
_DONE = object()


class _Failure:
    """Carries an exception from the producer thread to the main thread."""

    def __init__(self, exc: BaseException) -> None:
        self.exc = exc


def run_pipeline(
    issues: Iterable[Issue],
    transform: Callable[[Issue], EnrichedIssue],
    sink: Sink,
    *,
    queue_size: int = DEFAULT_QUEUE_SIZE,
) -> int:
    """Move issues from `issues` through `transform` into `sink`; return how many were written.

    A producer thread pulls from `issues` (the slow, network-bound side) into a bounded queue,
    while this thread transforms and writes. The bound caps memory and stops the producer from
    running far ahead of the writer. An error in the producer is re-raised here, and an error
    here stops the producer. The caller owns `sink` and is responsible for closing it.
    """
    if queue_size < 1:
        raise ValueError("queue_size must be >= 1")
    buffer: queue.Queue[object] = queue.Queue(maxsize=queue_size)
    stop = threading.Event()

    def put(item: object) -> bool:
        """Block until there is room; give up (False) once the consumer has stopped."""
        while not stop.is_set():
            try:
                buffer.put(item, timeout=_POLL_SECONDS)
                return True
            except queue.Full:
                continue
        return False

    def produce() -> None:
        iterator = iter(issues)
        try:
            for issue in iterator:
                if not put(issue):
                    return
            put(_DONE)
        except BaseException as exc:  # noqa: BLE001 - forwarded to the consumer, not swallowed
            put(_Failure(exc))
        finally:
            close = getattr(iterator, "close", None)
            if close is not None:
                close()

    producer = threading.Thread(target=produce, name="issueflow-fetch", daemon=True)
    producer.start()
    written = 0
    try:
        while True:
            item = buffer.get()
            if item is _DONE:
                break
            if isinstance(item, _Failure):
                raise item.exc
            sink.write(transform(item))  # type: ignore[arg-type]
            written += 1
    finally:
        stop.set()
        producer.join(timeout=_JOIN_SECONDS)
    return written
