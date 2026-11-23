import argparse
import sys
import urllib.error
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

from kafka.errors import KafkaError

from issueflow.aggregate import BY_LABEL_TABLE, aggregate_by_label
from issueflow.config import Settings
from issueflow.messaging.kafka import (
    DEFAULT_BOOTSTRAP,
    DEFAULT_GROUP,
    DEFAULT_IDLE_TIMEOUT,
    DEFAULT_TOPIC,
    KafkaPublisher,
    KafkaReader,
    KafkaSettings,
    KafkaUnavailableError,
)
from issueflow.messaging.messages import MessageError
from issueflow.pipeline import ConsumeResult, PipelineError, Publisher, Reader, RunSummary, consume, produce
from issueflow.sinks.csv_sink import CsvSink
from issueflow.sources.github import GitHubIssuesSource
from issueflow.sources.http import HttpClient, HttpError
from issueflow.sources.rate_limit import RateLimiter, RateLimitExceeded
from issueflow.transforms.derived import enrich


def parse_as_of(text: str) -> datetime:
    """Parse an ISO 8601 timestamp; a value without a timezone is taken as UTC."""
    try:
        value = datetime.fromisoformat(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"not an ISO 8601 timestamp: {text!r}") from None
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)


def _kafka_options() -> argparse.ArgumentParser:
    parent = argparse.ArgumentParser(add_help=False)
    parent.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP, help="Kafka address (default: %(default)s)")
    parent.add_argument("--topic", default=DEFAULT_TOPIC, help="Kafka topic (default: %(default)s)")
    return parent


def _produce_options() -> argparse.ArgumentParser:
    parent = argparse.ArgumentParser(add_help=False)
    parent.add_argument("--repo", default="microsoft/vscode", help="owner/name (default: %(default)s)")
    parent.add_argument("--limit", type=int, help="stop after this many issues (default: all)")
    parent.add_argument("--as-of", type=parse_as_of, help="reference time, ISO 8601 (default: now, UTC)")
    parent.add_argument(
        "--max-wait",
        type=float,
        default=60.0,
        help="longest rate-limit reset to wait through, in seconds (default: %(default)s)",
    )
    return parent


def _consume_options() -> argparse.ArgumentParser:
    parent = argparse.ArgumentParser(add_help=False)
    parent.add_argument("--out", type=Path, default=Path("out"), help="output root folder (default: %(default)s)")
    parent.add_argument("--group", default=DEFAULT_GROUP, help="Kafka consumer group (default: %(default)s)")
    parent.add_argument(
        "--idle-timeout",
        type=float,
        default=DEFAULT_IDLE_TIMEOUT,
        help="give up after this many seconds without a message (default: %(default)s)",
    )
    return parent


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="issueflow",
        description="Fetch GitHub issues into Kafka, then turn them into CSV files with derived fields.",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    kafka, fetch, write = _kafka_options(), _produce_options(), _consume_options()
    commands.add_parser("produce", parents=[kafka, fetch], help="fetch issues from GitHub and publish them to Kafka")
    commands.add_parser("consume", parents=[kafka, write], help="read a run from Kafka and write the CSV files")
    commands.add_parser("run", parents=[kafka, fetch, write], help="produce, then consume that run")
    return parser


def _produce(settings: Settings, kafka: KafkaSettings, client: HttpClient | None, publisher: Publisher | None) -> RunSummary:
    source = GitHubIssuesSource(
        settings.owner,
        settings.name,
        limit=settings.limit,
        client=client,
        limiter=RateLimiter(max_wait=settings.rate_limit_max_wait),
    )
    owned = publisher is None
    publisher = publisher or KafkaPublisher(kafka)
    try:
        summary = produce(source.iter_issues(), publisher, as_of=settings.as_of, repo=settings.repo)
    finally:
        if owned:
            publisher.close()  # type: ignore[attr-defined]
    print(f"published {summary.count} issues to topic {kafka.topic} (run {summary.run_id})")
    return summary


def _consume(out: Path, kafka: KafkaSettings, reader: Reader | None, run_id: str | None) -> ConsumeResult:
    owned = reader is None
    reader = reader or KafkaReader(kafka)
    try:
        result = consume(reader, lambda start: CsvSink(out, start.as_of), enrich, run_id=run_id)
    finally:
        if owned:
            reader.close()  # type: ignore[attr-defined]
    sink = result.sink
    assert isinstance(sink, CsvSink)
    labels = aggregate_by_label(sink.issues_path, sink.labels_path, sink.path_for(BY_LABEL_TABLE))
    print(f"wrote {result.count} issues and {labels} labels to {sink.directory}/ (files stamped {sink.stamp})")
    return result


def main(
    argv: Sequence[str] | None = None,
    *,
    client: HttpClient | None = None,
    publisher: Publisher | None = None,
    reader: Reader | None = None,
) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    try:  # validate every option before doing any work
        kafka = KafkaSettings(
            args.bootstrap_servers,
            args.topic,
            getattr(args, "group", DEFAULT_GROUP),
            getattr(args, "idle_timeout", DEFAULT_IDLE_TIMEOUT),
        )
        settings = (
            Settings(
                repo=args.repo,
                limit=args.limit,
                rate_limit_max_wait=args.max_wait,
                **({"as_of": args.as_of} if args.as_of else {}),
            )
            if args.command in ("produce", "run")
            else None
        )
    except ValueError as exc:
        parser.error(str(exc))
    try:
        if args.command == "produce":
            _produce(settings, kafka, client, publisher)  # type: ignore[arg-type]
        elif args.command == "consume":
            _consume(args.out, kafka, reader, None)
        else:
            summary = _produce(settings, kafka, client, publisher)  # type: ignore[arg-type]
            _consume(args.out, kafka, reader, summary.run_id)
    except MessageError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except KafkaUnavailableError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except RateLimitExceeded as exc:
        print(f"error: {exc} Nothing was ended in Kafka for this run.", file=sys.stderr)
        return 1
    except (HttpError, urllib.error.URLError, TimeoutError) as exc:
        print(f"error: fetching issues failed: {exc}. Nothing was ended in Kafka for this run.", file=sys.stderr)
        return 1
    except PipelineError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except KafkaError as exc:
        print(f"error: Kafka failed: {exc!r}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return 130
    return 0
