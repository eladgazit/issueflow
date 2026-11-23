import argparse
import sys
import urllib.error
from collections.abc import Sequence
from datetime import UTC, datetime
from functools import partial
from pathlib import Path

from issueflow.aggregate import BY_LABEL_TABLE, aggregate_by_label
from issueflow.config import Settings
from issueflow.pipeline import run_pipeline
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


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="issueflow",
        description="Fetch GitHub issues, add derived fields, and write CSV files.",
    )
    parser.add_argument("--repo", default="microsoft/vscode", help="owner/name (default: %(default)s)")
    parser.add_argument("--limit", type=int, help="stop after this many issues (default: all)")
    parser.add_argument("--as-of", type=parse_as_of, help="reference time, ISO 8601 (default: now, UTC)")
    parser.add_argument("--out", type=Path, default=Path("out"), help="output directory (default: %(default)s)")
    parser.add_argument(
        "--max-wait",
        type=float,
        default=60.0,
        help="longest rate-limit reset to wait through, in seconds (default: %(default)s)",
    )
    return parser


def main(argv: Sequence[str] | None = None, *, client: HttpClient | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    try:
        settings = Settings(
            repo=args.repo,
            limit=args.limit,
            out_dir=args.out,
            rate_limit_max_wait=args.max_wait,
            **({"as_of": args.as_of} if args.as_of else {}),
        )
    except ValueError as exc:
        parser.error(str(exc))

    source = GitHubIssuesSource(
        settings.owner,
        settings.name,
        limit=settings.limit,
        client=client,
        limiter=RateLimiter(max_wait=settings.rate_limit_max_wait),
    )
    try:
        with CsvSink(settings.out_dir, settings.as_of) as sink:
            written = run_pipeline(source.iter_issues(), partial(enrich, as_of=settings.as_of), sink)
        by_label_path = sink.path_for(BY_LABEL_TABLE)
        labels = aggregate_by_label(sink.issues_path, sink.labels_path, by_label_path)
    except RateLimitExceeded as exc:
        print(f"error: {exc} Output in {settings.out_dir} is incomplete.", file=sys.stderr)
        return 1
    except (HttpError, urllib.error.URLError, TimeoutError) as exc:
        print(f"error: fetching issues failed: {exc}. Output in {settings.out_dir} is incomplete.", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("interrupted; output is incomplete", file=sys.stderr)
        return 130

    print(f"wrote {written} issues and {labels} labels to {sink.directory}/ (files stamped {sink.stamp})")
    return 0
