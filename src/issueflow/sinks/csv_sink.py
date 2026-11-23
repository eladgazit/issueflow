import csv
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import IO, Any, Self

from issueflow.models import EnrichedIssue

ISSUES_TABLE = "issues"
LABELS_TABLE = "issue_labels"
ASSIGNEES_TABLE = "issue_assignees"

ISSUE_COLUMNS = (
    "number",
    "id",
    "title",
    "state",
    "state_reason",
    "author",
    "created_at",
    "updated_at",
    "closed_at",
    "comments",
    "milestone",
    "html_url",
    "open_age_days",
    "days_since_update",
    "is_unassigned",
    "is_stale",
    "as_of",
)
LABEL_COLUMNS = ("issue_number", "label")
ASSIGNEE_COLUMNS = ("issue_number", "assignee")


def _ts(value: datetime | None) -> str:
    """ISO 8601 UTC with second precision, e.g. 2025-01-31T12:00:00Z; empty for null."""
    return "" if value is None else value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _bool(value: bool) -> str:
    return "true" if value else "false"


def _opt(value: object) -> str:
    return "" if value is None else str(value)


def issue_row(item: EnrichedIssue) -> list[str]:
    """One issues.csv row, in ISSUE_COLUMNS order."""
    issue = item.issue
    return [
        str(issue.number),
        str(issue.id),
        issue.title,
        issue.state,
        _opt(issue.state_reason),
        _opt(issue.author),
        _ts(issue.created_at),
        _ts(issue.updated_at),
        _ts(issue.closed_at),
        str(issue.comments),
        _opt(issue.milestone),
        issue.html_url,
        _opt(item.open_age_days),
        str(item.days_since_update),
        _bool(item.is_unassigned),
        _bool(item.is_stale),
        _ts(item.as_of),
    ]


def partition_dir(root: Path | str, as_of: datetime) -> Path:
    """The daily partition folder for a run: <root>/as_of=YYYY-MM-DD (the as_of date in UTC)."""
    return Path(root) / f"as_of={as_of.astimezone(UTC):%Y-%m-%d}"


def file_stamp(created: datetime) -> str:
    """Creation timestamp used in file names, e.g. 20261123T090233Z."""
    return f"{created.astimezone(UTC):%Y%m%dT%H%M%SZ}"


class CsvSink:
    """Streams enriched issues into three normalized CSV files, one row at a time.

    Files go in a daily partition folder named after `as_of` (<out_dir>/as_of=YYYY-MM-DD/) and are
    named <table>_<creation timestamp>.csv, for example issues_20261123T090233Z.csv. All files of
    one run share the same timestamp. Files are never overwritten: a second run on the same day
    adds new files beside the earlier ones. Rows are written as they arrive, so memory use does
    not depend on the number of issues.
    """

    def __init__(
        self,
        out_dir: Path | str,
        as_of: datetime,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.directory = partition_dir(out_dir, as_of)
        self.stamp = file_stamp(clock())
        self.directory.mkdir(parents=True, exist_ok=True)
        self.issues_path = self.path_for(ISSUES_TABLE)
        self.labels_path = self.path_for(LABELS_TABLE)
        self.assignees_path = self.path_for(ASSIGNEES_TABLE)
        self._files: list[IO[str]] = []
        try:
            self._issues = self._open(self.issues_path, ISSUE_COLUMNS)
            self._labels = self._open(self.labels_path, LABEL_COLUMNS)
            self._assignees = self._open(self.assignees_path, ASSIGNEE_COLUMNS)
        except BaseException:
            self.close()
            raise

    def path_for(self, table: str) -> Path:
        """Path of a table's file for this run, e.g. path_for("by_label") for the aggregate."""
        return self.directory / f"{table}_{self.stamp}.csv"

    def _open(self, path: Path, columns: tuple[str, ...]) -> Any:
        file = path.open("x", encoding="utf-8", newline="")  # "x": never overwrite an existing file
        self._files.append(file)
        writer = csv.writer(file)
        writer.writerow(columns)
        return writer

    def write(self, item: EnrichedIssue) -> None:
        number = str(item.issue.number)
        self._issues.writerow(issue_row(item))
        for label in item.issue.labels:
            self._labels.writerow([number, label])
        for assignee in item.issue.assignees:
            self._assignees.writerow([number, assignee])

    def close(self) -> None:
        for file in self._files:
            file.close()  # idempotent; flushes buffered rows

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
