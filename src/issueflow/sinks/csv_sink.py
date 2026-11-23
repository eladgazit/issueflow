import csv
from datetime import UTC, datetime
from pathlib import Path
from typing import IO, Any, Self

from issueflow.models import EnrichedIssue

ISSUES_FILE = "issues.csv"
LABELS_FILE = "issue_labels.csv"
ASSIGNEES_FILE = "issue_assignees.csv"

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


class CsvSink:
    """Streams enriched issues into three normalized CSV files, one row at a time.

    Existing files in `out_dir` are overwritten. Rows are written as they arrive, so memory
    use does not depend on the number of issues.
    """

    def __init__(self, out_dir: Path | str) -> None:
        directory = Path(out_dir)
        directory.mkdir(parents=True, exist_ok=True)
        self._files: list[IO[str]] = []
        try:
            self._issues = self._open(directory / ISSUES_FILE, ISSUE_COLUMNS)
            self._labels = self._open(directory / LABELS_FILE, LABEL_COLUMNS)
            self._assignees = self._open(directory / ASSIGNEES_FILE, ASSIGNEE_COLUMNS)
        except BaseException:
            self.close()
            raise

    def _open(self, path: Path, columns: tuple[str, ...]) -> Any:
        file = path.open("w", encoding="utf-8", newline="")
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
