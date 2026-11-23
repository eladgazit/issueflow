import csv
from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest

from issueflow.models import EnrichedIssue, Issue
from issueflow.sinks.csv_sink import (
    ASSIGNEE_COLUMNS,
    ISSUE_COLUMNS,
    LABEL_COLUMNS,
    CsvSink,
    issue_row,
)
from issueflow.transforms.derived import enrich

AS_OF = datetime(2025, 3, 1, 12, 0, tzinfo=UTC)


def make_issue(**overrides: object) -> Issue:
    base = Issue(
        number=7,
        id=70,
        title="Crash on save",
        state="open",
        state_reason=None,
        author="alice",
        created_at=datetime(2025, 2, 1, 8, 30, tzinfo=UTC),
        updated_at=datetime(2025, 2, 20, 9, 0, tzinfo=UTC),
        closed_at=None,
        comments=4,
        milestone="Backlog",
        html_url="https://github.com/microsoft/vscode/issues/7",
        labels=("bug", "ui"),
        assignees=("bob",),
    )
    return replace(base, **overrides)


def read(path: Path) -> list[list[str]]:
    with path.open(encoding="utf-8", newline="") as f:
        return list(csv.reader(f))


def write_all(out: Path, *issues: Issue) -> None:
    with CsvSink(out) as sink:
        for issue in issues:
            sink.write(enrich(issue, AS_OF))


def test_headers_are_written_even_with_no_rows(tmp_path: Path) -> None:
    with CsvSink(tmp_path):
        pass
    assert read(tmp_path / "issues.csv") == [list(ISSUE_COLUMNS)]
    assert read(tmp_path / "issue_labels.csv") == [list(LABEL_COLUMNS)]
    assert read(tmp_path / "issue_assignees.csv") == [list(ASSIGNEE_COLUMNS)]


def test_issue_row_values(tmp_path: Path) -> None:
    write_all(tmp_path, make_issue())
    header, row = read(tmp_path / "issues.csv")
    assert dict(zip(header, row, strict=True)) == {
        "number": "7",
        "id": "70",
        "title": "Crash on save",
        "state": "open",
        "state_reason": "",
        "author": "alice",
        "created_at": "2025-02-01T08:30:00Z",
        "updated_at": "2025-02-20T09:00:00Z",
        "closed_at": "",
        "comments": "4",
        "milestone": "Backlog",
        "html_url": "https://github.com/microsoft/vscode/issues/7",
        "open_age_days": "28",
        "days_since_update": "9",
        "is_unassigned": "false",
        "is_stale": "false",
        "as_of": "2025-03-01T12:00:00Z",
    }


def test_row_length_matches_header() -> None:
    assert len(issue_row(enrich(make_issue(), AS_OF))) == len(ISSUE_COLUMNS)


def test_closed_issue_has_empty_open_age_and_closed_at_filled(tmp_path: Path) -> None:
    closed = make_issue(
        state="closed", state_reason="completed", closed_at=datetime(2025, 2, 10, tzinfo=UTC)
    )
    write_all(tmp_path, closed)
    header, row = read(tmp_path / "issues.csv")
    record = dict(zip(header, row, strict=True))
    assert record["open_age_days"] == ""
    assert record["closed_at"] == "2025-02-10T00:00:00Z"
    assert record["state_reason"] == "completed"


def test_nulls_become_empty_strings(tmp_path: Path) -> None:
    write_all(tmp_path, make_issue(author=None, milestone=None, assignees=()))
    header, row = read(tmp_path / "issues.csv")
    record = dict(zip(header, row, strict=True))
    assert record["author"] == ""
    assert record["milestone"] == ""
    assert record["is_unassigned"] == "true"


def test_stale_flag_is_written_as_true(tmp_path: Path) -> None:
    write_all(tmp_path, make_issue(updated_at=AS_OF - timedelta(days=45)))
    header, row = read(tmp_path / "issues.csv")
    assert dict(zip(header, row, strict=True))["is_stale"] == "true"


def test_labels_and_assignees_go_to_child_files(tmp_path: Path) -> None:
    write_all(tmp_path, make_issue(number=7, labels=("bug", "ui"), assignees=("bob", "amy")))
    assert read(tmp_path / "issue_labels.csv")[1:] == [["7", "bug"], ["7", "ui"]]
    assert read(tmp_path / "issue_assignees.csv")[1:] == [["7", "bob"], ["7", "amy"]]


def test_issue_without_labels_or_assignees_adds_no_child_rows(tmp_path: Path) -> None:
    write_all(tmp_path, make_issue(labels=(), assignees=()))
    assert len(read(tmp_path / "issues.csv")) == 2
    assert len(read(tmp_path / "issue_labels.csv")) == 1
    assert len(read(tmp_path / "issue_assignees.csv")) == 1


def test_many_issues_keep_order_and_child_links(tmp_path: Path) -> None:
    write_all(
        tmp_path,
        make_issue(number=1, labels=("a",)),
        make_issue(number=2, labels=("b", "c")),
        make_issue(number=3, labels=()),
    )
    assert [r[0] for r in read(tmp_path / "issues.csv")[1:]] == ["1", "2", "3"]
    assert read(tmp_path / "issue_labels.csv")[1:] == [["1", "a"], ["2", "b"], ["2", "c"]]


def test_special_characters_round_trip(tmp_path: Path) -> None:
    title = 'Bug, with "quotes"\nand a newline — ünïcödé 🐞'
    write_all(tmp_path, make_issue(title=title, labels=("needs, triage",)))
    assert read(tmp_path / "issues.csv")[1][2] == title
    assert read(tmp_path / "issue_labels.csv")[1] == ["7", "needs, triage"]


def test_timestamps_are_utc_second_precision(tmp_path: Path) -> None:
    plus_two = timezone(timedelta(hours=2))
    issue = make_issue(created_at=datetime(2025, 2, 1, 10, 30, 15, 999_999, tzinfo=plus_two))
    with CsvSink(tmp_path) as sink:
        sink.write(enrich(issue, datetime(2025, 3, 1, 12, 0, 0, 123_456, tzinfo=UTC)))
    header, row = read(tmp_path / "issues.csv")
    record = dict(zip(header, row, strict=True))
    assert record["created_at"] == "2025-02-01T08:30:15Z"
    assert record["as_of"] == "2025-03-01T12:00:00Z"


def test_creates_missing_output_directory(tmp_path: Path) -> None:
    out = tmp_path / "nested" / "out"
    with CsvSink(out):
        pass
    assert (out / "issues.csv").exists()


def test_existing_files_are_overwritten(tmp_path: Path) -> None:
    write_all(tmp_path, make_issue(number=1), make_issue(number=2))
    write_all(tmp_path, make_issue(number=3))
    assert [r[0] for r in read(tmp_path / "issues.csv")[1:]] == ["3"]


def test_context_manager_flushes_and_closes(tmp_path: Path) -> None:
    sink = CsvSink(tmp_path)
    with sink:
        sink.write(enrich(make_issue(), AS_OF))
    assert len(read(tmp_path / "issues.csv")) == 2
    with pytest.raises(ValueError):
        sink.write(enrich(make_issue(), AS_OF))


def test_close_is_idempotent(tmp_path: Path) -> None:
    sink = CsvSink(tmp_path)
    sink.close()
    sink.close()


def test_files_are_closed_when_the_block_raises(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError), CsvSink(tmp_path) as sink:
        sink.write(enrich(make_issue(), AS_OF))
        raise RuntimeError("boom")
    assert len(read(tmp_path / "issues.csv")) == 2


def test_enriched_issue_type_is_accepted() -> None:
    assert isinstance(enrich(make_issue(), AS_OF), EnrichedIssue)
