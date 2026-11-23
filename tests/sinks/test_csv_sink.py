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
    file_stamp,
    issue_row,
    partition_dir,
)
from issueflow.transforms.derived import enrich

AS_OF = datetime(2025, 3, 1, 12, 0, tzinfo=UTC)
CREATED = datetime(2025, 3, 1, 12, 5, 9, tzinfo=UTC)
STAMP = "20250301T120509Z"


def new_sink(out: Path, as_of: datetime = AS_OF, created: datetime = CREATED) -> CsvSink:
    return CsvSink(out, as_of, clock=lambda: created)


def table(out: Path, name: str) -> Path:
    return out / "as_of=2025-03-01" / f"{name}_{STAMP}.csv"


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
    with new_sink(out) as sink:
        for issue in issues:
            sink.write(enrich(issue, AS_OF))


def test_headers_are_written_even_with_no_rows(tmp_path: Path) -> None:
    with new_sink(tmp_path):
        pass
    assert read(table(tmp_path, "issues")) == [list(ISSUE_COLUMNS)]
    assert read(table(tmp_path, "issue_labels")) == [list(LABEL_COLUMNS)]
    assert read(table(tmp_path, "issue_assignees")) == [list(ASSIGNEE_COLUMNS)]


def test_issue_row_values(tmp_path: Path) -> None:
    write_all(tmp_path, make_issue())
    header, row = read(table(tmp_path, "issues"))
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
    header, row = read(table(tmp_path, "issues"))
    record = dict(zip(header, row, strict=True))
    assert record["open_age_days"] == ""
    assert record["closed_at"] == "2025-02-10T00:00:00Z"
    assert record["state_reason"] == "completed"


def test_nulls_become_empty_strings(tmp_path: Path) -> None:
    write_all(tmp_path, make_issue(author=None, milestone=None, assignees=()))
    header, row = read(table(tmp_path, "issues"))
    record = dict(zip(header, row, strict=True))
    assert record["author"] == ""
    assert record["milestone"] == ""
    assert record["is_unassigned"] == "true"


def test_stale_flag_is_written_as_true(tmp_path: Path) -> None:
    write_all(tmp_path, make_issue(updated_at=AS_OF - timedelta(days=45)))
    header, row = read(table(tmp_path, "issues"))
    assert dict(zip(header, row, strict=True))["is_stale"] == "true"


def test_labels_and_assignees_go_to_child_files(tmp_path: Path) -> None:
    write_all(tmp_path, make_issue(number=7, labels=("bug", "ui"), assignees=("bob", "amy")))
    assert read(table(tmp_path, "issue_labels"))[1:] == [["7", "bug"], ["7", "ui"]]
    assert read(table(tmp_path, "issue_assignees"))[1:] == [["7", "bob"], ["7", "amy"]]


def test_issue_without_labels_or_assignees_adds_no_child_rows(tmp_path: Path) -> None:
    write_all(tmp_path, make_issue(labels=(), assignees=()))
    assert len(read(table(tmp_path, "issues"))) == 2
    assert len(read(table(tmp_path, "issue_labels"))) == 1
    assert len(read(table(tmp_path, "issue_assignees"))) == 1


def test_many_issues_keep_order_and_child_links(tmp_path: Path) -> None:
    write_all(
        tmp_path,
        make_issue(number=1, labels=("a",)),
        make_issue(number=2, labels=("b", "c")),
        make_issue(number=3, labels=()),
    )
    assert [r[0] for r in read(table(tmp_path, "issues"))[1:]] == ["1", "2", "3"]
    assert read(table(tmp_path, "issue_labels"))[1:] == [["1", "a"], ["2", "b"], ["2", "c"]]


def test_special_characters_round_trip(tmp_path: Path) -> None:
    title = 'Bug, with "quotes"\nand a newline — ünïcödé 🐞'
    write_all(tmp_path, make_issue(title=title, labels=("needs, triage",)))
    assert read(table(tmp_path, "issues"))[1][2] == title
    assert read(table(tmp_path, "issue_labels"))[1] == ["7", "needs, triage"]


def test_timestamps_are_utc_second_precision(tmp_path: Path) -> None:
    plus_two = timezone(timedelta(hours=2))
    issue = make_issue(created_at=datetime(2025, 2, 1, 10, 30, 15, 999_999, tzinfo=plus_two))
    with new_sink(tmp_path) as sink:
        sink.write(enrich(issue, datetime(2025, 3, 1, 12, 0, 0, 123_456, tzinfo=UTC)))
    header, row = read(table(tmp_path, "issues"))
    record = dict(zip(header, row, strict=True))
    assert record["created_at"] == "2025-02-01T08:30:15Z"
    assert record["as_of"] == "2025-03-01T12:00:00Z"


def test_files_are_placed_in_a_daily_as_of_partition_folder(tmp_path: Path) -> None:
    out = tmp_path / "nested" / "out"
    with new_sink(out) as sink:
        pass
    folder = out / "as_of=2025-03-01"
    assert sink.directory == folder
    assert sorted(f.name for f in folder.iterdir()) == [
        f"issue_assignees_{STAMP}.csv",
        f"issue_labels_{STAMP}.csv",
        f"issues_{STAMP}.csv",
    ]


def test_folder_follows_as_of_but_file_names_follow_creation_time(tmp_path: Path) -> None:
    past = datetime(2024, 12, 31, 23, 0, tzinfo=UTC)
    with new_sink(tmp_path, as_of=past) as sink:
        pass
    assert sink.directory == tmp_path / "as_of=2024-12-31"
    assert sink.issues_path.name == f"issues_{STAMP}.csv"


def test_as_of_with_offset_uses_the_utc_date(tmp_path: Path) -> None:
    late_local = datetime(2025, 3, 1, 23, 30, tzinfo=timezone(timedelta(hours=5)))  # 18:30 UTC, same day
    early_local = datetime(2025, 3, 2, 1, 0, tzinfo=timezone(timedelta(hours=5)))  # 20:00 UTC the day before
    assert partition_dir(tmp_path, late_local).name == "as_of=2025-03-01"
    assert partition_dir(tmp_path, early_local).name == "as_of=2025-03-01"


def test_all_files_of_a_run_share_one_timestamp_and_path_for_matches(tmp_path: Path) -> None:
    with new_sink(tmp_path) as sink:
        assert sink.stamp == STAMP
        assert sink.path_for("by_label") == sink.directory / f"by_label_{STAMP}.csv"
        assert {sink.issues_path, sink.labels_path, sink.assignees_path} == {
            sink.path_for("issues"),
            sink.path_for("issue_labels"),
            sink.path_for("issue_assignees"),
        }


def test_file_stamp_format_is_utc() -> None:
    assert file_stamp(datetime(2026, 11, 23, 9, 2, 33, tzinfo=UTC)) == "20261123T090233Z"
    assert file_stamp(datetime(2026, 11, 23, 11, 2, 33, tzinfo=timezone(timedelta(hours=2)))) == "20261123T090233Z"


def test_later_run_on_the_same_day_adds_files_and_keeps_earlier_ones(tmp_path: Path) -> None:
    write_all(tmp_path, make_issue(number=1))
    later = datetime(2025, 3, 1, 18, 0, 0, tzinfo=UTC)
    with new_sink(tmp_path, created=later) as sink:
        sink.write(enrich(make_issue(number=2), AS_OF))
    folder = tmp_path / "as_of=2025-03-01"
    assert len(list(folder.glob("issues_*.csv"))) == 2
    assert [r[0] for r in read(table(tmp_path, "issues"))[1:]] == ["1"]
    assert [r[0] for r in read(sink.issues_path)[1:]] == ["2"]


def test_existing_file_is_never_overwritten(tmp_path: Path) -> None:
    write_all(tmp_path, make_issue(number=1))
    with pytest.raises(FileExistsError):
        new_sink(tmp_path)  # same as_of date and same creation second
    assert [r[0] for r in read(table(tmp_path, "issues"))[1:]] == ["1"]


def test_context_manager_flushes_and_closes(tmp_path: Path) -> None:
    sink = new_sink(tmp_path)
    with sink:
        sink.write(enrich(make_issue(), AS_OF))
    assert len(read(table(tmp_path, "issues"))) == 2
    with pytest.raises(ValueError):
        sink.write(enrich(make_issue(), AS_OF))


def test_close_is_idempotent(tmp_path: Path) -> None:
    sink = new_sink(tmp_path)
    sink.close()
    sink.close()


def test_files_are_closed_when_the_block_raises(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError), new_sink(tmp_path) as sink:
        sink.write(enrich(make_issue(), AS_OF))
        raise RuntimeError("boom")
    assert len(read(table(tmp_path, "issues"))) == 2


def test_enriched_issue_type_is_accepted() -> None:
    assert isinstance(enrich(make_issue(), AS_OF), EnrichedIssue)
