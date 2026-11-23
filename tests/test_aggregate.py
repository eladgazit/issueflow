import csv
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from issueflow.aggregate import BY_LABEL_COLUMNS, BY_LABEL_TABLE, aggregate_by_label
from issueflow.models import Issue
from issueflow.sinks.csv_sink import CsvSink
from issueflow.transforms.derived import enrich

AS_OF = datetime(2025, 3, 1, 12, 0, tzinfo=UTC)
CREATED = AS_OF - timedelta(days=100)
BASE = Issue(
    number=0,
    id=0,
    title="t",
    state="open",
    state_reason=None,
    author="a",
    created_at=CREATED,
    updated_at=AS_OF - timedelta(days=1),
    closed_at=None,
    comments=0,
    milestone=None,
    html_url="http://x",
    assignees=("bob",),
)


def make(number: int, **overrides: object) -> Issue:
    return replace(BASE, number=number, id=number, **overrides)


def closed(number: int, days_open: int, **overrides: object) -> Issue:
    return make(
        number,
        state="closed",
        closed_at=CREATED + timedelta(days=days_open, hours=3),
        updated_at=CREATED + timedelta(days=days_open),
        **overrides,
    )


def new_sink(tmp_path: Path) -> CsvSink:
    return CsvSink(tmp_path, AS_OF, clock=lambda: AS_OF)


def aggregate(sink: CsvSink) -> Path:
    out = sink.path_for(BY_LABEL_TABLE)
    aggregate_by_label(sink.issues_path, sink.labels_path, out)
    return out


def run(tmp_path: Path, *issues: Issue) -> dict[str, dict[str, str]]:
    with new_sink(tmp_path) as sink:
        for item in issues:
            sink.write(enrich(item, AS_OF))
    with aggregate(sink).open(encoding="utf-8", newline="") as f:
        return {row["label"]: row for row in csv.DictReader(f)}


def test_header_and_empty_input(tmp_path: Path) -> None:
    assert run(tmp_path) == {}
    (path,) = tmp_path.glob("as_of=*/by_label_*.csv")
    header = path.read_text().strip().split(",")
    assert tuple(header) == BY_LABEL_COLUMNS


def test_counts_per_label(tmp_path: Path) -> None:
    result = run(
        tmp_path,
        make(1, labels=("bug",)),  # open, assigned, not stale
        make(2, labels=("bug",), assignees=()),  # open, unassigned
        make(3, labels=("bug", "ui"), updated_at=AS_OF - timedelta(days=45)),  # open, stale
        closed(4, 10, labels=("ui",)),
    )
    bug, ui = result["bug"], result["ui"]
    assert (bug["total"], bug["open"], bug["stale"], bug["unassigned"]) == ("3", "3", "1", "1")
    assert (ui["total"], ui["open"], ui["stale"], ui["unassigned"]) == ("2", "1", "1", "0")


def test_median_open_age_uses_open_issues_only(tmp_path: Path) -> None:
    result = run(
        tmp_path,
        make(1, labels=("bug",), created_at=AS_OF - timedelta(days=10)),
        make(2, labels=("bug",), created_at=AS_OF - timedelta(days=30)),
        make(3, labels=("bug",), created_at=AS_OF - timedelta(days=90)),
        closed(4, 5, labels=("bug",)),  # ignored for open age
    )
    assert result["bug"]["median_open_age_days"] == "30"


def test_even_count_median_can_be_fractional(tmp_path: Path) -> None:
    result = run(
        tmp_path,
        make(1, labels=("a",), created_at=AS_OF - timedelta(days=10)),
        make(2, labels=("a",), created_at=AS_OF - timedelta(days=11)),
    )
    assert result["a"]["median_open_age_days"] == "10.5"


def test_median_days_to_close_uses_closed_issues_only(tmp_path: Path) -> None:
    result = run(
        tmp_path,
        closed(1, 2, labels=("bug",)),
        closed(2, 4, labels=("bug",)),
        closed(3, 20, labels=("bug",)),
        make(4, labels=("bug",)),  # open, ignored
    )
    assert result["bug"]["median_days_to_close"] == "4"


def test_medians_are_empty_when_no_data(tmp_path: Path) -> None:
    result = run(tmp_path, make(1, labels=("only-open",)), closed(2, 3, labels=("only-closed",)))
    assert result["only-open"]["median_days_to_close"] == ""
    assert result["only-closed"]["median_open_age_days"] == ""


def test_unlabeled_issues_are_skipped_and_do_not_break_the_join(tmp_path: Path) -> None:
    result = run(
        tmp_path,
        make(1, labels=()),
        make(2, labels=("bug",)),
        make(3, labels=()),
        make(4, labels=("bug",)),
    )
    assert list(result) == ["bug"]
    assert result["bug"]["total"] == "2"


def test_rows_sorted_by_total_then_name(tmp_path: Path) -> None:
    run(
        tmp_path,
        make(1, labels=("b", "a")),
        make(2, labels=("c",)),
        make(3, labels=("c",)),
    )
    (path,) = tmp_path.glob("as_of=*/by_label_*.csv")
    with path.open(encoding="utf-8", newline="") as f:
        assert [row["label"] for row in csv.DictReader(f)] == ["c", "a", "b"]


def test_returns_number_of_label_rows(tmp_path: Path) -> None:
    with new_sink(tmp_path) as sink:
        sink.write(enrich(make(1, labels=("a", "b")), AS_OF))
    assert aggregate_by_label(sink.issues_path, sink.labels_path, sink.path_for(BY_LABEL_TABLE)) == 2


def test_out_of_order_label_file_is_rejected(tmp_path: Path) -> None:
    with new_sink(tmp_path) as sink:
        sink.write(enrich(make(1, labels=("a",)), AS_OF))
        sink.write(enrich(make(2, labels=("b",)), AS_OF))
    header, *rows = sink.labels_path.read_text().splitlines()
    sink.labels_path.write_text("\n".join([header, *reversed(rows)]) + "\n")
    with pytest.raises(ValueError, match="not in the same order"):
        aggregate_by_label(sink.issues_path, sink.labels_path, sink.path_for(BY_LABEL_TABLE))
