import csv
import statistics
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from issueflow.sinks.csv_sink import ISSUES_FILE, LABELS_FILE

BY_LABEL_FILE = "by_label.csv"
BY_LABEL_COLUMNS = (
    "label",
    "total",
    "open",
    "stale",
    "unassigned",
    "median_open_age_days",
    "median_days_to_close",
)
_SECONDS_PER_DAY = 86_400


@dataclass
class _LabelStats:
    total: int = 0
    open: int = 0
    stale: int = 0
    unassigned: int = 0
    open_ages: list[int] = field(default_factory=list)
    days_to_close: list[int] = field(default_factory=list)


def _median(values: list[int]) -> str:
    if not values:
        return ""
    middle = statistics.median(values)
    return str(int(middle)) if float(middle).is_integer() else f"{middle:.1f}"


def _days_to_close(row: dict[str, str]) -> int | None:
    if row["state"] != "closed" or not row["closed_at"]:
        return None
    seconds = (datetime.fromisoformat(row["closed_at"]) - datetime.fromisoformat(row["created_at"])).total_seconds()
    return max(0, int(seconds // _SECONDS_PER_DAY))


def aggregate_by_label(out_dir: Path | str) -> int:
    """Read issues.csv and issue_labels.csv from `out_dir` and write by_label.csv; return rows written.

    Both files are streamed together as a merge join: CsvSink writes issue_labels.csv in the same
    order as issues.csv, so no issue lookup table is needed. Memory holds only per-label counters
    and the integer lists the medians need. Issues without labels do not appear.
    """
    directory = Path(out_dir)
    stats: dict[str, _LabelStats] = {}
    with (
        (directory / ISSUES_FILE).open(encoding="utf-8", newline="") as issues_file,
        (directory / LABELS_FILE).open(encoding="utf-8", newline="") as labels_file,
    ):
        labels = csv.DictReader(labels_file)
        pending = next(labels, None)
        for row in csv.DictReader(issues_file):
            names: list[str] = []
            while pending is not None and pending["issue_number"] == row["number"]:
                names.append(pending["label"])
                pending = next(labels, None)
            is_open = row["state"] == "open"
            is_stale = row["is_stale"] == "true"
            is_unassigned = row["is_unassigned"] == "true"
            open_age = int(row["open_age_days"]) if is_open and row["open_age_days"] else None
            to_close = _days_to_close(row)
            for name in names:
                entry = stats.setdefault(name, _LabelStats())
                entry.total += 1
                entry.open += is_open
                entry.stale += is_stale
                entry.unassigned += is_unassigned
                if open_age is not None:
                    entry.open_ages.append(open_age)
                if to_close is not None:
                    entry.days_to_close.append(to_close)
        if pending is not None:
            raise ValueError(f"{LABELS_FILE} is not in the same order as {ISSUES_FILE} (stuck at issue {pending['issue_number']})")

    rows = sorted(stats.items(), key=lambda item: (-item[1].total, item[0]))
    with (directory / BY_LABEL_FILE).open("w", encoding="utf-8", newline="") as out:
        writer = csv.writer(out)
        writer.writerow(BY_LABEL_COLUMNS)
        for name, s in rows:
            writer.writerow(
                [name, s.total, s.open, s.stale, s.unassigned, _median(s.open_ages), _median(s.days_to_close)]
            )
    return len(rows)
