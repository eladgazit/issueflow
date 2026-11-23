import csv
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from issueflow.cli import main, parse_as_of
from issueflow.sources.http import HttpError, Response


def raw(number: int, **overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "number": number,
        "id": number,
        "title": f"issue {number}",
        "state": "open",
        "state_reason": None,
        "user": {"login": "alice"},
        "created_at": "2025-01-01T00:00:00Z",
        "updated_at": "2025-02-01T00:00:00Z",
        "closed_at": None,
        "comments": 0,
        "milestone": None,
        "html_url": f"http://x/{number}",
        "labels": [{"name": "bug"}],
        "assignees": [],
    }
    return {**base, **overrides}


class FakeClient:
    def __init__(self, *outcomes: Response | Exception) -> None:
        self.outcomes = list(outcomes)
        self.calls: list[tuple[str, Mapping[str, Any] | None]] = []

    def get(self, url: str, params: Mapping[str, Any] | None = None) -> Response:
        self.calls.append((url, params))
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def ok(*items: dict[str, Any]) -> Response:
    return Response(200, {}, list(items))


def read(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def find(out: Path, table: str) -> Path:
    """The single file of `table` inside the daily partition folder."""
    (path,) = out.glob(f"as_of=*/{table}_*.csv")
    return path


def run(tmp_path: Path, client: FakeClient, *args: str) -> int:
    return main(["--out", str(tmp_path), "--as-of", "2025-03-01T00:00:00Z", *args], client=client)  # type: ignore[arg-type]


def test_end_to_end_writes_all_four_files(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    client = FakeClient(
        ok(
            raw(1),
            raw(2, pull_request={"url": "x"}),
            raw(3, state="closed", closed_at="2025-01-11T00:00:00Z", assignees=[{"login": "bob"}]),
        )
    )
    assert run(tmp_path, client) == 0

    issues = read(find(tmp_path, "issues"))
    assert [r["number"] for r in issues] == ["1", "3"]  # PR 2 excluded
    assert issues[0]["open_age_days"] == "59"
    assert issues[0]["days_since_update"] == "28"
    assert issues[0]["is_stale"] == "false"  # 28 days is within the 30-day limit
    assert issues[1]["open_age_days"] == ""
    assert issues[1]["is_unassigned"] == "false"
    assert read(find(tmp_path, "issue_labels")) == [
        {"issue_number": "1", "label": "bug"},
        {"issue_number": "3", "label": "bug"},
    ]
    by_label = read(find(tmp_path, "by_label"))
    assert by_label[0]["label"] == "bug" and by_label[0]["total"] == "2"
    assert "wrote 2 issues" in capsys.readouterr().out


def test_files_land_in_the_as_of_partition_with_a_timestamp(tmp_path: Path) -> None:
    run(tmp_path, FakeClient(ok(raw(1))))
    folder = tmp_path / "as_of=2025-03-01"  # from --as-of 2025-03-01T00:00:00Z
    names = sorted(f.name for f in folder.iterdir())
    assert len(names) == 4
    stamp = names[0].rsplit("_", 1)[1].removesuffix(".csv")
    assert len(stamp) == 16 and stamp.endswith("Z")
    assert names == sorted(f"{t}_{stamp}.csv" for t in ("issues", "issue_labels", "issue_assignees", "by_label"))


def test_as_of_is_stamped_on_every_row(tmp_path: Path) -> None:
    run(tmp_path, FakeClient(ok(raw(1), raw(2))))
    assert {r["as_of"] for r in read(find(tmp_path, "issues"))} == {"2025-03-01T00:00:00Z"}


def test_limit_caps_output_and_requests(tmp_path: Path) -> None:
    client = FakeClient(
        Response(200, {"link": '<http://next>; rel="next"'}, [raw(1), raw(2)]),
        ok(raw(3)),
    )
    assert run(tmp_path, client, "--limit", "2") == 0
    assert len(read(find(tmp_path, "issues"))) == 2
    assert len(client.calls) == 1


def test_repo_option_is_used_in_the_request(tmp_path: Path) -> None:
    client = FakeClient(ok())
    run(tmp_path, client, "--repo", "octo/cat")
    assert client.calls[0][0] == "https://api.github.com/repos/octo/cat/issues"


def test_rate_limit_too_long_exits_with_message(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    far_future = str(datetime.now(UTC).timestamp() + 3000)
    limited = HttpError(403, {"x-ratelimit-remaining": "0", "x-ratelimit-reset": far_future})
    assert run(tmp_path, FakeClient(limited)) == 1
    err = capsys.readouterr().err
    assert "rate limit" in err and "incomplete" in err


def test_http_error_exits_nonzero_without_traceback(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert run(tmp_path, FakeClient(HttpError(404, {}, "Not Found"))) == 1
    assert "404" in capsys.readouterr().err


def test_invalid_repo_is_a_usage_error(tmp_path: Path) -> None:
    with pytest.raises(SystemExit) as excinfo:
        main(["--out", str(tmp_path), "--repo", "nope"], client=FakeClient())  # type: ignore[arg-type]
    assert excinfo.value.code == 2


def test_parse_as_of_treats_naive_as_utc_and_converts_offsets() -> None:
    assert parse_as_of("2025-03-01T12:00:00") == datetime(2025, 3, 1, 12, tzinfo=UTC)
    assert parse_as_of("2025-03-01T14:00:00+02:00") == datetime(2025, 3, 1, 12, tzinfo=UTC)


def test_parse_as_of_rejects_garbage() -> None:
    import argparse

    with pytest.raises(argparse.ArgumentTypeError):
        parse_as_of("yesterday")
