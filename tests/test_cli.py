import csv
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from issueflow.cli import main, parse_as_of
from issueflow.messaging.kafka import KafkaUnavailableError
from issueflow.messaging.messages import IssueMessage, RunEnd, RunStart, decode
from issueflow.sources.http import HttpError, Response

from conftest import FakeBroker


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


AS_OF = "2025-03-01T00:00:00Z"


def run(tmp_path: Path, client: FakeClient, broker: FakeBroker, *args: str, command: str = "run") -> int:
    argv = [command, *args]
    if command in ("run", "consume"):
        argv += ["--out", str(tmp_path)]
    if command in ("run", "produce"):
        argv += ["--as-of", AS_OF]
    return main(argv, client=client, publisher=broker.publisher(), reader=broker.reader())  # type: ignore[arg-type]


def messages(broker: FakeBroker) -> list[Any]:
    return [decode(r) for r in broker.records]


def test_run_end_to_end_writes_all_four_files(tmp_path: Path, broker: FakeBroker, capsys: pytest.CaptureFixture[str]) -> None:
    client = FakeClient(
        ok(
            raw(1),
            raw(2, pull_request={"url": "x"}),
            raw(3, state="closed", closed_at="2025-01-11T00:00:00Z", assignees=[{"login": "bob"}]),
        )
    )
    assert run(tmp_path, client, broker) == 0

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
    out = capsys.readouterr().out
    assert "published 2 issues" in out and "wrote 2 issues" in out


def test_issues_really_travel_through_kafka_as_messages(tmp_path: Path, broker: FakeBroker) -> None:
    run(tmp_path, FakeClient(ok(raw(1), raw(2))), broker)
    sent = messages(broker)
    assert isinstance(sent[0], RunStart) and isinstance(sent[-1], RunEnd)
    assert [m.issue.number for m in sent if isinstance(m, IssueMessage)] == [1, 2]
    assert sent[-1].count == 2
    assert broker.committed == len(broker.records)


def test_produce_only_publishes_and_writes_no_files(tmp_path: Path, broker: FakeBroker, capsys: pytest.CaptureFixture[str]) -> None:
    assert run(tmp_path, FakeClient(ok(raw(1), raw(2))), broker, command="produce") == 0
    assert len(broker.records) == 4  # start, 2 issues, end
    assert not list(tmp_path.iterdir())
    assert "published 2 issues" in capsys.readouterr().out


def test_consume_only_reads_what_produce_left_behind(tmp_path: Path, broker: FakeBroker) -> None:
    run(tmp_path, FakeClient(ok(raw(1), raw(2), raw(3))), broker, command="produce")
    assert run(tmp_path, FakeClient(), broker, command="consume") == 0
    assert [r["number"] for r in read(find(tmp_path, "issues"))] == ["1", "2", "3"]


def test_consume_with_an_empty_topic_reports_no_run(tmp_path: Path, broker: FakeBroker, capsys: pytest.CaptureFixture[str]) -> None:
    assert run(tmp_path, FakeClient(), broker, command="consume") == 1
    assert "no run found" in capsys.readouterr().err
    assert not list(tmp_path.iterdir())


def test_run_reads_its_own_run_even_if_older_runs_are_waiting(tmp_path: Path, broker: FakeBroker) -> None:
    run(tmp_path, FakeClient(ok(raw(1))), broker, command="produce")  # left unread
    assert run(tmp_path, FakeClient(ok(raw(40), raw(41))), broker) == 0
    assert [r["number"] for r in read(find(tmp_path, "issues"))] == ["40", "41"]


def test_as_of_is_stamped_on_every_row(tmp_path: Path, broker: FakeBroker) -> None:
    run(tmp_path, FakeClient(ok(raw(1), raw(2))), broker)
    assert {r["as_of"] for r in read(find(tmp_path, "issues"))} == {"2025-03-01T00:00:00Z"}


def test_files_land_in_the_as_of_partition_with_a_timestamp(tmp_path: Path, broker: FakeBroker) -> None:
    run(tmp_path, FakeClient(ok(raw(1))), broker)
    folder = tmp_path / "as_of=2025-03-01"  # from --as-of 2025-03-01T00:00:00Z
    names = sorted(f.name for f in folder.iterdir())
    assert len(names) == 4
    stamp = names[0].rsplit("_", 1)[1].removesuffix(".csv")
    assert len(stamp) == 16 and stamp.endswith("Z")
    assert names == sorted(f"{t}_{stamp}.csv" for t in ("issues", "issue_labels", "issue_assignees", "by_label"))


def test_limit_caps_output_and_requests(tmp_path: Path, broker: FakeBroker) -> None:
    client = FakeClient(
        Response(200, {"link": '<http://next>; rel="next"'}, [raw(1), raw(2)]),
        ok(raw(3)),
    )
    assert run(tmp_path, client, broker, "--limit", "2") == 0
    assert len(read(find(tmp_path, "issues"))) == 2
    assert len(client.calls) == 1


def test_repo_option_is_used_in_the_request(tmp_path: Path, broker: FakeBroker) -> None:
    client = FakeClient(ok())
    run(tmp_path, client, broker, "--repo", "octo/cat")
    assert client.calls[0][0] == "https://api.github.com/repos/octo/cat/issues"


def test_rate_limit_too_long_exits_with_message_and_no_end_marker(
    tmp_path: Path, broker: FakeBroker, capsys: pytest.CaptureFixture[str]
) -> None:
    far_future = str(datetime.now(UTC).timestamp() + 3000)
    limited = HttpError(403, {"x-ratelimit-remaining": "0", "x-ratelimit-reset": far_future})
    assert run(tmp_path, FakeClient(limited), broker) == 1
    err = capsys.readouterr().err
    assert "rate limit" in err and "Nothing was ended" in err
    assert not any(isinstance(m, RunEnd) for m in messages(broker))


def test_http_error_exits_nonzero_without_traceback(tmp_path: Path, broker: FakeBroker, capsys: pytest.CaptureFixture[str]) -> None:
    assert run(tmp_path, FakeClient(HttpError(404, {}, "Not Found")), broker) == 1
    assert "404" in capsys.readouterr().err


def test_unreachable_kafka_exits_with_a_helpful_message(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    class Down:
        def publish(self, message: Any) -> None:
            raise KafkaUnavailableError("localhost:9092")

        def flush(self) -> None: ...

    assert main(["produce", "--as-of", AS_OF], client=FakeClient(ok(raw(1))), publisher=Down()) == 1  # type: ignore[arg-type]
    assert "docker compose up -d" in capsys.readouterr().err


def test_invalid_repo_is_a_usage_error(tmp_path: Path, broker: FakeBroker) -> None:
    with pytest.raises(SystemExit) as excinfo:
        main(["produce", "--repo", "nope"], publisher=broker.publisher())
    assert excinfo.value.code == 2


def test_invalid_idle_timeout_is_a_usage_error(tmp_path: Path, broker: FakeBroker) -> None:
    with pytest.raises(SystemExit) as excinfo:
        main(["consume", "--idle-timeout", "0"], reader=broker.reader())
    assert excinfo.value.code == 2


def test_a_command_is_required() -> None:
    with pytest.raises(SystemExit) as excinfo:
        main([])
    assert excinfo.value.code == 2


def test_parse_as_of_treats_naive_as_utc_and_converts_offsets() -> None:
    assert parse_as_of("2025-03-01T12:00:00") == datetime(2025, 3, 1, 12, tzinfo=UTC)
    assert parse_as_of("2025-03-01T14:00:00+02:00") == datetime(2025, 3, 1, 12, tzinfo=UTC)


def test_parse_as_of_rejects_garbage() -> None:
    import argparse

    with pytest.raises(argparse.ArgumentTypeError):
        parse_as_of("yesterday")
