import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone

import pytest

from issueflow.messaging.messages import (
    IssueMessage,
    MessageError,
    RunEnd,
    RunStart,
    decode,
    encode,
    issue_from_dict,
    issue_to_dict,
)
from issueflow.models import Issue

ISSUE = Issue(
    number=7,
    id=70,
    title='Crash, "quoted"\nnewline — ünïcödé 🐞',
    state="closed",
    state_reason="completed",
    author="alice",
    created_at=datetime(2025, 2, 1, 8, 30, tzinfo=UTC),
    updated_at=datetime(2025, 2, 20, 9, 0, tzinfo=UTC),
    closed_at=datetime(2025, 2, 21, tzinfo=UTC),
    comments=4,
    milestone="Backlog",
    html_url="https://github.com/microsoft/vscode/issues/7",
    labels=("bug", "ui"),
    assignees=("bob",),
)
AS_OF = datetime(2025, 3, 1, 12, 0, tzinfo=UTC)


def test_start_round_trip() -> None:
    message = RunStart("run1", AS_OF, "microsoft/vscode")
    assert decode(encode(message)) == message


def test_end_round_trip() -> None:
    assert decode(encode(RunEnd("run1", 42))) == RunEnd("run1", 42)


def test_issue_round_trip_preserves_everything() -> None:
    assert decode(encode(IssueMessage("run1", 5, ISSUE))) == IssueMessage("run1", 5, ISSUE)


def test_issue_with_nulls_round_trips() -> None:
    sparse = replace(ISSUE, state="open", state_reason=None, author=None, closed_at=None, milestone=None, labels=(), assignees=())
    assert decode(encode(IssueMessage("r", 1, sparse))) == IssueMessage("r", 1, sparse)


def test_wire_format_is_documented_json() -> None:
    body = json.loads(encode(RunStart("run1", AS_OF, "o/r")))
    assert body == {"type": "start", "run_id": "run1", "as_of": "2025-03-01T12:00:00+00:00", "repo": "o/r"}
    issue_body = json.loads(encode(IssueMessage("run1", 5, ISSUE)))
    assert issue_body["type"] == "issue" and issue_body["run_id"] == "run1" and issue_body["seq"] == 5
    assert issue_body["issue"]["labels"] == ["bug", "ui"]
    assert issue_body["issue"]["created_at"] == "2025-02-01T08:30:00+00:00"
    assert json.loads(encode(RunEnd("run1", 3))) == {"type": "end", "run_id": "run1", "count": 3}


def test_non_ascii_is_written_as_utf8_not_escaped() -> None:
    assert "ünïcödé 🐞".encode() in encode(IssueMessage("r", 1, ISSUE))


def test_offsets_are_converted_to_utc() -> None:
    local = datetime(2025, 3, 1, 14, 0, tzinfo=timezone(timedelta(hours=2)))
    decoded = decode(encode(RunStart("r", local, "o/r")))
    assert isinstance(decoded, RunStart) and decoded.as_of == datetime(2025, 3, 1, 12, tzinfo=UTC)


def test_issue_dict_helpers_are_inverse() -> None:
    assert issue_from_dict(issue_to_dict(ISSUE)) == ISSUE


@pytest.mark.parametrize(
    "data",
    [
        b"not json",
        b'{"run_id":"r"}',  # no type
        b'{"type":"start","run_id":"r"}',  # missing fields
        b'{"type":"end","run_id":"r","count":"x"}',
        b'{"type":"issue","run_id":"r","issue":{}}',  # no seq
        b'{"type":"mystery","run_id":"r"}',
        b"[]",
    ],
)
def test_bad_messages_raise_message_error(data: bytes) -> None:
    with pytest.raises(MessageError):
        decode(data)
