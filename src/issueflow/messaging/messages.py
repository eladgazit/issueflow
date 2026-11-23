import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from issueflow.models import Issue


class MessageError(ValueError):
    """A message could not be decoded."""


@dataclass(frozen=True)
class RunStart:
    run_id: str
    as_of: datetime
    repo: str


@dataclass(frozen=True)
class IssueMessage:
    run_id: str
    seq: int  # 1, 2, 3... within the run; lets the consumer drop retried duplicates
    issue: Issue


@dataclass(frozen=True)
class RunEnd:
    run_id: str
    count: int  # number of issue messages sent in the run


Message = RunStart | IssueMessage | RunEnd

_DATETIME_FIELDS = ("created_at", "updated_at", "closed_at")


def _iso(value: datetime | None) -> str | None:
    return None if value is None else value.astimezone(UTC).isoformat()


def _parse(value: str | None) -> datetime | None:
    return None if value is None else datetime.fromisoformat(value).astimezone(UTC)


def issue_to_dict(issue: Issue) -> dict[str, Any]:
    return {
        "number": issue.number,
        "id": issue.id,
        "title": issue.title,
        "state": issue.state,
        "state_reason": issue.state_reason,
        "author": issue.author,
        "created_at": _iso(issue.created_at),
        "updated_at": _iso(issue.updated_at),
        "closed_at": _iso(issue.closed_at),
        "comments": issue.comments,
        "milestone": issue.milestone,
        "html_url": issue.html_url,
        "labels": list(issue.labels),
        "assignees": list(issue.assignees),
    }


def issue_from_dict(data: dict[str, Any]) -> Issue:
    fields = {k: v for k, v in data.items() if k not in _DATETIME_FIELDS}
    return Issue(
        **{**fields, "labels": tuple(data["labels"]), "assignees": tuple(data["assignees"])},
        created_at=_parse(data["created_at"]),  # type: ignore[arg-type]
        updated_at=_parse(data["updated_at"]),  # type: ignore[arg-type]
        closed_at=_parse(data["closed_at"]),
    )


def encode(message: Message) -> bytes:
    """Serialize a message to UTF-8 JSON."""
    match message:
        case RunStart():
            body: dict[str, Any] = {
                "type": "start",
                "run_id": message.run_id,
                "as_of": _iso(message.as_of),
                "repo": message.repo,
            }
        case IssueMessage():
            body = {
                "type": "issue",
                "run_id": message.run_id,
                "seq": message.seq,
                "issue": issue_to_dict(message.issue),
            }
        case RunEnd():
            body = {"type": "end", "run_id": message.run_id, "count": message.count}
    return json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def decode(data: bytes) -> Message:
    """Parse a message; raise MessageError for bad JSON, an unknown type or missing fields."""
    try:
        body = json.loads(data)
        kind = body["type"]
        run_id = body["run_id"]
        if kind == "start":
            return RunStart(run_id, _parse(body["as_of"]), body["repo"])  # type: ignore[arg-type]
        if kind == "issue":
            return IssueMessage(run_id, int(body["seq"]), issue_from_dict(body["issue"]))
        if kind == "end":
            return RunEnd(run_id, int(body["count"]))
    except (ValueError, KeyError, TypeError) as exc:
        raise MessageError(f"cannot decode message: {exc!r}") from None
    raise MessageError(f"unknown message type {kind!r}")
