from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

import pytest

from issueflow.sources.github import GITHUB_HEADERS, GitHubIssuesSource, next_url, parse_issue
from issueflow.sources.http import HttpError, Response
from issueflow.sources.rate_limit import RateLimiter, RateLimitExceeded
from issueflow.sources.retry import RetryPolicy

BASE = "https://api.github.com/repos/microsoft/vscode/issues"
PAGE2 = f"{BASE}?page=2"
PAGE3 = f"{BASE}?page=3"
NOW = 1_000.0


def raw_issue(number: int, **overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "number": number,
        "id": number * 10,
        "title": f"issue {number}",
        "state": "open",
        "state_reason": None,
        "user": {"login": "alice"},
        "created_at": "2025-01-01T10:00:00Z",
        "updated_at": "2025-02-01T10:00:00Z",
        "closed_at": None,
        "comments": 3,
        "milestone": {"title": "Backlog"},
        "html_url": f"https://github.com/microsoft/vscode/issues/{number}",
        "labels": [{"name": "bug"}, {"name": "ui"}],
        "assignees": [{"login": "bob"}],
    }
    return {**base, **overrides}


def pull_request(number: int) -> dict[str, Any]:
    return raw_issue(number, pull_request={"url": "x"})


def page(items: list[dict[str, Any]], next_page: str | None = None, **headers: str) -> Response:
    hdrs = dict(headers)
    if next_page:
        hdrs["link"] = f'<{next_page}>; rel="next", <{BASE}?page=9>; rel="last"'
    return Response(200, hdrs, items)


class ScriptedClient:
    """Plays back outcomes in order and records every call."""

    def __init__(self, *outcomes: Response | Exception) -> None:
        self.outcomes = list(outcomes)
        self.calls: list[tuple[str, Mapping[str, Any] | None]] = []

    def get(self, url: str, params: Mapping[str, Any] | None = None) -> Response:
        self.calls.append((url, params))
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


class Clock:
    def __init__(self) -> None:
        self.sleeps: list[float] = []

    def now(self) -> float:
        return NOW

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)


def make_source(client: ScriptedClient, clock: Clock | None = None, **kwargs: Any) -> GitHubIssuesSource:
    clock = clock or Clock()
    return GitHubIssuesSource(
        client=client,  # type: ignore[arg-type]
        retry=RetryPolicy(sleep=clock.sleep, random=lambda: 1.0),
        limiter=RateLimiter(max_wait=60, clock=clock.now, sleep=clock.sleep),
        **kwargs,
    )


def numbers(source: GitHubIssuesSource) -> list[int]:
    return [r["number"] for r in source.iter_records()]


def test_first_request_uses_expected_endpoint_and_params() -> None:
    client = ScriptedClient(page([]))
    numbers(make_source(client))
    url, params = client.calls[0]
    assert url == BASE
    assert params == {"state": "all", "sort": "created", "direction": "asc", "per_page": 100, "page": 1}


def test_follows_next_link_until_it_disappears() -> None:
    client = ScriptedClient(
        page([raw_issue(1)], PAGE2),
        page([raw_issue(2)], PAGE3),
        page([raw_issue(3)]),
    )
    assert numbers(make_source(client)) == [1, 2, 3]
    assert [c[0] for c in client.calls] == [BASE, PAGE2, PAGE3]
    assert client.calls[1][1] is None  # next link already carries the query


def test_pull_requests_are_excluded() -> None:
    client = ScriptedClient(page([raw_issue(1), pull_request(2), raw_issue(3)]))
    assert numbers(make_source(client)) == [1, 3]


def test_limit_stops_mid_page() -> None:
    client = ScriptedClient(page([raw_issue(1), raw_issue(2), raw_issue(3)], PAGE2))
    assert numbers(make_source(client, limit=2)) == [1, 2]
    assert len(client.calls) == 1


def test_limit_at_page_boundary_does_not_fetch_next_page() -> None:
    client = ScriptedClient(page([raw_issue(1), raw_issue(2)], PAGE2))
    assert numbers(make_source(client, limit=2)) == [1, 2]
    assert len(client.calls) == 1


def test_limit_counts_issues_not_pull_requests() -> None:
    client = ScriptedClient(page([pull_request(1), raw_issue(2)], PAGE2), page([raw_issue(3), raw_issue(4)]))
    assert numbers(make_source(client, limit=2)) == [2, 3]
    assert len(client.calls) == 2


def test_limit_zero_yields_nothing() -> None:
    assert numbers(make_source(ScriptedClient(page([raw_issue(1)])), limit=0)) == []


def test_records_are_lazy() -> None:
    client = ScriptedClient(page([raw_issue(1)], PAGE2), page([raw_issue(2)]))
    records = make_source(client).iter_records()
    assert client.calls == []
    next(records)
    assert len(client.calls) == 1


def test_iter_issues_yields_parsed_models() -> None:
    issues = list(make_source(ScriptedClient(page([raw_issue(7)]))).iter_issues())
    assert [i.number for i in issues] == [7]
    assert issues[0].labels == ("bug", "ui")


def test_non_list_payload_is_rejected() -> None:
    client = ScriptedClient(Response(200, {}, {"message": "weird"}))
    with pytest.raises(ValueError, match="expected a JSON list"):
        numbers(make_source(client))


def test_default_headers_request_github_json_api() -> None:
    assert GITHUB_HEADERS == {"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"}


# --- retry and rate limiting -------------------------------------------------


def test_5xx_is_retried_with_backoff() -> None:
    clock = Clock()
    client = ScriptedClient(HttpError(502, {}), HttpError(503, {}), page([raw_issue(1)]))
    assert numbers(make_source(client, clock)) == [1]
    assert len(client.calls) == 3
    assert clock.sleeps == [1, 2]


def test_404_is_not_retried() -> None:
    client = ScriptedClient(HttpError(404, {}))
    with pytest.raises(HttpError) as excinfo:
        numbers(make_source(client))
    assert excinfo.value.status == 404
    assert len(client.calls) == 1


def test_rate_limited_response_waits_for_reset_then_retries() -> None:
    clock = Clock()
    limited = HttpError(403, {"x-ratelimit-remaining": "0", "x-ratelimit-reset": str(NOW + 20)})
    client = ScriptedClient(limited, page([raw_issue(1)]))
    assert numbers(make_source(client, clock)) == [1]
    assert clock.sleeps == [20.0]
    assert len(client.calls) == 2


def test_retry_after_on_429_is_honored() -> None:
    clock = Clock()
    client = ScriptedClient(HttpError(429, {"retry-after": "5"}), page([raw_issue(1)]))
    assert numbers(make_source(client, clock)) == [1]
    assert clock.sleeps == [5.0]


def test_rate_limit_reset_beyond_cap_fails_fast() -> None:
    clock = Clock()
    limited = HttpError(403, {"x-ratelimit-remaining": "0", "x-ratelimit-reset": str(NOW + 3000)})
    client = ScriptedClient(limited)
    with pytest.raises(RateLimitExceeded):
        numbers(make_source(client, clock))
    assert clock.sleeps == []
    assert len(client.calls) == 1


def test_exhausted_quota_in_success_response_waits_before_next_page() -> None:
    clock = Clock()
    client = ScriptedClient(
        page([raw_issue(1)], PAGE2, **{"x-ratelimit-remaining": "0", "x-ratelimit-reset": str(NOW + 15)}),
        page([raw_issue(2)]),
    )
    assert numbers(make_source(client, clock)) == [1, 2]
    assert clock.sleeps == [15.0]


def test_persistent_rate_limiting_eventually_raises() -> None:
    clock = Clock()
    limited = HttpError(403, {"x-ratelimit-remaining": "0", "x-ratelimit-reset": str(NOW + 1)})
    client = ScriptedClient(*[limited for _ in range(10)])
    with pytest.raises(HttpError):
        numbers(make_source(client, clock))
    assert len(client.calls) == 4  # first try + 3 waits


# --- helpers -----------------------------------------------------------------


def test_next_url_parsing() -> None:
    header = '<https://x/?page=2>; rel="next", <https://x/?page=9>; rel="last"'
    assert next_url(header) == "https://x/?page=2"
    assert next_url('<https://x/?page=1>; rel="prev"') is None
    assert next_url(None) is None
    assert next_url("") is None


def test_parse_issue_full_payload() -> None:
    issue = parse_issue(raw_issue(5, state="closed", state_reason="completed", closed_at="2025-03-01T00:00:00Z"))
    assert issue.number == 5 and issue.id == 50
    assert issue.state == "closed" and issue.state_reason == "completed"
    assert issue.author == "alice"
    assert issue.created_at == datetime(2025, 1, 1, 10, tzinfo=UTC)
    assert issue.closed_at == datetime(2025, 3, 1, tzinfo=UTC)
    assert issue.milestone == "Backlog"
    assert issue.labels == ("bug", "ui") and issue.assignees == ("bob",)
    assert issue.created_at.tzinfo is not None


def test_parse_issue_handles_nulls() -> None:
    issue = parse_issue(raw_issue(6, user=None, milestone=None, labels=[], assignees=[], closed_at=None))
    assert issue.author is None
    assert issue.milestone is None
    assert issue.closed_at is None
    assert issue.labels == () and issue.assignees == ()


def test_parse_issue_converts_offsets_to_utc() -> None:
    issue = parse_issue(raw_issue(8, created_at="2025-01-01T12:00:00+02:00"))
    assert issue.created_at == datetime(2025, 1, 1, 10, tzinfo=UTC)
