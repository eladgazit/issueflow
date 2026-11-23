import re
from collections.abc import Iterator, Mapping
from datetime import UTC, datetime
from typing import Any

from issueflow.models import Issue
from issueflow.sources.http import HttpClient, HttpError, Response
from issueflow.sources.rate_limit import RateLimiter, is_rate_limit_error
from issueflow.sources.retry import RetryPolicy

GITHUB_API = "https://api.github.com"
GITHUB_HEADERS = {
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
}
MAX_RATE_LIMIT_WAITS = 3

_NEXT_LINK = re.compile(r'<([^>]+)>\s*;\s*rel="next"')


def next_url(link_header: str | None) -> str | None:
    """Extract the rel="next" target from a Link header, if any."""
    match = _NEXT_LINK.search(link_header or "")
    return match.group(1) if match else None


def _timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(UTC)


def parse_issue(raw: Mapping[str, Any]) -> Issue:
    """Normalize one GitHub issue payload into an Issue."""
    closed_at = raw.get("closed_at")
    milestone = raw.get("milestone")
    user = raw.get("user")
    return Issue(
        number=raw["number"],
        id=raw["id"],
        title=raw["title"],
        state=raw["state"],
        state_reason=raw.get("state_reason"),
        author=user["login"] if user else None,
        created_at=_timestamp(raw["created_at"]),
        updated_at=_timestamp(raw["updated_at"]),
        closed_at=_timestamp(closed_at) if closed_at else None,
        comments=raw.get("comments", 0),
        milestone=milestone["title"] if milestone else None,
        html_url=raw["html_url"],
        labels=tuple(label["name"] for label in raw.get("labels") or []),
        assignees=tuple(a["login"] for a in raw.get("assignees") or []),
    )


class GitHubIssuesSource:
    """Streams a repository's open and closed issues (pull requests excluded), one page at a time."""

    def __init__(
        self,
        owner: str = "microsoft",
        repo: str = "vscode",
        *,
        limit: int | None = None,
        per_page: int = 100,
        client: HttpClient | None = None,
        retry: RetryPolicy | None = None,
        limiter: RateLimiter | None = None,
        base_url: str = GITHUB_API,
    ) -> None:
        self._url = f"{base_url}/repos/{owner}/{repo}/issues"
        self._limit = limit
        self._per_page = per_page
        self._client = client or HttpClient(headers=GITHUB_HEADERS)
        self._retry = retry or RetryPolicy()
        self._limiter = limiter or RateLimiter()

    def iter_records(self) -> Iterator[dict[str, Any]]:
        """Raw issue payloads: PRs dropped, at most `limit` yielded, no page fetched beyond that."""
        emitted = 0
        for page in self._pages():
            for raw in page:
                if self._limit is not None and emitted >= self._limit:
                    return
                if "pull_request" in raw:
                    continue
                emitted += 1
                yield raw
            if self._limit is not None and emitted >= self._limit:
                return

    def iter_issues(self) -> Iterator[Issue]:
        for raw in self.iter_records():
            yield parse_issue(raw)

    def _pages(self) -> Iterator[list[dict[str, Any]]]:
        params: dict[str, Any] | None = {
            "state": "all",
            "sort": "created",
            "direction": "asc",
            "per_page": self._per_page,
            "page": 1,
        }
        url: str | None = self._url
        while url:
            resp = self._get(url, params)
            if not isinstance(resp.json, list):
                raise ValueError(f"expected a JSON list from {url}, got {type(resp.json).__name__}")
            yield resp.json
            url, params = next_url(resp.headers.get("link")), None  # next link is a full URL

    def _get(self, url: str, params: Mapping[str, Any] | None) -> Response:
        def attempt() -> Response:
            self._limiter.before_request()  # waits for the reset, or raises if it is too far away
            try:
                resp = self._client.get(url, params)
            except HttpError as exc:
                self._limiter.update(exc.headers)
                raise
            self._limiter.update(resp.headers)
            return resp

        rate_limit_waits = 0
        while True:
            try:
                return self._retry.call(attempt)
            except HttpError as exc:
                rate_limit_waits += 1
                if not is_rate_limit_error(exc) or rate_limit_waits > MAX_RATE_LIMIT_WAITS:
                    raise
