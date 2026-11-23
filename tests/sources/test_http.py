import io
import json
import urllib.error
import urllib.request
from typing import Any

import pytest

from issueflow.sources.http import HttpClient, HttpError


class FakeResponse:
    def __init__(self, status: int = 200, headers: dict[str, str] | None = None, body: bytes = b"") -> None:
        self.status = status
        self.headers = headers or {}
        self._body = body

    def read(self) -> bytes:
        return self._body

    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, *exc: object) -> None:
        return None


class FakeOpener:
    """Records requests and returns/raises the scripted outcome."""

    def __init__(self, outcome: FakeResponse | Exception) -> None:
        self.outcome = outcome
        self.requests: list[urllib.request.Request] = []
        self.timeouts: list[float] = []

    def __call__(self, request: urllib.request.Request, timeout: float) -> FakeResponse:
        self.requests.append(request)
        self.timeouts.append(timeout)
        if isinstance(self.outcome, Exception):
            raise self.outcome
        return self.outcome


def http_error(status: int, headers: dict[str, str], body: bytes = b"") -> urllib.error.HTTPError:
    return urllib.error.HTTPError("http://x", status, "err", headers, io.BytesIO(body))  # type: ignore[arg-type]


def ok(payload: Any, headers: dict[str, str] | None = None) -> FakeResponse:
    return FakeResponse(200, headers, json.dumps(payload).encode())


def test_get_parses_status_headers_and_json() -> None:
    opener = FakeOpener(ok([{"id": 1}], {"Link": "<next>", "X-RateLimit-Remaining": "59"}))
    resp = HttpClient(opener=opener).get("http://x/issues")
    assert resp.status == 200
    assert resp.json == [{"id": 1}]
    assert resp.headers == {"link": "<next>", "x-ratelimit-remaining": "59"}


def test_params_are_encoded_and_none_values_dropped() -> None:
    opener = FakeOpener(ok([]))
    HttpClient(opener=opener).get("http://x/issues", {"state": "all", "per_page": 100, "since": None})
    assert opener.requests[0].full_url == "http://x/issues?state=all&per_page=100"


def test_params_append_to_existing_query() -> None:
    opener = FakeOpener(ok([]))
    HttpClient(opener=opener).get("http://x/issues?page=2", {"per_page": 100})
    assert opener.requests[0].full_url == "http://x/issues?page=2&per_page=100"


def test_no_params_leaves_url_untouched() -> None:
    opener = FakeOpener(ok([]))
    HttpClient(opener=opener).get("http://x/issues")
    assert opener.requests[0].full_url == "http://x/issues"


def test_extra_headers_and_user_agent_are_sent() -> None:
    opener = FakeOpener(ok([]))
    HttpClient(opener=opener, headers={"Accept": "application/vnd.github+json"}).get("http://x")
    request = opener.requests[0]
    assert request.get_header("Accept") == "application/vnd.github+json"
    assert request.get_header("User-agent") == "issueflow"


def test_timeout_is_passed_to_opener() -> None:
    opener = FakeOpener(ok([]))
    HttpClient(opener=opener, timeout=7.5).get("http://x")
    assert opener.timeouts == [7.5]


def test_empty_body_gives_none_json() -> None:
    resp = HttpClient(opener=FakeOpener(FakeResponse(204))).get("http://x")
    assert resp.status == 204
    assert resp.json is None


def test_http_error_carries_status_and_lowercased_headers() -> None:
    err = http_error(403, {"X-RateLimit-Remaining": "0", "Retry-After": "30"}, b'{"message":"rate limited"}')
    with pytest.raises(HttpError) as excinfo:
        HttpClient(opener=FakeOpener(err)).get("http://x")
    assert excinfo.value.status == 403
    assert excinfo.value.headers == {"x-ratelimit-remaining": "0", "retry-after": "30"}
    assert "rate limited" in excinfo.value.body


def test_non_2xx_returned_by_opener_raises_http_error() -> None:
    with pytest.raises(HttpError) as excinfo:
        HttpClient(opener=FakeOpener(FakeResponse(500, {"Retry-After": "1"}, b"boom"))).get("http://x")
    assert excinfo.value.status == 500
    assert excinfo.value.headers == {"retry-after": "1"}
