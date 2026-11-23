import json
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode

DEFAULT_USER_AGENT = "issueflow"


@dataclass(frozen=True)
class Response:
    status: int
    headers: Mapping[str, str]  # keys lower-cased
    json: Any  # parsed body, None when the body is empty


class HttpError(Exception):
    """Raised for any non-2xx response."""

    def __init__(self, status: int, headers: Mapping[str, str], body: str = "") -> None:
        super().__init__(f"HTTP {status}: {body[:200]}" if body else f"HTTP {status}")
        self.status = status
        self.headers = headers
        self.body = body


def _lower_headers(headers: Any) -> dict[str, str]:
    return {k.lower(): v for k, v in headers.items()}


class HttpClient:
    def __init__(
        self,
        opener: Callable[..., Any] = urllib.request.urlopen,
        timeout: float = 30.0,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        self._opener = opener
        self._timeout = timeout
        self._headers = {"User-Agent": DEFAULT_USER_AGENT, **(headers or {})}

    def get(self, url: str, params: Mapping[str, Any] | None = None) -> Response:
        query = urlencode({k: v for k, v in (params or {}).items() if v is not None})
        if query:
            url = f"{url}{'&' if '?' in url else '?'}{query}"
        request = urllib.request.Request(url, headers=self._headers, method="GET")
        try:
            with self._opener(request, timeout=self._timeout) as resp:
                status, headers, body = resp.status, _lower_headers(resp.headers), resp.read()
        except urllib.error.HTTPError as exc:
            body_text = exc.read().decode("utf-8", errors="replace")
            raise HttpError(exc.code, _lower_headers(exc.headers), body_text) from None
        if not 200 <= status < 300:
            raise HttpError(status, headers, body.decode("utf-8", errors="replace"))
        return Response(status, headers, json.loads(body) if body else None)
