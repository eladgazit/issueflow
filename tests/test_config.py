from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from issueflow.config import Settings


def test_defaults() -> None:
    settings = Settings()
    assert settings.repo == "microsoft/vscode"
    assert (settings.owner, settings.name) == ("microsoft", "vscode")
    assert settings.limit is None
    assert settings.out_dir == Path("out")
    assert settings.rate_limit_max_wait == 60.0
    assert settings.as_of.tzinfo is not None


def test_default_as_of_is_now_in_utc() -> None:
    assert abs(datetime.now(UTC) - Settings().as_of) < timedelta(seconds=5)


@pytest.mark.parametrize("repo", ["vscode", "a/b/c", "", "/x", "x/", "has space/x"])
def test_invalid_repo_is_rejected(repo: str) -> None:
    with pytest.raises(ValueError, match="owner/name"):
        Settings(repo=repo)


def test_naive_as_of_is_rejected() -> None:
    with pytest.raises(ValueError, match="timezone"):
        Settings(as_of=datetime(2025, 1, 1))


def test_negative_limit_is_rejected_but_zero_is_allowed() -> None:
    assert Settings(limit=0).limit == 0
    with pytest.raises(ValueError, match="limit"):
        Settings(limit=-1)


def test_negative_max_wait_is_rejected() -> None:
    with pytest.raises(ValueError, match="rate_limit_max_wait"):
        Settings(rate_limit_max_wait=-1)


def test_repo_with_dots_and_dashes_is_valid() -> None:
    assert Settings(repo="my-org/some.repo_1").name == "some.repo_1"
