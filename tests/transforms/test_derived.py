from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from issueflow.models import Issue
from issueflow.transforms.derived import enrich

AS_OF = datetime(2026, 1, 31, 12, 0, tzinfo=UTC)


def make_issue(**overrides: object) -> Issue:
    base = Issue(
        number=1,
        id=100,
        title="t",
        state="open",
        state_reason=None,
        author="a",
        created_at=AS_OF - timedelta(days=10),
        updated_at=AS_OF - timedelta(days=2),
        closed_at=None,
        comments=0,
        milestone=None,
        html_url="http://x/1",
        assignees=("bob",),
    )
    return replace(base, **overrides)


def test_open_age_days_for_open_issue() -> None:
    assert enrich(make_issue(), AS_OF).open_age_days == 10


def test_open_age_days_is_none_for_closed_issue() -> None:
    closed = make_issue(state="closed", closed_at=AS_OF - timedelta(days=1))
    assert enrich(closed, AS_OF).open_age_days is None


def test_days_are_floored() -> None:
    issue = make_issue(created_at=AS_OF - timedelta(days=3, hours=23), updated_at=AS_OF - timedelta(hours=23))
    result = enrich(issue, AS_OF)
    assert result.open_age_days == 3
    assert result.days_since_update == 0


def test_days_since_update_applies_to_closed_issues_too() -> None:
    closed = make_issue(state="closed", updated_at=AS_OF - timedelta(days=45))
    assert enrich(closed, AS_OF).days_since_update == 45


@pytest.mark.parametrize(
    ("age", "expected"),
    [
        (timedelta(days=29), False),
        (timedelta(days=30), False),
        (timedelta(days=30, hours=23), False),  # floors to 30, not "more than 30"
        (timedelta(days=31), True),
        (timedelta(days=400), True),
    ],
)
def test_is_stale_boundary_for_open_issues(age: timedelta, expected: bool) -> None:
    assert enrich(make_issue(updated_at=AS_OF - age), AS_OF).is_stale is expected


def test_closed_issue_is_never_stale() -> None:
    closed = make_issue(state="closed", updated_at=AS_OF - timedelta(days=400))
    assert enrich(closed, AS_OF).is_stale is False


def test_is_unassigned() -> None:
    assert enrich(make_issue(assignees=()), AS_OF).is_unassigned is True
    assert enrich(make_issue(assignees=("bob",)), AS_OF).is_unassigned is False
    assert enrich(make_issue(assignees=("bob", "amy")), AS_OF).is_unassigned is False


def test_timestamp_after_as_of_is_clamped_to_zero() -> None:
    future = make_issue(created_at=AS_OF + timedelta(days=1), updated_at=AS_OF + timedelta(hours=1))
    result = enrich(future, AS_OF)
    assert result.open_age_days == 0
    assert result.days_since_update == 0


def test_as_of_and_issue_are_carried_through() -> None:
    issue = make_issue()
    result = enrich(issue, AS_OF)
    assert result.issue is issue
    assert result.as_of == AS_OF
