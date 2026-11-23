from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class Issue:
    """A GitHub issue normalized to the fields we keep; all datetimes are timezone-aware UTC."""

    number: int
    id: int
    title: str
    state: str  # "open" | "closed"
    state_reason: str | None
    author: str | None
    created_at: datetime
    updated_at: datetime
    closed_at: datetime | None
    comments: int
    milestone: str | None
    html_url: str
    labels: tuple[str, ...] = ()
    assignees: tuple[str, ...] = ()


@dataclass(frozen=True)
class EnrichedIssue:
    issue: Issue
    as_of: datetime
    open_age_days: int | None  # None for closed issues
    days_since_update: int
    is_unassigned: bool
    is_stale: bool
