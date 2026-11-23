from datetime import datetime

from issueflow.models import EnrichedIssue, Issue

STALE_AFTER_DAYS = 30
_SECONDS_PER_DAY = 86_400


def _whole_days(earlier: datetime, later: datetime) -> int:
    """Floored whole days between two aware datetimes, clamped at 0 if `earlier` is after `later`."""
    return max(0, int((later - earlier).total_seconds() // _SECONDS_PER_DAY))


def enrich(issue: Issue, as_of: datetime) -> EnrichedIssue:
    is_open = issue.state == "open"
    days_since_update = _whole_days(issue.updated_at, as_of)
    return EnrichedIssue(
        issue=issue,
        as_of=as_of,
        open_age_days=_whole_days(issue.created_at, as_of) if is_open else None,
        days_since_update=days_since_update,
        is_unassigned=not issue.assignees,
        is_stale=is_open and days_since_update > STALE_AFTER_DAYS,
    )
