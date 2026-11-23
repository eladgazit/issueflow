import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

_REPO = re.compile(r"^[\w.-]+/[\w.-]+$")


@dataclass(frozen=True)
class Settings:
    repo: str = "microsoft/vscode"  # "owner/name"
    limit: int | None = None  # max issues to fetch; None means all
    as_of: datetime = field(default_factory=lambda: datetime.now(UTC))
    out_dir: Path = Path("out")
    rate_limit_max_wait: float = 60.0  # longest quota reset we will sleep through, in seconds

    def __post_init__(self) -> None:
        if not _REPO.match(self.repo):
            raise ValueError(f"repo must look like 'owner/name', got {self.repo!r}")
        if self.limit is not None and self.limit < 0:
            raise ValueError("limit must be >= 0")
        if self.as_of.tzinfo is None:
            raise ValueError("as_of must be timezone-aware")
        if self.rate_limit_max_wait < 0:
            raise ValueError("rate_limit_max_wait must be >= 0")

    @property
    def owner(self) -> str:
        return self.repo.split("/", 1)[0]

    @property
    def name(self) -> str:
        return self.repo.split("/", 1)[1]
