from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class RuleSelection:
    ruleset: str
    profile: str
    version: str
    path: Path
    data: dict[str, Any]


@dataclass(frozen=True)
class ExplainRecord:
    run_id: str
    mode: str
    email_profile: str
    email_version: str
    content_profile: str
    content_version: str
    notes: list[str]

