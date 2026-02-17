from __future__ import annotations

import json
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo

from app.adapters.content_rules_adapter import to_content_runtime
from app.adapters.email_rules_adapter import to_email_runtime
from app.rules.engine import RuleEngine


def run_dryrun(
    email_profile: str | None = None,
    content_profile: str | None = None,
) -> dict:
    engine = RuleEngine()
    email_rule, content_rule = engine.load_pair(email_profile, content_profile)
    explain = engine.explain(
        email=email_rule,
        content=content_rule,
        mode="dryrun",
        run_id=f"dryrun-{uuid.uuid4().hex[:10]}",
        notes=["No send, no persistence."],
    )

    content_runtime = to_content_runtime(content_rule)
    email_runtime = to_email_runtime(email_rule)
    tz_name = content_runtime.get("date_tz", "Asia/Shanghai")
    now = datetime.now(ZoneInfo(tz_name))

    return {
        "run_id": explain.run_id,
        "mode": explain.mode,
        "profile": {
            "email": explain.email_profile,
            "content": explain.content_profile,
        },
        "rules_version": {
            "email": explain.email_version,
            "content": explain.content_version,
        },
        "would_collect": {
            "at_time": now.isoformat(),
            "windows": content_runtime.get("windows", {}),
            "items": content_runtime.get("items", {}),
            "sources": content_runtime.get("sources", {}),
            "quality_gates": content_runtime.get("quality_gates", {}),
        },
        "would_send": {
            "transport": email_runtime.get("transport"),
            "subject_template": email_runtime.get("subject_template"),
            "retry": email_runtime.get("retry", {}),
            "fallback": email_runtime.get("fallback", {}),
        },
        "explain": {
            "notes": explain.notes,
        },
    }


def main(email_profile: str | None = None, content_profile: str | None = None) -> int:
    print(
        json.dumps(
            run_dryrun(email_profile=email_profile, content_profile=content_profile),
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0

