from __future__ import annotations

from typing import Any

from app.rules.models import RuleSelection


def to_email_runtime(rule: RuleSelection) -> dict[str, Any]:
    delivery = rule.data.get("delivery", {})
    retry = rule.data.get("retry", {})
    fallback = rule.data.get("fallback", {})
    flags = rule.data.get("feature_flags", {})
    return {
        "profile": rule.profile,
        "rules_version": rule.version,
        "date_tz": delivery.get("date_tz", "Asia/Shanghai"),
        "subject_template": delivery.get("subject_template", "全球IVD晨报 - {{date}}"),
        "transport": delivery.get("transport", "icloud_smtp"),
        "retry": {
            "connect_timeout_sec": retry.get("connect_timeout_sec", 10),
            "max_time_sec": retry.get("max_time_sec", 60),
            "max_retries": retry.get("max_retries", 5),
            "backoff_initial_sec": retry.get("backoff_initial_sec", 2),
            "backoff_max_sec": retry.get("backoff_max_sec", 60),
        },
        "fallback": {
            "cloud_backup_enabled": bool(fallback.get("cloud_backup_enabled", True)),
            "cloud_backup_check_sent_mail": bool(
                fallback.get("cloud_backup_check_sent_mail", True)
            ),
        },
        "feature_flags": flags,
    }

