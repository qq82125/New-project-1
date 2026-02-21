from __future__ import annotations

import json
import os
import subprocess
import time
import uuid
import hashlib
from subprocess import CalledProcessError
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from app.adapters.rule_bridge import load_runtime_rules
from app.rules.engine import RuleEngine
from app.services.collect_asset_store import CollectAssetStore, render_digest_from_assets
from app.services.rules_store import RulesStore


def _resolve_env_template(value: str, env: dict[str, str]) -> str:
    """Resolve ${VAR} and ${VAR:-default} templates used in rules."""
    s = str(value or "").strip()
    if not s:
        return ""
    if s.startswith("${") and s.endswith("}"):
        inner = s[2:-1]
        if ":-" in inner:
            k, d = inner.split(":-", 1)
            return str(env.get(k.strip(), d)).strip()
        return str(env.get(inner.strip(), "")).strip()
    return s


def _utc_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            b = f.read(1024 * 1024)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def run_digest(
    *,
    profile: str,
    trigger: str,
    schedule_id: str,
    send: bool = True,
    report_date: str | None = None,
    collect_window_hours: int = 24,
    collect_asset_dir: str = "artifacts/collect",
    use_collect_assets: bool = False,
    project_root: Path | None = None,
) -> dict[str, Any]:
    """
    Live run: generate newsletter (network fetch) and optionally send email.

    This intentionally reuses existing scripts to avoid breaking behavior.
    """
    engine = RuleEngine(project_root=project_root) if project_root else RuleEngine()
    root = engine.project_root
    store = RulesStore(root)

    run_id = f"run-{uuid.uuid4().hex[:10]}"
    artifacts_dir = root / "artifacts" / run_id
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    started_at = time.time()
    status = "success"
    error_summary = ""
    analysis_meta: dict[str, Any] = {}

    # Build decision once for version recording & timezone.
    decision = engine.build_decision(profile=profile, run_id=run_id)
    rules_version = decision.get("rules_version", {})
    email_decision = decision.get("email_decision", {}) if isinstance(decision.get("email_decision"), dict) else {}
    tz_name = str((email_decision.get("schedule", {}) or {}).get("timezone", "Asia/Shanghai"))

    env = os.environ.copy()
    env["REPORT_TZ"] = tz_name
    if report_date:
        env["REPORT_DATE"] = report_date
    env["REPORT_RUN_ID"] = run_id
    env["DRYRUN_ARTIFACTS_DIR"] = str(artifacts_dir)
    if profile == "enhanced":
        env["ENHANCED_RULES_PROFILE"] = "enhanced"
    else:
        env.pop("ENHANCED_RULES_PROFILE", None)

    # Determine output file path consistent with existing conventions.
    if report_date:
        date_str = report_date
    else:
        date_str = datetime.now(ZoneInfo(tz_name)).strftime("%Y-%m-%d")
    out_dir = root / "reports"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"ivd_morning_{date_str}.txt"

    try:
        store.upsert_run_execution(
            run_id=run_id,
            profile=profile,
            triggered_by=trigger,
            window=str(report_date or ""),
            status="running",
            started_at=_utc_iso(),
            ended_at=None,
        )
        rt = load_runtime_rules(date_str=date_str, env=env, run_id=run_id)
        subject = str(rt.get("email", {}).get("subject") or f"全球IVD晨报 - {date_str}")
        if use_collect_assets:
            content_cfg = rt.get("content", {}) if isinstance(rt.get("content"), dict) else {}
            output_cfg = rt.get("output", {}) if isinstance(rt.get("output"), dict) else {}
            thresh = content_cfg.get("relevance_thresholds", {}) if isinstance(content_cfg.get("relevance_thresholds"), dict) else {}
            quota_cfg = content_cfg.get("frontier_quota", {}) if isinstance(content_cfg.get("frontier_quota"), dict) else {}
            analysis_cfg = content_cfg.get("analysis_cache", {}) if isinstance(content_cfg.get("analysis_cache"), dict) else {}
            source_policy = content_cfg.get("source_policy", {}) if isinstance(content_cfg.get("source_policy"), dict) else {}
            frontier_policy = content_cfg.get("frontier_policy", {}) if isinstance(content_cfg.get("frontier_policy"), dict) else {}
            evidence_policy = content_cfg.get("evidence_policy", {}) if isinstance(content_cfg.get("evidence_policy"), dict) else {}
            opportunity_index = content_cfg.get("opportunity_index", {}) if isinstance(content_cfg.get("opportunity_index"), dict) else {}
            collector = CollectAssetStore(root, asset_dir=collect_asset_dir)
            rows = collector.load_window_items(window_hours=int(collect_window_hours or 24))
            rendered = render_digest_from_assets(
                date_str=date_str,
                items=rows,
                subject=subject,
                core_min_level_for_A=int(thresh.get("core_min_level_for_A") or 3),
                frontier_min_level_for_F=int(thresh.get("frontier_min_level_for_F") or 2),
                frontier_quota=int(quota_cfg.get("max_items_per_day") or (output_cfg.get("E", {}) or {}).get("trends_count", 3)),
                analysis_cfg={
                    "enable_analysis_cache": bool(analysis_cfg.get("enable_analysis_cache", True)),
                    "always_generate": bool(analysis_cfg.get("always_generate", False)),
                    "prompt_version": str(analysis_cfg.get("prompt_version", "v2")),
                    "model": str(analysis_cfg.get("model", "local-heuristic-v1")),
                    "model_primary": str(analysis_cfg.get("model_primary", env.get("MODEL_PRIMARY", "local-heuristic-v1"))),
                    "model_fallback": str(analysis_cfg.get("model_fallback", env.get("MODEL_FALLBACK", "local-lite-v1"))),
                    "model_policy": str(analysis_cfg.get("model_policy", env.get("MODEL_POLICY", "tiered"))),
                    "core_model": str(analysis_cfg.get("core_model", "primary")),
                    "frontier_model": str(analysis_cfg.get("frontier_model", "fallback")),
                    "temperature": float(analysis_cfg.get("temperature", 0.2) or 0.2),
                    "retries": int(analysis_cfg.get("retries", 1) or 1),
                    "timeout_seconds": int(analysis_cfg.get("timeout_seconds", 20) or 20),
                    "backoff_seconds": float(analysis_cfg.get("backoff_seconds", 0.5) or 0.5),
                    "asset_dir": str(analysis_cfg.get("asset_dir", "artifacts/analysis")),
                    "source_policy": source_policy,
                    "profile": str(rt.get("active_profile", profile)),
                    "anchors_pack": content_cfg.get("anchors_pack", {}) if isinstance(content_cfg.get("anchors_pack"), dict) else {},
                    "negatives_pack": content_cfg.get("negatives_pack", []) if isinstance(content_cfg.get("negatives_pack"), list) else [],
                    "frontier_policy": frontier_policy,
                    "evidence_policy": evidence_policy,
                    "opportunity_index": opportunity_index,
                },
                return_meta=True,
            )
            if isinstance(rendered, dict):
                report_text = str(rendered.get("text", ""))
                analysis_meta = rendered.get("meta", {}) if isinstance(rendered.get("meta"), dict) else {}
            else:
                report_text = str(rendered)
            out_file.write_text(report_text, encoding="utf-8")
        else:
            proc = subprocess.run(
                ["python3", "scripts/generate_ivd_report.py"],
                cwd=root,
                env=env,
                check=True,
                capture_output=True,
                text=True,
            )
            if proc.stderr:
                print(proc.stderr, end="")
            out_file.write_text(proc.stdout, encoding="utf-8")

        sent = False
        fallback_triggered = False
        fallback_ok = False
        fallback_error = ""
        send_cmd: list[str] | None = None
        if send:
            to_email = env.get("TO_EMAIL", "")
            if not to_email:
                # Backward-compatible: if TO_EMAIL absent, pick first configured recipient or fail.
                rec = (rt.get("email", {}).get("recipients") or [])
                if isinstance(rec, list) and rec:
                    to_email = _resolve_env_template(str(rec[0]), env)
            if not to_email:
                raise RuntimeError("missing TO_EMAIL (and no recipients available)")
            send_cmd = ["./send_mail_icloud.sh", to_email, subject, str(out_file)]
            try:
                subprocess.run(send_cmd, cwd=root, env=env, check=True)
                sent = True
            except CalledProcessError as se:
                try:
                    store.record_source_fetch_event(
                        run_id=run_id,
                        source_id="mail_send",
                        status="failed",
                        http_status=None,
                        items_count=0,
                        error=f"send_mail_icloud_failed: {se}",
                        duration_ms=0,
                    )
                except Exception:
                    pass
                # Non-breaking fallback: invoke cloud backup sender for visibility/idempotent retry path.
                fallback_triggered = True
                fb_env = env.copy()
                fb_env["REPORT_DATE"] = date_str
                # Prefix is used by cloud backup sender to build the final subject.
                fb_env.setdefault("REPORT_SUBJECT_PREFIX", "全球IVD晨报 - ")
                try:
                    subprocess.run(
                        ["python3", "scripts/cloud_backup_send.py"],
                        cwd=root,
                        env=fb_env,
                        check=True,
                        capture_output=True,
                        text=True,
                    )
                    fallback_ok = True
                except Exception as fe:
                    fallback_ok = False
                    fallback_error = str(fe)
                # Keep original failure visible to scheduler/run status.
                raise RuntimeError(
                    f"primary_send_failed={se}; fallback_triggered={fallback_triggered}; "
                    f"fallback_ok={fallback_ok}; fallback_error={fallback_error}"
                ) from se

        return_payload = {
            "ok": True,
            "run_id": run_id,
            "mode": "live",
            "trigger": trigger,
            "schedule_id": schedule_id,
            "profile": profile,
            "date": date_str,
            "rules_version": rules_version,
            "artifacts_dir": str(artifacts_dir),
            "output_file": str(out_file),
            "use_collect_assets": bool(use_collect_assets),
            "collect_window_hours": int(collect_window_hours or 24),
            "collect_asset_dir": str(collect_asset_dir),
            "analysis_meta": analysis_meta,
            "sent": sent,
            "send_cmd": send_cmd,
            "fallback_triggered": fallback_triggered,
            "fallback_ok": fallback_ok,
            "fallback_error": fallback_error,
        }
        try:
            store.record_report_artifact(
                run_id=run_id,
                artifact_path=str(out_file),
                artifact_type="report_text",
                sha256=_sha256_file(out_file),
                created_at=_utc_iso(),
            )
        except Exception:
            pass
    except Exception as e:
        status = "failed"
        error_summary = str(e)
        return_payload = {
            "ok": False,
            "run_id": run_id,
            "mode": "live",
            "trigger": trigger,
            "schedule_id": schedule_id,
            "profile": profile,
            "rules_version": rules_version,
            "artifacts_dir": str(artifacts_dir),
            "status": status,
            "error_summary": error_summary,
        }
    finally:
        finished_at = time.time()
        ended_at = _utc_iso()
        meta = {
            "run_id": run_id,
            "trigger": trigger,
            "schedule_id": schedule_id,
            "profile": profile,
            "rules_version": rules_version,
            "status": status,
            "error_summary": error_summary,
            "started_at": _utc_iso(),
            "duration_ms": int((finished_at - started_at) * 1000),
            "analysis": analysis_meta,
        }
        (artifacts_dir / "run_meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
        try:
            store.record_report_artifact(
                run_id=run_id,
                artifact_path=str(artifacts_dir / "run_meta.json"),
                artifact_type="run_meta",
                sha256=_sha256_file(artifacts_dir / "run_meta.json"),
                created_at=ended_at,
            )
        except Exception:
            pass
        try:
            store.finish_run_execution(
                run_id=run_id,
                status="success" if status == "success" else "failed",
                ended_at=ended_at,
            )
        except Exception:
            pass

    return return_payload
