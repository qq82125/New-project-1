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


def _parse_send_times(schedule: dict[str, Any]) -> list[tuple[int, int]]:
    out: list[tuple[int, int]] = []
    raw = schedule.get("send_times")
    if isinstance(raw, list):
        for it in raw:
            s = str(it or "").strip()
            if not s:
                continue
            try:
                h, m = s.split(":", 1)
                hh = int(h)
                mm = int(m)
                if 0 <= hh <= 23 and 0 <= mm <= 59:
                    out.append((hh, mm))
            except Exception:
                continue
    if not out:
        hh_raw = schedule.get("hour", 8)
        mm_raw = schedule.get("minute", 30)
        hh = int(8 if hh_raw is None else hh_raw)
        mm = int(30 if mm_raw is None else mm_raw)
        out = [(hh, mm)]
    out = sorted(list(dict.fromkeys(out)))
    return out


def _compute_segment_window(
    *,
    now_local: datetime,
    send_times: list[tuple[int, int]],
) -> tuple[datetime, datetime]:
    # Build boundaries across previous day / today / next day then pick latest <= now.
    candidates: list[datetime] = []
    for d_shift in (-1, 0, 1):
        d = now_local.date().fromordinal(now_local.date().toordinal() + d_shift)
        for hh, mm in send_times:
            candidates.append(now_local.replace(year=d.year, month=d.month, day=d.day, hour=hh, minute=mm, second=0, microsecond=0))
    candidates.sort()
    latest_idx = 0
    for i, t in enumerate(candidates):
        if t <= now_local:
            latest_idx = i
    end_local = candidates[latest_idx]
    start_local = candidates[max(0, latest_idx - 1)]
    if start_local >= end_local:
        # Defensive fallback to 12h split.
        start_local = end_local.replace(hour=max(0, end_local.hour - 12))
    return start_local, end_local


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
    send_error_summary = ""

    # Build decision once for version recording & timezone.
    decision = engine.build_decision(profile=profile, run_id=run_id)
    rules_version = decision.get("rules_version", {})
    email_decision = decision.get("email_decision", {}) if isinstance(decision.get("email_decision"), dict) else {}
    schedule_cfg = (email_decision.get("schedule", {}) or {}) if isinstance(email_decision, dict) else {}
    tz_name = str(schedule_cfg.get("timezone", "Asia/Shanghai"))

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
        rt_schedule = (rt.get("email", {}).get("schedule", {}) or {}) if isinstance(rt.get("email", {}), dict) else {}
        send_times = _parse_send_times(rt_schedule if isinstance(rt_schedule, dict) else {})
        window_mode = str((rt_schedule.get("window_mode", "rolling") if isinstance(rt_schedule, dict) else "rolling") or "rolling").strip().lower()
        window_start_utc = None
        window_end_utc = None
        resolved_collect_window_hours = int(collect_window_hours or 24)
        edition = "default"
        now_local = datetime.now(ZoneInfo(tz_name))
        if window_mode == "segmented" and send_times:
            start_local, end_local = _compute_segment_window(now_local=now_local, send_times=send_times)
            window_start_utc = start_local.astimezone(ZoneInfo("UTC"))
            window_end_utc = end_local.astimezone(ZoneInfo("UTC"))
            span_h = max(1, int((window_end_utc - window_start_utc).total_seconds() // 3600))
            resolved_collect_window_hours = span_h
            if send_times:
                first_slot = send_times[0]
                edition = "morning" if (end_local.hour, end_local.minute) == first_slot else "evening"
        else:
            window_mode = "rolling"
        if use_collect_assets:
            content_cfg = rt.get("content", {}) if isinstance(rt.get("content"), dict) else {}
            output_cfg = rt.get("output", {}) if isinstance(rt.get("output"), dict) else {}
            thresh = content_cfg.get("relevance_thresholds", {}) if isinstance(content_cfg.get("relevance_thresholds"), dict) else {}
            quota_cfg = content_cfg.get("frontier_quota", {}) if isinstance(content_cfg.get("frontier_quota"), dict) else {}
            analysis_cfg = content_cfg.get("analysis_cache", {}) if isinstance(content_cfg.get("analysis_cache"), dict) else {}
            collector = CollectAssetStore(root, asset_dir=collect_asset_dir)
            rows = collector.load_window_items(
                window_hours=resolved_collect_window_hours,
                window_start_utc=window_start_utc,
                window_end_utc=window_end_utc,
            )
            rendered = render_digest_from_assets(
                date_str=date_str,
                items=rows,
                subject=subject,
                core_min_level_for_A=int(thresh.get("core_min_level_for_A") or 3),
                frontier_min_level_for_F=int(thresh.get("frontier_min_level_for_F") or 2),
                frontier_quota=int(quota_cfg.get("max_items_per_day") or (output_cfg.get("E", {}) or {}).get("trends_count", 3)),
                analysis_cfg={
                    "profile": str(profile or "legacy"),
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
                    "source_policy": content_cfg.get("source_policy", {}),
                    "source_guard": content_cfg.get("source_guard", {}),
                    "frontier_policy": content_cfg.get("frontier_policy", {}),
                    "evidence_policy": content_cfg.get("evidence_policy", {}),
                    "opportunity_index": content_cfg.get("opportunity_index", {}),
                    "anchors_pack": content_cfg.get("anchors_pack", {}),
                    "negatives_pack": content_cfg.get("negatives_pack", []),
                    "style": output_cfg.get("style", {}) if isinstance(output_cfg.get("style"), dict) else {},
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
        send_failure_nonfatal = False
        send_failure_fatal = str(env.get("MAIL_SEND_STRICT", "0")).strip().lower() in {"1", "true", "yes", "on"}
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
                # Drill switch: simulate primary send failure while keeping fallback path intact.
                if str(env.get("MAIL_SEND_FORCE_FAIL", "0")).strip().lower() in {"1", "true", "yes", "on"}:
                    raise CalledProcessError(
                        returncode=99,
                        cmd=send_cmd,
                        output="MAIL_SEND_FORCE_FAIL enabled",
                    )
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
                send_error_summary = (
                    f"primary_send_failed={se}; fallback_triggered={fallback_triggered}; "
                    f"fallback_ok={fallback_ok}; fallback_error={fallback_error}"
                )
                if send_failure_fatal:
                    # Strict mode: preserve original fatal behavior.
                    raise RuntimeError(send_error_summary) from se
                send_failure_nonfatal = True

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
            "collect_window_hours": int(resolved_collect_window_hours),
            "window_mode": window_mode,
            "window_start_utc": window_start_utc.isoformat().replace("+00:00", "Z") if window_start_utc else "",
            "window_end_utc": window_end_utc.isoformat().replace("+00:00", "Z") if window_end_utc else "",
            "edition": edition,
            "collect_asset_dir": str(collect_asset_dir),
            "analysis_meta": analysis_meta,
            "sent": sent,
            "send_cmd": send_cmd,
            "fallback_triggered": fallback_triggered,
            "fallback_ok": fallback_ok,
            "fallback_error": fallback_error,
            "send_error_summary": send_error_summary,
            "send_failure_nonfatal": send_failure_nonfatal,
            "mail_send_strict": send_failure_fatal,
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
            "send_error_summary": send_error_summary,
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
