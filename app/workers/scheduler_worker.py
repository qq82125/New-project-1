from __future__ import annotations

import json
import os
import random
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import yaml

from app.services.run_lock import RunLockError, acquire_run_lock
from app.services.rules_store import RulesStore
from app.services.source_registry import test_source
from app.workers.live_run import run_digest


def _log(msg: str) -> None:
    print(f"[SCHED] {msg}", file=sys.stderr, flush=True)


def _load_yaml(path: Path) -> dict[str, Any]:
    obj = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise RuntimeError(f"invalid yaml: {path}")
    return obj


def _load_schema(project_root: Path) -> dict[str, Any]:
    # Prefer workspace schema (published), then repo fallback.
    candidates = [
        project_root / "rules" / "console" / "versions",
        project_root / "rules" / "schemas" / "scheduler_rules.schema.json",
    ]
    # Workspace published schema path isn't stable here; use RuleEngine in later prompts.
    # For now, repo schema is sufficient for sanity checks.
    schema_path = project_root / "rules" / "schemas" / "scheduler_rules.schema.json"
    return json.loads(schema_path.read_text(encoding="utf-8"))


def _extract_defaults(cfg: dict[str, Any]) -> dict[str, Any]:
    d = cfg.get("defaults", {})
    return d if isinstance(d, dict) else {}


def _active_scheduler_cfg(store: RulesStore, project_root: Path, profile: str) -> tuple[dict[str, Any], str, str]:
    """
    Returns: (config, source, version)
    """
    db = store.get_active_rules("scheduler_rules", profile)
    if isinstance(db, dict) and db.get("ruleset") == "scheduler_rules":
        meta = db.get("_store_meta", {}) if isinstance(db.get("_store_meta"), dict) else {}
        return db, "db", str(meta.get("version", db.get("version", "")))

    # Fallback to repo rules.
    p = project_root / "rules" / "scheduler_rules" / f"{profile}.yaml"
    cfg = _load_yaml(p)
    return cfg, "file", str(cfg.get("version", ""))


@dataclass(frozen=True)
class JobSpec:
    id: str
    type: str  # cron|interval
    cron: str | None
    interval_minutes: int | None
    purpose: str  # collect|digest
    profile: str
    jitter_seconds: int


def build_job_specs(defaults: dict[str, Any]) -> list[JobSpec]:
    specs: list[JobSpec] = []
    for raw in defaults.get("schedules", []) if isinstance(defaults.get("schedules"), list) else []:
        if not isinstance(raw, dict):
            continue
        specs.append(
            JobSpec(
                id=str(raw.get("id", "")),
                type=str(raw.get("type", "")),
                cron=str(raw.get("cron")) if raw.get("cron") is not None else None,
                interval_minutes=int(raw.get("interval_minutes")) if raw.get("interval_minutes") is not None else None,
                purpose=str(raw.get("purpose", "")),
                profile=str(raw.get("profile", "")),
                jitter_seconds=int(raw.get("jitter_seconds") or 0),
            )
        )
    return [s for s in specs if s.id and s.type in ("cron", "interval") and s.purpose in ("collect", "digest")]


class SchedulerWorker:
    def __init__(self, project_root: Path | None = None) -> None:
        self.project_root = project_root or Path(__file__).resolve().parents[2]
        self.store = RulesStore(self.project_root)
        self.profile = os.environ.get("SCHEDULER_PROFILE", "enhanced").strip() or "enhanced"
        self.refresh_seconds = int(os.environ.get("SCHEDULER_REFRESH_SECONDS", "60") or "60")
        self.lock_path = self.project_root / "data" / "ivd_digest.lock"
        self.heartbeat_path = self.project_root / "logs" / "scheduler_worker_heartbeat.json"
        self._active_version = ""
        self._enabled = False

        try:
            from apscheduler.schedulers.background import BackgroundScheduler  # type: ignore
            from apscheduler.triggers.cron import CronTrigger  # type: ignore
            from apscheduler.triggers.interval import IntervalTrigger  # type: ignore
        except Exception as e:
            raise SystemExit(
                "APScheduler is required but not installed. Install with: pip install apscheduler\n"
                f"import_error={e}"
            )

        self.BackgroundScheduler = BackgroundScheduler
        self.CronTrigger = CronTrigger
        self.IntervalTrigger = IntervalTrigger

        self.scheduler = self.BackgroundScheduler(
            job_defaults={
                "max_instances": 1,
                "coalesce": True,
                # misfire_grace_time is set per job from rules.
            }
        )

    def _write_heartbeat(self) -> None:
        self.heartbeat_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "ts": time.time(),
            "profile": self.profile,
            "active_version": self._active_version,
            "enabled": self._enabled,
            "jobs": len(self.scheduler.get_jobs()) if self.scheduler else 0,
        }
        self.heartbeat_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def _run_job(self, *, schedule_id: str, purpose: str, profile: str, jitter: int, misfire_grace: int) -> None:
        if jitter:
            time.sleep(random.randint(0, int(jitter)))

        # Secondary guard: prevent duplicate digest runs across triggers/processes.
        run_id = f"{purpose}-{int(time.time())}"
        try:
            with acquire_run_lock(self.lock_path, run_id=run_id, purpose=purpose):
                if purpose == "collect":
                    out = self._run_collect(schedule_id=schedule_id, profile=profile)
                else:
                    out = run_digest(
                        profile=profile,
                        trigger="schedule",
                        schedule_id=schedule_id,
                        send=True,
                        project_root=self.project_root,
                    )
                _log(
                    f"job_done schedule_id={schedule_id} purpose={purpose} ok={out.get('ok')} run_id={out.get('run_id')}"
                )
        except RunLockError as e:
            _log(f"job_skipped_locked schedule_id={schedule_id} purpose={purpose} error={e}")
        except Exception as e:
            _log(f"job_failed schedule_id={schedule_id} purpose={purpose} error={e}")

    def _run_collect(self, *, schedule_id: str, profile: str) -> dict[str, Any]:
        """
        Per-source collection loop with min-interval gating.

        - Reads enabled sources from DB.
        - For each source, checks last_fetched_at against fetch.interval_minutes.
        - Only due sources are fetched; skipped sources are recorded.
        """
        run_id = f"collect-{int(time.time())}"
        artifacts_dir = self.project_root / "artifacts" / run_id
        artifacts_dir.mkdir(parents=True, exist_ok=True)

        now = time.time()
        fetched = 0
        skipped = 0
        failed = 0

        rows = self.store.list_sources(enabled_only=True)
        for s in rows:
            sid = str(s.get("id", "")).strip()
            if not sid:
                continue
            fetch_cfg = s.get("fetch", {}) if isinstance(s.get("fetch"), dict) else {}
            interval_m = fetch_cfg.get("interval_minutes")
            try:
                interval_min = int(interval_m) if interval_m is not None and str(interval_m).strip() != "" else 0
            except Exception:
                interval_min = 0

            last_fetched_at = str(s.get("last_fetched_at") or "").strip()
            due = True
            if interval_min > 0 and last_fetched_at:
                try:
                    # stored as isoformat from _utc_now (timezone aware)
                    from datetime import datetime

                    dt_last = datetime.fromisoformat(last_fetched_at.replace("Z", "+00:00"))
                    age = now - dt_last.timestamp()
                    due = age >= interval_min * 60
                except Exception:
                    due = True

            if not due:
                skipped += 1
                # record as skipped (keep last_fetched_at unchanged)
                # Keep last_fetched_at unchanged; only update status field.
                try:
                    self.store.record_source_fetch(sid, status="skipped", http_status=None, error=None)
                except Exception:
                    pass
                continue

            result = test_source(s, limit=3)
            ok = bool(result.get("ok"))
            status = "ok" if ok else "fail"
            http_status = result.get("http_status")
            err = result.get("error")
            try:
                self.store.record_source_fetch(
                    sid,
                    status=status,
                    http_status=int(http_status) if http_status is not None else None,
                    error=str(err or "") if not ok else None,
                )
            except Exception:
                pass

            if ok:
                fetched += 1
            else:
                failed += 1

        meta = {
            "run_id": run_id,
            "trigger": "schedule",
            "schedule_id": schedule_id,
            "profile": profile,
            "purpose": "collect",
            "ts": time.time(),
            "counts": {"enabled_sources": len(rows), "fetched": fetched, "skipped": skipped, "failed": failed},
        }
        (artifacts_dir / "run_meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
        return {"ok": True, "run_id": run_id, "counts": meta["counts"], "artifacts_dir": str(artifacts_dir)}

    def _apply_config(self, cfg: dict[str, Any]) -> None:
        defaults = _extract_defaults(cfg)
        enabled = bool(defaults.get("enabled", False))
        tz = str(defaults.get("timezone", "Asia/Singapore"))
        conc = defaults.get("concurrency", {}) if isinstance(defaults.get("concurrency"), dict) else {}
        misfire = int(conc.get("misfire_grace_seconds") or 600)

        specs = build_job_specs(defaults)

        # Reset jobs on any config change.
        self.scheduler.remove_all_jobs()
        self._enabled = enabled
        if not enabled:
            _log("scheduler disabled (no jobs scheduled)")
            return

        for s in specs:
            if s.type == "cron":
                trig = self.CronTrigger.from_crontab(str(s.cron), timezone=tz)
            else:
                trig = self.IntervalTrigger(minutes=int(s.interval_minutes or 60), timezone=tz)

            self.scheduler.add_job(
                self._run_job,
                trigger=trig,
                id=str(s.id),
                kwargs={
                    "schedule_id": s.id,
                    "purpose": s.purpose,
                    "profile": s.profile or self.profile,
                    "jitter": int(s.jitter_seconds or 0),
                    "misfire_grace": misfire,
                },
                max_instances=1,
                coalesce=True,
                misfire_grace_time=misfire,
                replace_existing=True,
            )
        _log(f"scheduled_jobs={len(specs)} tz={tz} misfire_grace={misfire}s")

    def refresh(self) -> None:
        cfg, source, version = _active_scheduler_cfg(self.store, self.project_root, self.profile)
        if version == self._active_version:
            # Still update heartbeat to prove liveness.
            self._write_heartbeat()
            return
        self._active_version = version
        _log(f"config_change source={source} profile={self.profile} version={version}")
        self._apply_config(cfg)
        self._write_heartbeat()

    def run_forever(self) -> None:
        _log(f"start profile={self.profile} refresh_seconds={self.refresh_seconds}")
        self.refresh()
        self.scheduler.start()
        self._write_heartbeat()
        try:
            while True:
                time.sleep(max(5, self.refresh_seconds))
                self.refresh()
        except KeyboardInterrupt:
            _log("stop (keyboard interrupt)")
        finally:
            try:
                self.scheduler.shutdown(wait=False)
            except Exception:
                pass


def main() -> None:
    worker = SchedulerWorker()
    worker.run_forever()


if __name__ == "__main__":
    main()
