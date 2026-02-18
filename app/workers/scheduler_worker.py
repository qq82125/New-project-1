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
        # Poll cadence to support "immediate" admin actions without running APScheduler in FastAPI.
        self.tick_seconds = int(os.environ.get("SCHEDULER_TICK_SECONDS", "5") or "5")
        self.lock_path = self.project_root / "data" / "ivd_digest.lock"
        self.heartbeat_path = self.project_root / "logs" / "scheduler_worker_heartbeat.json"
        self.status_path = self.project_root / "logs" / "scheduler_worker_status.json"
        self.reload_signal_path = self.project_root / "data" / "scheduler_reload.signal"
        self.commands_dir = self.project_root / "data" / "scheduler_commands"
        self._active_version = ""
        self._enabled = False
        self._paused = False
        self._last_reload_mtime = 0.0

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
            "paused": self._paused,
            "jobs": len(self.scheduler.get_jobs()) if self.scheduler else 0,
        }
        self.heartbeat_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def _write_status(self) -> None:
        self.status_path.parent.mkdir(parents=True, exist_ok=True)
        jobs = []
        try:
            for j in self.scheduler.get_jobs():
                nrt = getattr(j, "next_run_time", None)
                jobs.append(
                    {
                        "id": str(getattr(j, "id", "")),
                        "name": str(getattr(j, "name", "")),
                        "next_run_time": nrt.isoformat() if nrt else None,
                    }
                )
        except Exception:
            jobs = []
        payload = {
            "ts": time.time(),
            "profile": self.profile,
            "active_version": self._active_version,
            "enabled": self._enabled,
            "paused": self._paused,
            "jobs": jobs,
        }
        self.status_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def _check_reload_signal(self) -> bool:
        try:
            if not self.reload_signal_path.exists():
                return False
            mt = float(self.reload_signal_path.stat().st_mtime)
            if mt > self._last_reload_mtime:
                self._last_reload_mtime = mt
                return True
        except Exception:
            return False
        return False

    def _drain_commands(self) -> None:
        """
        Commands are small JSON files written by admin-api under data/scheduler_commands/.

        Supported:
        - {"cmd":"trigger","purpose":"collect"|"digest","profile":"enhanced"}
        """
        self.commands_dir.mkdir(parents=True, exist_ok=True)
        files = sorted([p for p in self.commands_dir.glob("*.json") if p.is_file()], key=lambda p: p.name)
        if not files:
            return
        done_dir = self.commands_dir / "done"
        done_dir.mkdir(parents=True, exist_ok=True)
        for p in files[:5]:
            try:
                raw = json.loads(p.read_text(encoding="utf-8"))
                cmd = str(raw.get("cmd", "")).strip()
                if cmd == "trigger":
                    purpose = str(raw.get("purpose", "collect")).strip() or "collect"
                    prof = str(raw.get("profile", self.profile)).strip() or self.profile
                    schedule_id = str(raw.get("schedule_id", "manual")).strip() or "manual"
                    _log(f"manual_trigger cmd_file={p.name} purpose={purpose} profile={prof}")
                    self._run_job(schedule_id=schedule_id, purpose=purpose, profile=prof, jitter=0, misfire_grace=600, trigger="manual")
            except Exception as e:
                _log(f"command_failed file={p.name} error={e}")
            finally:
                try:
                    p.rename(done_dir / p.name)
                except Exception:
                    try:
                        p.unlink(missing_ok=True)  # type: ignore[arg-type]
                    except Exception:
                        pass

    def _run_job(
        self,
        *,
        schedule_id: str,
        purpose: str,
        profile: str,
        jitter: int,
        misfire_grace: int,
        trigger: str = "schedule",
    ) -> None:
        if self._paused:
            _log(f"job_skipped_paused schedule_id={schedule_id} purpose={purpose} profile={profile}")
            return
        if jitter:
            time.sleep(random.randint(0, int(jitter)))

        # Secondary guard: prevent duplicate digest runs across triggers/processes.
        run_id = f"{purpose}-{int(time.time())}"
        try:
            with acquire_run_lock(self.lock_path, run_id=run_id, purpose=purpose):
                if purpose == "collect":
                    out = self._run_collect(schedule_id=schedule_id, profile=profile, trigger=trigger)
                else:
                    out = run_digest(
                        profile=profile,
                        trigger=trigger,
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

    def _run_collect(self, *, schedule_id: str, profile: str, trigger: str = "schedule") -> dict[str, Any]:
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
            "trigger": trigger,
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
        max_instances = int(conc.get("max_instances") or 1)
        coalesce = bool(conc.get("coalesce", True))
        policies = defaults.get("run_policies", {}) if isinstance(defaults.get("run_policies"), dict) else {}
        # pause_switch is treated as a state switch here: true => paused.
        self._paused = bool(policies.get("pause_switch", False))

        specs = build_job_specs(defaults)

        # Reset jobs on any config change.
        self.scheduler.remove_all_jobs()
        self._enabled = enabled
        if not enabled:
            _log("scheduler disabled (no jobs scheduled)")
            return
        if self._paused:
            _log("scheduler paused (no jobs scheduled)")
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
                    "trigger": "schedule",
                },
                max_instances=max_instances,
                coalesce=coalesce,
                misfire_grace_time=misfire,
                replace_existing=True,
            )
        _log(f"scheduled_jobs={len(specs)} tz={tz} misfire_grace={misfire}s")

    def refresh(self) -> None:
        cfg, source, version = _active_scheduler_cfg(self.store, self.project_root, self.profile)
        if version == self._active_version:
            # Still update heartbeat to prove liveness.
            self._write_heartbeat()
            self._write_status()
            return
        self._active_version = version
        _log(f"config_change source={source} profile={self.profile} version={version}")
        self._apply_config(cfg)
        self._write_heartbeat()
        self._write_status()

    def run_forever(self) -> None:
        _log(f"start profile={self.profile} refresh_seconds={self.refresh_seconds}")
        self.refresh()
        self.scheduler.start()
        self._write_heartbeat()
        try:
            last_refresh = 0.0
            while True:
                time.sleep(max(1, self.tick_seconds))
                # Commands are processed quickly to feel "immediate" from UI.
                self._drain_commands()
                # Allow admin-api to request immediate reload via signal file.
                if self._check_reload_signal():
                    _log("reload_signal detected; refreshing config now")
                    self.refresh()
                    last_refresh = time.time()
                # Periodic refresh in case DB active version changes without signal.
                if time.time() - last_refresh >= float(self.refresh_seconds):
                    self.refresh()
                    last_refresh = time.time()
                else:
                    # still write status/heartbeat periodically
                    self._write_heartbeat()
                    self._write_status()
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
