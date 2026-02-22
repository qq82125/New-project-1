from __future__ import annotations

import json
import os
import random
import sys
import time
import hashlib
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import yaml

from app.adapters.rule_bridge import load_runtime_rules
from app.services.run_lock import RunLockError, acquire_run_lock
from app.services.rules_store import RulesStore
from app.services.source_policy import exclusion_reason, filter_entries_for_collect, normalize_source_policy, source_passes_min_trust_tier
from app.services.source_registry import fetch_source_entries, list_sources_for_profile
from app.workers.live_run import run_digest
from app.services.collect_asset_store import CollectAssetStore


def _log(msg: str) -> None:
    print(f"[SCHED] {msg}", file=sys.stderr, flush=True)


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            b = f.read(1024 * 1024)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


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


def _is_due(*, last_fetched_at: str, interval_min: int, now_ts: float) -> bool:
    if int(interval_min or 0) <= 0:
        return True
    lf = str(last_fetched_at or "").strip()
    if not lf:
        return True
    try:
        dt_last = datetime.fromisoformat(lf.replace("Z", "+00:00"))
        age = now_ts - dt_last.timestamp()
        return age >= int(interval_min) * 60
    except Exception:
        return True


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
        self._collect_window_hours = 24
        self._collect_asset_dir = "artifacts/collect"
        self._last_reload_mtime = 0.0

        try:
            from apscheduler.schedulers.background import BackgroundScheduler  # type: ignore
            from apscheduler.triggers.cron import CronTrigger  # type: ignore
            from apscheduler.triggers.interval import IntervalTrigger  # type: ignore
        except Exception as e:
            raise SystemExit(
                "APScheduler is required but not installed. Install with: pip install -r requirements.txt\n"
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
                        collect_window_hours=self._collect_window_hours,
                        collect_asset_dir=self._collect_asset_dir,
                        use_collect_assets=True,
                        project_root=self.project_root,
                    )
                _log(
                    f"job_done schedule_id={schedule_id} purpose={purpose} ok={out.get('ok')} run_id={out.get('run_id')}"
                )
        except RunLockError as e:
            _log(f"job_skipped_locked schedule_id={schedule_id} purpose={purpose} error={e}")
        except Exception as e:
            _log(f"job_failed schedule_id={schedule_id} purpose={purpose} error={e}")

    def _run_collect(
        self,
        *,
        schedule_id: str,
        profile: str,
        trigger: str = "schedule",
        max_sources: int | None = None,
        force: bool = False,
        fetch_limit: int = 50,
    ) -> dict[str, Any]:
        """
        Per-source collection loop with min-interval gating.

        - Reads enabled sources from DB.
        - For each source, checks last_fetched_at against fetch.interval_minutes.
        - Only due sources are fetched; skipped sources are recorded.
        """
        run_id = f"collect-{int(time.time())}"
        artifacts_dir = self.project_root / "artifacts" / run_id
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        started_iso = datetime.now(timezone.utc).isoformat()
        try:
            self.store.upsert_run_execution(
                run_id=run_id,
                profile=profile,
                triggered_by=trigger,
                window="collect",
                status="running",
                started_at=started_iso,
                ended_at=None,
            )
        except Exception:
            pass

        now = time.time()
        fetched = 0
        skipped = 0
        failed = 0
        assets_written = 0
        deduped_count = 0
        errors: list[str] = []
        sources_fetched_count = 0
        sources_failed_count = 0
        collector = CollectAssetStore(self.project_root, asset_dir=self._collect_asset_dir)
        env_for_rules = os.environ.copy()
        if str(profile).strip().lower() == "enhanced":
            env_for_rules["ENHANCED_RULES_PROFILE"] = "enhanced"
        else:
            env_for_rules.pop("ENHANCED_RULES_PROFILE", None)
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        rt = load_runtime_rules(date_str=date_str, env=env_for_rules, run_id=run_id)
        content_cfg = rt.get("content", {}) if isinstance(rt.get("content"), dict) else {}
        source_policy = normalize_source_policy(
            content_cfg.get("source_policy", {}) if isinstance(content_cfg.get("source_policy"), dict) else {},
            profile=str(rt.get("active_profile", profile)),
        )
        source_guard_cfg_raw = content_cfg.get("source_guard", {}) if isinstance(content_cfg.get("source_guard"), dict) else {}
        source_guard = {
            "enabled": bool(source_guard_cfg_raw.get("enabled", str(rt.get("active_profile", profile)).strip().lower() == "enhanced")),
            "enforce_article_only": bool(source_guard_cfg_raw.get("enforce_article_only", True)),
            "article_min_paragraphs": int(source_guard_cfg_raw.get("article_min_paragraphs", 2) or 2),
            "article_min_text_chars": int(source_guard_cfg_raw.get("article_min_text_chars", 200) or 200),
            "allow_body_fetch_for_rss": bool(source_guard_cfg_raw.get("allow_body_fetch_for_rss", False)),
        }
        dropped_by_source_policy_count = 0
        dropped_by_source_policy_reasons: dict[str, int] = {}
        dropped_static_or_listing_count = 0
        dropped_too_short_count = 0
        dropped_static_or_listing_domains: dict[str, int] = {}
        interval_defaulted_count = 0
        sources_attempted: list[str] = []
        sources_written_counts: dict[str, int] = {}
        sources_dropped_counts: dict[str, dict[str, int]] = {}
        sources_fetch_errors: dict[str, str] = {}

        rows = list_sources_for_profile(self.project_root, profile)
        rows = [r for r in rows if bool(r.get("enabled", True))]
        if isinstance(max_sources, int) and max_sources > 0:
            rows = rows[: int(max_sources)]
        for s in rows:
            sid = str(s.get("id", "")).strip()
            if not sid:
                continue
            sources_attempted.append(sid)
            source_url = str(s.get("url", "")).strip()
            source_tt = str(s.get("trust_tier", "C")).strip().upper() or "C"
            if not source_passes_min_trust_tier(source_tt, source_policy):
                skipped += 1
                dropped_by_source_policy_count += 1
                dropped_by_source_policy_reasons["below_min_trust_tier"] = (
                    dropped_by_source_policy_reasons.get("below_min_trust_tier", 0) + 1
                )
                dropped = sources_dropped_counts.setdefault(sid, {})
                dropped["below_min_trust_tier"] = dropped.get("below_min_trust_tier", 0) + 1
                continue
            rs = exclusion_reason(sid, source_url, source_policy)
            if rs:
                skipped += 1
                dropped_by_source_policy_count += 1
                dropped_by_source_policy_reasons[rs] = dropped_by_source_policy_reasons.get(rs, 0) + 1
                dropped = sources_dropped_counts.setdefault(sid, {})
                dropped[rs] = dropped.get(rs, 0) + 1
                continue
            t0 = time.time()
            fetch_cfg = s.get("fetch", {}) if isinstance(s.get("fetch"), dict) else {}
            interval_m = s.get("effective_interval_minutes", fetch_cfg.get("interval_minutes"))
            try:
                interval_min = int(interval_m) if interval_m is not None and str(interval_m).strip() != "" else 0
            except Exception:
                interval_min = 0
            if interval_min <= 0:
                interval_min = 60
                interval_defaulted_count += 1
                errors.append(f"{sid}:interval_defaulted_to_60")
                dropped = sources_dropped_counts.setdefault(sid, {})
                dropped["interval_defaulted_to_60"] = dropped.get("interval_defaulted_to_60", 0) + 1

            last_fetched_at = str(s.get("last_fetched_at") or "").strip()
            due = _is_due(last_fetched_at=last_fetched_at, interval_min=interval_min, now_ts=now)

            if not force and not due:
                skipped += 1
                dropped = sources_dropped_counts.setdefault(sid, {})
                dropped["not_due"] = dropped.get("not_due", 0) + 1
                # not_due should not overwrite the last effective fetch status.
                # We only record an event for auditing so UI can continue to show
                # the most recent real fetch result (ok/fail) instead of "skipped".
                try:
                    self.store.record_source_fetch_event(
                        run_id=run_id,
                        source_id=sid,
                        status="skipped",
                        http_status=None,
                        items_count=0,
                        error=None,
                        duration_ms=int((time.time() - t0) * 1000),
                    )
                except Exception:
                    pass
                continue

            timeout_s = 20
            retries = 1
            try:
                if isinstance(fetch_cfg, dict):
                    timeout_s = int(fetch_cfg.get("timeout_seconds") or 20)
                    retries = int(fetch_cfg.get("retries") or 1)
            except Exception:
                timeout_s = 20
                retries = 1
            # Auto-fallback on consecutive failures: switch to backup URL/fetcher when configured.
            source_for_fetch = dict(s)
            source_for_fetch["fetch"] = dict(fetch_cfg) if isinstance(fetch_cfg, dict) else {}
            fallback_used = False
            try:
                fallback_after = int(source_for_fetch["fetch"].get("fallback_after_failures") or 0)
            except Exception:
                fallback_after = 0
            if fallback_after > 0:
                consec_fail = int(self.store.source_consecutive_failures(sid, lookback=max(20, fallback_after * 3)))
                if consec_fail >= fallback_after:
                    f_url = str(
                        source_for_fetch["fetch"].get("fallback_url")
                        or source_for_fetch.get("fallback_url")
                        or ""
                    ).strip()
                    f_fetcher = str(
                        source_for_fetch["fetch"].get("fallback_fetcher")
                        or source_for_fetch.get("fallback_fetcher")
                        or ""
                    ).strip().lower()
                    if f_url:
                        source_for_fetch["url"] = f_url
                        fallback_used = True
                    if f_fetcher in {"rss", "html", "rsshub", "google_news", "api", "web"}:
                        source_for_fetch["fetcher"] = f_fetcher
                        source_for_fetch["connector"] = "web" if f_fetcher == "html" else f_fetcher
                        fallback_used = True
                    if fallback_used:
                        _log(
                            f"source_fallback_applied source_id={sid} "
                            f"consecutive_failures={consec_fail} threshold={fallback_after} "
                            f"url={source_for_fetch.get('url','')} fetcher={source_for_fetch.get('fetcher') or source_for_fetch.get('connector')}"
                        )

            result = fetch_source_entries(
                source_for_fetch,
                limit=max(5, int(fetch_limit or 50)),
                timeout_seconds=max(3, timeout_s),
                retries=max(0, retries),
                source_guard=source_guard,
            )
            dropped_static_or_listing_count += int(result.get("dropped_static_or_listing_count", 0) or 0)
            dropped_too_short_count += int(result.get("dropped_too_short_count", 0) or 0)
            result_drop_reasons = result.get("drop_reasons", {})
            if isinstance(result_drop_reasons, dict):
                dropped = sources_dropped_counts.setdefault(sid, {})
                for rk, rv in result_drop_reasons.items():
                    kk = str(rk).strip() or "dropped"
                    dropped[kk] = dropped.get(kk, 0) + int(rv or 0)
            dtop = result.get("dropped_by_domain_topN", [])
            if isinstance(dtop, list):
                for row in dtop:
                    if not isinstance(row, dict):
                        continue
                    dm = str(row.get("domain", "")).strip()
                    if not dm:
                        continue
                    dropped_static_or_listing_domains[dm] = dropped_static_or_listing_domains.get(dm, 0) + int(row.get("count", 0) or 0)
            ok = bool(result.get("ok"))
            status = "ok" if ok else "fail"
            if ok and fallback_used:
                status = "ok_fallback"
            http_status = result.get("http_status")
            err = result.get("error")
            if fallback_used and not ok:
                err = f"[fallback] {err}" if err else "[fallback] fetch_failed"
            if not ok and str(err or "").strip():
                sources_fetch_errors[sid] = str(err).strip()
            items_count = len(result.get("samples", []) if isinstance(result.get("samples"), list) else [])
            try:
                self.store.record_source_fetch(
                    sid,
                    status=status,
                    http_status=int(http_status) if http_status is not None else None,
                    error=str(err or "") if not ok else None,
                )
                self.store.record_source_fetch_event(
                    run_id=run_id,
                    source_id=sid,
                    status=status,
                    http_status=int(http_status) if http_status is not None else None,
                    items_count=int(items_count),
                    error=str(err or "") if not ok else None,
                    duration_ms=int((time.time() - t0) * 1000),
                )
            except Exception:
                pass

            if ok:
                fetched += 1
                sources_fetched_count += 1
                try:
                    entries_raw = list(result.get("entries", [])) if isinstance(result.get("entries", []), list) else []
                    entries_kept, pre_dropped, pre_reasons = filter_entries_for_collect(
                        entries_raw,
                        source_id=sid,
                        policy=source_policy,
                    )
                    if pre_dropped > 0:
                        dropped_by_source_policy_count += int(pre_dropped)
                        for rk, rv in pre_reasons.items():
                            dropped_by_source_policy_reasons[str(rk)] = dropped_by_source_policy_reasons.get(str(rk), 0) + int(rv or 0)
                    wr = collector.append_items(
                        run_id=run_id,
                        source_id=sid,
                        source_name=str(s.get("name", sid)),
                        source_group=str(s.get("source_group", "")).strip()
                        or ("regulatory" if "regulatory" in str(s.get("tags", "")) else "media"),
                        items=entries_kept,
                        rules_runtime={
                            "source_policy": source_policy,
                            "source_guard": source_guard,
                            "profile": str(rt.get("active_profile", profile)),
                            "anchors_pack": content_cfg.get("anchors_pack", {}) if isinstance(content_cfg.get("anchors_pack"), dict) else {},
                            "negatives_pack": content_cfg.get("negatives_pack", []) if isinstance(content_cfg.get("negatives_pack"), list) else [],
                            "frontier_policy": content_cfg.get("frontier_policy", {}) if isinstance(content_cfg.get("frontier_policy"), dict) else {},
                        },
                        source_trust_tier=source_tt,
                    )
                    assets_written += int(wr.get("written", 0))
                    deduped_count += int(wr.get("skipped", 0))
                    sources_written_counts[sid] = sources_written_counts.get(sid, 0) + int(wr.get("written", 0) or 0)
                    dropped_by_source_policy_count += int(wr.get("dropped_by_source_policy", 0) or 0)
                    wr_reasons = wr.get("dropped_by_source_policy_reasons", {})
                    if isinstance(wr_reasons, dict):
                        dropped = sources_dropped_counts.setdefault(sid, {})
                        for rk, rv in wr_reasons.items():
                            dropped_by_source_policy_reasons[str(rk)] = dropped_by_source_policy_reasons.get(str(rk), 0) + int(rv or 0)
                            kk = str(rk).strip() or "dropped"
                            dropped[kk] = dropped.get(kk, 0) + int(rv or 0)

                    # For non-RSS sources, keep at least one observable stub if parser produced no rows.
                    connector = str(source_for_fetch.get("connector") or source_for_fetch.get("fetcher") or "").lower()
                    is_non_rss = connector in {"html", "web", "api"}
                    if is_non_rss and int(result.get("items_count") or 0) <= 0:
                        sw = collector.append_stub_item(
                            run_id=run_id,
                            source_id=sid,
                            source_name=str(s.get("name", sid)),
                            source_group="media",
                            url=str(source_for_fetch.get("url", "")),
                            error=str(result.get("error_message") or ""),
                        )
                        assets_written += int(sw.get("written", 0))
                        deduped_count += int(sw.get("skipped", 0))
                        sources_written_counts[sid] = sources_written_counts.get(sid, 0) + int(sw.get("written", 0) or 0)
                except Exception as e:
                    msg = f"{sid}:append_failed:{e}"
                    errors.append(msg)
                    sources_fetch_errors[sid] = str(e)
            else:
                failed += 1
                sources_failed_count += 1
                # Non-RSS source can still emit a stub item for observability.
                connector = str(source_for_fetch.get("connector") or source_for_fetch.get("fetcher") or "").lower()
                if connector in {"html", "web", "api"}:
                    try:
                        sw = collector.append_stub_item(
                            run_id=run_id,
                            source_id=sid,
                            source_name=str(s.get("name", sid)),
                            source_group="media",
                            url=str(source_for_fetch.get("url", "")),
                            error=str(err or ""),
                        )
                        assets_written += int(sw.get("written", 0))
                        deduped_count += int(sw.get("skipped", 0))
                        sources_written_counts[sid] = sources_written_counts.get(sid, 0) + int(sw.get("written", 0) or 0)
                    except Exception as e:
                        errors.append(f"{sid}:stub_failed:{e}")
                        sources_fetch_errors[sid] = str(e)

        meta = {
            "run_id": run_id,
            "trigger": trigger,
            "schedule_id": schedule_id,
            "profile": profile,
            "purpose": "collect",
            "ts": time.time(),
            "counts": {
                "enabled_sources": len(rows),
                "fetched": fetched,
                "skipped": skipped,
                "failed": failed,
                "assets_written": assets_written,
                "assets_skipped": deduped_count,
                "dropped_by_source_policy": dropped_by_source_policy_count,
                "dropped_static_or_listing": dropped_static_or_listing_count,
                "dropped_too_short": dropped_too_short_count,
                "interval_defaulted": interval_defaulted_count,
            },
            "collect_asset_dir": self._collect_asset_dir,
            "assets_path": str(collector.base_dir),
            "assets_written_count": assets_written,
            "deduped_count": deduped_count,
            "sources_fetched_count": sources_fetched_count,
            "sources_failed_count": sources_failed_count,
            "dropped_by_source_policy_count": dropped_by_source_policy_count,
            "dropped_by_source_policy_reasons": dropped_by_source_policy_reasons,
            "dropped_static_or_listing_count": dropped_static_or_listing_count,
            "dropped_too_short_count": dropped_too_short_count,
            "dropped_static_or_listing_top_domains": [
                {"domain": k, "count": v}
                for k, v in sorted(dropped_static_or_listing_domains.items(), key=lambda kv: (-kv[1], kv[0]))[:5]
            ],
            "interval_defaulted_count": interval_defaulted_count,
            "sources_attempted": sources_attempted,
            "sources_written_counts": sources_written_counts,
            "sources_dropped_counts": sources_dropped_counts,
            "sources_fetch_errors": sources_fetch_errors,
            "errors": errors,
        }
        meta_path = artifacts_dir / "run_meta.json"
        meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
        ended_iso = datetime.now(timezone.utc).isoformat()
        try:
            self.store.record_report_artifact(
                run_id=run_id,
                artifact_path=str(meta_path),
                artifact_type="run_meta",
                sha256=_sha256_file(meta_path),
                created_at=ended_iso,
            )
            self.store.finish_run_execution(
                run_id=run_id,
                status="success",
                ended_at=ended_iso,
            )
        except Exception:
            pass
        return {
            "ok": True,
            "run_id": run_id,
            "counts": meta["counts"],
            "assets_path": meta["assets_path"],
            "assets_written_count": meta["assets_written_count"],
            "deduped_count": meta["deduped_count"],
            "sources_fetched_count": meta["sources_fetched_count"],
            "sources_failed_count": meta["sources_failed_count"],
            "sources_attempted": meta.get("sources_attempted", []),
            "sources_written_counts": meta.get("sources_written_counts", {}),
            "sources_dropped_counts": meta.get("sources_dropped_counts", {}),
            "sources_fetch_errors": meta.get("sources_fetch_errors", {}),
            "errors": meta["errors"],
            "artifacts_dir": str(artifacts_dir),
        }

    def _apply_config(self, cfg: dict[str, Any]) -> None:
        defaults = _extract_defaults(cfg)
        enabled = bool(defaults.get("enabled", False))
        tz = str(defaults.get("timezone", "Asia/Singapore"))
        self._collect_window_hours = int(defaults.get("collect_window_hours") or 24)
        self._collect_asset_dir = str(defaults.get("collect_asset_dir") or "artifacts/collect")
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
            next_run_time = None
            if s.type == "cron":
                trig = self.CronTrigger.from_crontab(str(s.cron), timezone=tz)
            else:
                trig = self.IntervalTrigger(minutes=int(s.interval_minutes or 60), timezone=tz)
                # Run interval jobs once shortly after (re)start, then continue by interval.
                try:
                    next_run_time = datetime.now(ZoneInfo(tz)) + timedelta(seconds=5)
                except Exception:
                    next_run_time = datetime.now(timezone.utc) + timedelta(seconds=5)

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
                next_run_time=next_run_time,
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
