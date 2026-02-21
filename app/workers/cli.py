from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from subprocess import CalledProcessError

from app.rules.errors import RULES_003_RULESET_MISMATCH, RuleEngineError
from app.rules.engine import RuleEngine
from app.rules.models import RuleSelection
from app.services.db_migration import dual_replay_compare, migrate_sqlite_to_target, verify_sqlite_vs_target
from app.services.rules_store import RulesStore
from app.services.source_registry import (
    SourceRegistryError,
    diff_sources_for_profiles,
    effective_source_ids_for_profile,
    list_sources_for_profile,
    load_sources_registry_bundle,
    load_sources_registry,
    retire_source,
    run_sources_test_harness,
    test_source,
    validate_sources_registry,
)
from app.services.collect_asset_store import CollectAssetStore
from app.services.analysis_cache_store import AnalysisCacheStore
from app.services.analysis_generator import AnalysisGenerator, degraded_analysis
from app.workers.dryrun import main as dryrun_main
from app.workers.live_run import run_digest
from app.workers.replay import main as replay_main
from app.workers.scheduler_worker import SchedulerWorker


def _get_opt(argv: list[str], key: str) -> str | None:
    if key not in argv:
        return None
    idx = argv.index(key)
    if idx + 1 >= len(argv):
        return None
    return argv[idx + 1]


def cmd_rules_validate(argv: list[str]) -> int:
    engine = RuleEngine()
    source_validate = validate_sources_registry(engine.project_root, rules_root=engine.rules_root)
    profile = _get_opt(argv, "--profile")
    if profile:
        result = engine.validate_profile_pair(profile)
        print(
            json.dumps(
                {
                    "ok": True,
                    **result,
                    "sources_registry": source_validate,
                    "active_source_ids": effective_source_ids_for_profile(engine.project_root, profile, rules_root=engine.rules_root),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0

    validated: list[dict[str, str]] = []
    for ruleset in ("email_rules", "content_rules", "qc_rules", "output_rules", "scheduler_rules"):
        # Validate both workspace-published rules and repo fallbacks.
        dirs = [engine.rules_root / ruleset, engine.project_root / "rules" / ruleset]
        seen: set[str] = set()
        paths = []
        for d in dirs:
            if not d.exists():
                continue
            for p in sorted(d.glob("*.y*ml")) + sorted(d.glob("*.json")):
                sp = str(p)
                if sp in seen:
                    continue
                seen.add(sp)
                paths.append(p)
        for path in paths:
            data = engine._load_file(path)
            if data.get("ruleset") != ruleset:
                raise RuleEngineError(
                    RULES_003_RULESET_MISMATCH,
                    f"file={path} ruleset={data.get('ruleset')} expected={ruleset}",
                )
            engine.validate(ruleset, data)
            # Apply boundary assertions as part of validation.
            engine._boundary_check(RuleSelection(ruleset, str(data.get("profile", path.stem)), str(data.get("version", "")), path, data))
            selection = RuleSelection(ruleset, str(data.get("profile", path.stem)), str(data.get("version", "")), path, data)
            validated.append(
                {
                    "ruleset": selection.ruleset,
                    "profile": selection.profile,
                    "version": selection.version,
                    "path": str(path),
                }
            )
    print(
        json.dumps(
            {"ok": True, "validated": validated, "sources_registry": source_validate},
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def cmd_rules_print(argv: list[str]) -> int:
    profile = _get_opt(argv, "--profile") or "legacy"
    strategy = _get_opt(argv, "--strategy") or "priority_last_match"
    engine = RuleEngine()
    result = engine.build_decision(profile=profile, conflict_strategy=strategy)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def cmd_rules_dryrun(argv: list[str]) -> int:
    profile = _get_opt(argv, "--profile")
    report_date = _get_opt(argv, "--date")

    # Backward-compatible options
    email_profile = _get_opt(argv, "--email-profile")
    content_profile = _get_opt(argv, "--content-profile")
    if not profile and email_profile and content_profile and email_profile == content_profile:
        profile = email_profile

    return dryrun_main(profile=profile or "legacy", report_date=report_date)


def cmd_rules_replay(argv: list[str]) -> int:
    run_id = _get_opt(argv, "--run-id")
    send_opt = _get_opt(argv, "--send")
    profile = _get_opt(argv, "--profile")

    # Backward-compatible behavior: presence-only --send means true
    send_value: bool | str = send_opt if send_opt is not None else ("--send" in argv)

    if not run_id:
        print("--run-id is required for rules:replay", file=sys.stderr)
        return 2

    return replay_main(run_id=run_id, send=send_value, profile=profile)


def cmd_sources_list(argv: list[str]) -> int:
    profile = _get_opt(argv, "--profile") or "enhanced"
    engine = RuleEngine()
    sources = list_sources_for_profile(engine.project_root, profile)
    print(
        json.dumps(
            {"ok": True, "profile": profile, "count": len(sources), "sources": sources},
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def cmd_sources_validate(argv: list[str]) -> int:
    _ = argv
    engine = RuleEngine()
    out = validate_sources_registry(engine.project_root)
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


def cmd_sources_test(argv: list[str]) -> int:
    source_id = _get_opt(argv, "--source-id")
    limit = int(_get_opt(argv, "--limit") or "3")
    enabled_only = "--enabled-only" in argv
    json_out = _get_opt(argv, "--json-out")
    md_out = _get_opt(argv, "--md-out")
    workers = int(_get_opt(argv, "--workers") or "6")
    timeout_seconds = int(_get_opt(argv, "--timeout-seconds") or "20")
    retries = int(_get_opt(argv, "--retries") or "2")
    engine = RuleEngine()
    if source_id:
        sources = load_sources_registry(engine.project_root, rules_root=engine.rules_root)
        source = next((s for s in sources if str(s.get("id", "")) == source_id), None)
        if source is None:
            print(json.dumps({"ok": False, "error": f"source not found: {source_id}"}, ensure_ascii=False))
            return 3
        out = test_source(source, limit=limit, timeout_seconds=timeout_seconds, retries=retries)
        print(json.dumps(out, ensure_ascii=False, indent=2))
        return 0 if out.get("ok") else 4

    bundle = load_sources_registry_bundle(engine.project_root, rules_root=engine.rules_root)
    out = run_sources_test_harness(
        engine.project_root,
        rules_root=engine.rules_root,
        enabled_only=enabled_only,
        limit=limit,
        max_workers=workers,
        timeout_seconds=timeout_seconds,
        retries=retries,
    )
    if json_out:
        import os
        os.makedirs(os.path.dirname(json_out) or ".", exist_ok=True)
        with open(json_out, "w", encoding="utf-8") as f:
            f.write(json.dumps(out, ensure_ascii=False, indent=2))
    if md_out:
        import os
        os.makedirs(os.path.dirname(md_out) or ".", exist_ok=True)
        with open(md_out, "w", encoding="utf-8") as f:
            f.write(str(out.get("markdown", "")))

    payload = {
        "ok": True,
        "registry_file": bundle.get("source_file") if isinstance(bundle, dict) else "",
        "overrides_file": bundle.get("overrides_file") if isinstance(bundle, dict) else "",
        "summary": out.get("summary", {}),
        "json_out": json_out or "",
        "md_out": md_out or "",
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    fail_count = int(((out.get("summary") or {}).get("fail", 0)))
    return 0 if fail_count == 0 else 4


def cmd_sources_diff(argv: list[str]) -> int:
    from_profile = _get_opt(argv, "--from") or "legacy"
    to_profile = _get_opt(argv, "--to") or "enhanced"
    engine = RuleEngine()
    out = diff_sources_for_profiles(engine.project_root, from_profile, to_profile)
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


def cmd_sources_retire(argv: list[str]) -> int:
    source_id = _get_opt(argv, "--source-id")
    reason = _get_opt(argv, "--reason") or "retired via CLI"
    if not source_id:
        print("--source-id is required", file=sys.stderr)
        return 2
    engine = RuleEngine()
    out = retire_source(engine.project_root, source_id=source_id, reason=reason)
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


def cmd_db_migrate(argv: list[str]) -> int:
    from pathlib import Path

    from_value = _get_opt(argv, "--from") or _get_opt(argv, "--source-sqlite") or "sqlite:///data/rules.db"
    target_url = _get_opt(argv, "--to") or _get_opt(argv, "--target-url") or os.environ.get("DATABASE_URL", "")
    checkpoint = _get_opt(argv, "--checkpoint") or "data/db_migrate_checkpoint.json"
    tables_arg = _get_opt(argv, "--tables") or ""
    batch_size = int(_get_opt(argv, "--batch-size") or "1000")
    resume = (_get_opt(argv, "--resume") or "true").strip().lower() != "false"
    if not target_url:
        print("--to/--target-url (or DATABASE_URL) is required", file=sys.stderr)
        return 2
    tables = [x.strip() for x in tables_arg.split(",") if x.strip()] if tables_arg else None
    out = migrate_sqlite_to_target(
        project_root=Path.cwd(),
        target_url=target_url,
        source_sqlite_url_or_path=from_value,
        batch_size=batch_size,
        resume=resume,
        checkpoint_path=Path(checkpoint),
        tables=tables,
    )
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0 if bool(out.get("ok")) else 4


def cmd_db_verify(argv: list[str]) -> int:
    from pathlib import Path

    from_value = _get_opt(argv, "--from") or _get_opt(argv, "--source-sqlite") or "sqlite:///data/rules.db"
    target_url = _get_opt(argv, "--to") or _get_opt(argv, "--target-url") or os.environ.get("DATABASE_URL", "")
    tables_arg = _get_opt(argv, "--tables") or ""
    sample_rate = float(_get_opt(argv, "--sample") or "0.05")
    if not target_url:
        print("--to/--target-url (or DATABASE_URL) is required", file=sys.stderr)
        return 2
    tables = [x.strip() for x in tables_arg.split(",") if x.strip()] if tables_arg else None
    out = verify_sqlite_vs_target(
        project_root=Path.cwd(),
        target_url=target_url,
        source_sqlite_url_or_path=from_value,
        tables=tables,
        sample_rate=sample_rate,
    )
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0 if bool(out.get("ok")) else 4


def cmd_db_dual_replay(argv: list[str]) -> int:
    from pathlib import Path

    # Compatibility mode: explicit compare between two URLs.
    if "--compare" in argv:
        primary_url = _get_opt(argv, "--primary-url") or os.environ.get("DATABASE_URL", "")
        secondary_url = _get_opt(argv, "--secondary-url") or os.environ.get("DATABASE_URL_SECONDARY", "")
        if not primary_url or not secondary_url:
            print("--primary-url/--secondary-url (or DATABASE_URL/DATABASE_URL_SECONDARY) are required", file=sys.stderr)
            return 2
        out = dual_replay_compare(project_root=Path.cwd(), primary_url=primary_url, secondary_url=secondary_url)
        print(json.dumps(out, ensure_ascii=False, indent=2))
        return 0 if bool(out.get("ok")) else 4

    limit = int(_get_opt(argv, "--limit") or "100")
    store = RulesStore(Path.cwd())
    if not hasattr(store, "replay_dual_write_failures"):
        print(json.dumps({"ok": False, "error": "store does not support dual replay"}, ensure_ascii=False, indent=2))
        return 5
    out = store.replay_dual_write_failures(limit=limit)
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0 if bool(out.get("ok")) else 4


def cmd_db_status(argv: list[str]) -> int:
    _ = argv
    from pathlib import Path

    store = RulesStore(Path.cwd())
    if not hasattr(store, "db_status"):
        print(json.dumps({"ok": False, "error": "store does not support db status"}, ensure_ascii=False, indent=2))
        return 5
    out = {"ok": True, **store.db_status()}
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


def cmd_collect_now(argv: list[str]) -> int:
    profile = _get_opt(argv, "--profile") or "enhanced"
    trigger = _get_opt(argv, "--trigger") or "manual"
    schedule_id = _get_opt(argv, "--schedule-id") or "manual"
    max_sources = _get_opt(argv, "--max-sources")
    if not max_sources:
        max_sources = _get_opt(argv, "--limit-sources")
    force = (_get_opt(argv, "--force") or "false").strip().lower() in {"1", "true", "yes", "y", "on"}
    fetch_limit = _get_opt(argv, "--fetch-limit")
    worker = SchedulerWorker()
    out = worker._run_collect(
        schedule_id=schedule_id,
        profile=profile,
        trigger=trigger,
        max_sources=int(max_sources) if max_sources and str(max_sources).isdigit() else None,
        force=force,
        fetch_limit=int(fetch_limit) if fetch_limit and str(fetch_limit).isdigit() else 50,
    )
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0 if bool(out.get("ok")) else 4


def cmd_collect_clean(argv: list[str]) -> int:
    keep_days = int(_get_opt(argv, "--keep-days") or "30")
    asset_dir = _get_opt(argv, "--collect-asset-dir") or "artifacts/collect"
    root = Path(__file__).resolve().parents[2]
    store = CollectAssetStore(root, asset_dir=asset_dir)
    out = store.cleanup(keep_days=max(1, keep_days))
    payload = {"ok": True, "collect_asset_dir": str(store.base_dir), **out}
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def cmd_analysis_clean(argv: list[str]) -> int:
    keep_days = int(_get_opt(argv, "--keep-days") or "30")
    asset_dir = _get_opt(argv, "--analysis-asset-dir") or "artifacts/analysis"
    root = Path(__file__).resolve().parents[2]
    store = AnalysisCacheStore(root, asset_dir=asset_dir)
    out = store.cleanup(keep_days=max(1, keep_days))
    payload = {"ok": True, "analysis_asset_dir": str(store.base_dir), **out}
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def cmd_analysis_recompute(argv: list[str]) -> int:
    model = _get_opt(argv, "--model") or "primary"
    prompt_version = _get_opt(argv, "--prompt-version") or "v2"
    sample = int(_get_opt(argv, "--sample") or "20")
    asset_dir = _get_opt(argv, "--analysis-asset-dir") or "artifacts/analysis"
    root = Path(__file__).resolve().parents[2]
    store = AnalysisCacheStore(root, asset_dir=asset_dir)

    files = sorted(store.base_dir.glob("items-*.jsonl"), reverse=True)
    if not files:
        print(json.dumps({"ok": False, "error": "no analysis cache files found"}, ensure_ascii=False, indent=2))
        return 4

    rows: list[dict] = []
    with files[0].open("r", encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if not ln:
                continue
            try:
                rows.append(json.loads(ln))
            except Exception:
                continue
            if len(rows) >= max(1, sample):
                break
    if not rows:
        print(json.dumps({"ok": False, "error": "no valid cache rows"}, ensure_ascii=False, indent=2))
        return 4

    if str(model).strip().lower() == "primary":
        primary_model = os.environ.get("MODEL_PRIMARY", "local-heuristic-v1")
        fallback_model = os.environ.get("MODEL_FALLBACK", "local-lite-v1")
    elif str(model).strip().lower() == "fallback":
        primary_model = os.environ.get("MODEL_FALLBACK", "local-lite-v1")
        fallback_model = os.environ.get("MODEL_FALLBACK", "local-lite-v1")
    else:
        primary_model = str(model)
        fallback_model = str(model)
    gen = AnalysisGenerator(
        primary_model=primary_model,
        fallback_model=fallback_model,
        model_policy="always_primary",
        prompt_version=prompt_version,
    )

    changed = 0
    kept = 0
    lines = [
        f"# Analysis Recompute Compare",
        f"",
        f"- source_file: `{files[0]}`",
        f"- sample: {len(rows)}",
        f"- model: {model}",
        f"- prompt_version: {prompt_version}",
        f"",
    ]
    for idx, r in enumerate(rows, 1):
        item = {
            "title": r.get("title", ""),
            "source": r.get("source", ""),
            "event_type": r.get("event_type", ""),
            "track": r.get("track", "core"),
            "relevance_level": r.get("relevance_level", 0),
            "url": r.get("url", ""),
        }
        old_summary = str(r.get("summary", "")).strip()
        try:
            new = gen.generate(item, rules={})
        except Exception as e:
            new = degraded_analysis(item, str(e))
        new_summary = str(new.get("summary", "")).strip()
        if new_summary != old_summary:
            changed += 1
        else:
            kept += 1
        lines.extend(
            [
                f"## {idx}. {str(item.get('title',''))[:80]}",
                f"- old_model: {r.get('used_model') or r.get('model','')}",
                f"- new_model: {new.get('used_model') or new.get('model','')}",
                f"- old_prompt: {r.get('prompt_version','')}",
                f"- new_prompt: {new.get('prompt_version','')}",
                f"- changed: {'yes' if new_summary != old_summary else 'no'}",
                f"- old: {old_summary[:180]}",
                f"- new: {new_summary[:180]}",
                "",
            ]
        )

    out_file = store.base_dir / f"compare-{int(__import__('time').time())}.md"
    out_file.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    payload = {
        "ok": True,
        "compare_file": str(out_file),
        "sample": len(rows),
        "changed": changed,
        "unchanged": kept,
        "model": model,
        "prompt_version": prompt_version,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def cmd_digest_now(argv: list[str]) -> int:
    profile = _get_opt(argv, "--profile") or "enhanced"
    trigger = _get_opt(argv, "--trigger") or "manual"
    schedule_id = _get_opt(argv, "--schedule-id") or "manual"
    report_date = _get_opt(argv, "--date")
    send_raw = (_get_opt(argv, "--send") or "true").strip().lower()
    send = send_raw in {"1", "true", "yes", "y", "on"}
    use_collect_assets = (_get_opt(argv, "--use-collect-assets") or "true").strip().lower() in {"1", "true", "yes", "y", "on"}
    collect_window_hours = int(_get_opt(argv, "--collect-window-hours") or "24")
    collect_asset_dir = _get_opt(argv, "--collect-asset-dir") or "artifacts/collect"
    out = run_digest(
        profile=profile,
        trigger=trigger,
        schedule_id=schedule_id,
        send=send,
        report_date=report_date,
        collect_window_hours=collect_window_hours,
        collect_asset_dir=collect_asset_dir,
        use_collect_assets=use_collect_assets,
    )
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0 if bool(out.get("ok")) else 4


def main() -> int:
    argv = sys.argv[1:]
    if not argv:
        print(
            "Usage: python -m app.workers.cli "
            "rules:validate|rules:print|rules:dryrun|rules:replay|"
            "sources:list|sources:validate|sources:test|sources:diff|sources:retire|"
            "db:migrate|db:verify|db:dual-replay|db:status|"
            "collect-now|collect-clean|analysis-clean|analysis-recompute|digest-now [options]",
            file=sys.stderr,
        )
        return 2

    cmd = argv[0]
    tail = argv[1:]
    try:
        if cmd == "rules:validate":
            return cmd_rules_validate(tail)
        if cmd == "rules:print":
            return cmd_rules_print(tail)
        if cmd == "rules:dryrun":
            return cmd_rules_dryrun(tail)
        if cmd == "rules:replay":
            return cmd_rules_replay(tail)
        if cmd == "sources:list":
            return cmd_sources_list(tail)
        if cmd == "sources:validate":
            return cmd_sources_validate(tail)
        if cmd == "sources:test":
            return cmd_sources_test(tail)
        if cmd == "sources:diff":
            return cmd_sources_diff(tail)
        if cmd == "sources:retire":
            return cmd_sources_retire(tail)
        if cmd == "db:migrate":
            return cmd_db_migrate(tail)
        if cmd == "db:verify":
            return cmd_db_verify(tail)
        if cmd == "db:dual-replay":
            return cmd_db_dual_replay(tail)
        if cmd == "db:status":
            return cmd_db_status(tail)
        if cmd == "collect-now":
            return cmd_collect_now(tail)
        if cmd == "collect-clean":
            return cmd_collect_clean(tail)
        if cmd == "analysis-clean":
            return cmd_analysis_clean(tail)
        if cmd == "analysis-recompute":
            return cmd_analysis_recompute(tail)
        if cmd == "digest-now":
            return cmd_digest_now(tail)
        print(f"Unknown command: {cmd}", file=sys.stderr)
        return 2
    except RuleEngineError as e:
        print(json.dumps({"ok": False, "error_code": e.err.code, "error": str(e)}, ensure_ascii=False))
        return 10
    except CalledProcessError as e:
        print(
            json.dumps(
                {"ok": False, "error_code": "RULES_900_SUBPROCESS_FAILED", "error": str(e)},
                ensure_ascii=False,
            )
        )
        return 11
    except Exception as e:  # pragma: no cover - defensive
        if isinstance(e, SourceRegistryError):
            print(
                json.dumps(
                    {"ok": False, "error_code": "RULES_910_SOURCES_REGISTRY", "error": str(e)},
                    ensure_ascii=False,
                )
            )
            return 13
        print(
            json.dumps(
                {"ok": False, "error_code": "RULES_999_UNEXPECTED", "error": str(e)},
                ensure_ascii=False,
            )
        )
        return 12


if __name__ == "__main__":
    raise SystemExit(main())
