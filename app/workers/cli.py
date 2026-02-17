from __future__ import annotations

import json
import sys
from subprocess import CalledProcessError

from app.rules.errors import RULES_003_RULESET_MISMATCH, RuleEngineError
from app.rules.engine import RuleEngine
from app.rules.models import RuleSelection
from app.services.source_registry import (
    SourceRegistryError,
    diff_sources_for_profiles,
    list_sources_for_profile,
    load_sources_registry,
    retire_source,
    test_source,
    validate_sources_registry,
)
from app.workers.dryrun import main as dryrun_main
from app.workers.replay import main as replay_main


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
        print(json.dumps({"ok": True, **result, "sources_registry": source_validate}, ensure_ascii=False, indent=2))
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
    if not source_id:
        print("--source-id is required", file=sys.stderr)
        return 2
    limit = int(_get_opt(argv, "--limit") or "3")
    engine = RuleEngine()
    sources = load_sources_registry(engine.project_root)
    source = next((s for s in sources if str(s.get("id", "")) == source_id), None)
    if source is None:
        print(json.dumps({"ok": False, "error": f"source not found: {source_id}"}, ensure_ascii=False))
        return 3
    out = test_source(source, limit=limit)
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0 if out.get("ok") else 4


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


def main() -> int:
    argv = sys.argv[1:]
    if not argv:
        print(
            "Usage: python -m app.workers.cli "
            "rules:validate|rules:print|rules:dryrun|rules:replay|"
            "sources:list|sources:validate|sources:test|sources:diff|sources:retire [options]",
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
