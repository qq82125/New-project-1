from __future__ import annotations

import json
import sys
from subprocess import CalledProcessError

from app.rules.errors import RuleEngineError
from app.rules.engine import RuleEngine
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
    profile = _get_opt(argv, "--profile")
    if profile:
        result = engine.validate_profile_pair(profile)
        print(json.dumps({"ok": True, **result}, ensure_ascii=False, indent=2))
        return 0

    validated: list[dict[str, str]] = []
    for ruleset in ("email_rules", "content_rules"):
        rules_dir = engine.rules_root / ruleset
        for path in sorted(rules_dir.glob("*.y*ml")) + sorted(rules_dir.glob("*.json")):
            selection = engine.load(ruleset, path.stem)
            validated.append(
                {
                    "ruleset": selection.ruleset,
                    "profile": selection.profile,
                    "version": selection.version,
                    "path": str(path),
                }
            )
    print(json.dumps({"ok": True, "validated": validated}, ensure_ascii=False, indent=2))
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


def main() -> int:
    argv = sys.argv[1:]
    if not argv:
        print(
            "Usage: python -m app.workers.cli "
            "rules:validate|rules:print|rules:dryrun|rules:replay [options]",
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
        print(
            json.dumps(
                {"ok": False, "error_code": "RULES_999_UNEXPECTED", "error": str(e)},
                ensure_ascii=False,
            )
        )
        return 12


if __name__ == "__main__":
    raise SystemExit(main())
