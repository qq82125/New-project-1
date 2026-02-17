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


def cmd_rules_validate() -> int:
    engine = RuleEngine()
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


def cmd_rules_dryrun(argv: list[str]) -> int:
    email_profile = _get_opt(argv, "--email-profile")
    content_profile = _get_opt(argv, "--content-profile")
    return dryrun_main(email_profile=email_profile, content_profile=content_profile)


def cmd_rules_replay(argv: list[str]) -> int:
    replay_date = _get_opt(argv, "--date")
    run_id = _get_opt(argv, "--run-id")
    email_profile = _get_opt(argv, "--email-profile")
    content_profile = _get_opt(argv, "--content-profile")
    send = "--send" in argv
    return replay_main(
        replay_date=replay_date,
        run_id=run_id,
        send=send,
        email_profile=email_profile,
        content_profile=content_profile,
    )


def main() -> int:
    argv = sys.argv[1:]
    if not argv:
        print(
            "Usage: python -m app.workers.cli "
            "rules:validate|rules:dryrun|rules:replay [options]",
            file=sys.stderr,
        )
        return 2

    cmd = argv[0]
    tail = argv[1:]
    try:
        if cmd == "rules:validate":
            return cmd_rules_validate()
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
