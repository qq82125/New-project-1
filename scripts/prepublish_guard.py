#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _run(cmd: list[str]) -> tuple[bool, str]:
    p = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)
    out = ((p.stdout or "") + ("\n" + p.stderr if p.stderr else "")).strip()
    if p.returncode == 0:
        return True, out
    return False, out


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Pre-publish guard: lock output template and rule quality before publish."
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Run acceptance full (default: acceptance smoke).",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Treat all check failures as blocking.",
    )
    args = parser.parse_args()

    checks: list[tuple[str, list[str]]] = [
        ("rules_validate_enhanced", ["python3", "-m", "app.workers.cli", "rules:validate", "--profile", "enhanced"]),
        ("template_lock_A_H", ["pytest", "-q", "tests/test_h_template_lock_pr23_2.py"]),
        (
            "acceptance",
            ["python3", "-m", "app.workers.cli", "acceptance-run", "--mode", "full" if args.full else "smoke"],
        ),
    ]

    print("# Pre-publish Guard")
    print(f"- project: {ROOT}")
    print(f"- mode: {'full' if args.full else 'smoke'}")
    print("")

    failed = 0
    warned = 0
    for name, cmd in checks:
        print(f"## {name}")
        print(f"$ {' '.join(cmd)}")
        ok, out = _run(cmd)
        status = "PASS" if ok else "FAIL"
        blocking = True
        if not ok and name == "rules_validate_enhanced" and not args.strict:
            # Some local workspaces may carry temporary source group drift.
            # Keep non-strict mode usable, but surface a clear warning.
            if "RULES_910_SOURCES_REGISTRY" in out:
                status = "WARN"
                blocking = False
                warned += 1
        print(status)
        if out:
            tail = "\n".join(out.splitlines()[-20:])
            print(tail)
        print("")
        if not ok and blocking:
            failed += 1

    if failed:
        print(f"RESULT: FAIL ({failed} checks failed)")
        return 1
    if warned:
        print(f"RESULT: PASS_WITH_WARNINGS ({warned} warnings)")
    else:
        print("RESULT: PASS (all checks passed)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
