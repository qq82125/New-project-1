from __future__ import annotations

"""
Thin entrypoint for the Rules Admin (FastAPI) server.

This keeps the user-facing command stable:
  ADMIN_USER=... ADMIN_PASS=... python -m app.admin_server
"""

from app.web.rules_admin_api import run_server


def main() -> None:
    run_server()


if __name__ == "__main__":
    main()

