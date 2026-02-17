from __future__ import annotations

import json
import os
import socket
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator


class RunLockError(RuntimeError):
    pass


def _holder_id() -> str:
    host = socket.gethostname()
    pid = os.getpid()
    return f"{host}:{pid}"


@contextmanager
def acquire_run_lock(lock_path: Path, *, run_id: str, purpose: str) -> Iterator[dict[str, Any]]:
    """
    Cross-process single-concurrency lock.

    - Uses fcntl.flock (works well on macOS/Linux).
    - Writes a small JSON sidecar `*.meta.json` so operators can see who holds the lock.
    - If the process exits/crashes, OS releases the flock automatically.
    """
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    meta_path = lock_path.with_suffix(lock_path.suffix + ".meta.json")
    holder = _holder_id()
    started = time.time()

    # Lazy import: fcntl is not available on Windows.
    try:
        import fcntl  # type: ignore
    except Exception as e:  # pragma: no cover
        raise RunLockError(f"fcntl not available: {e}") from e

    f = lock_path.open("a+", encoding="utf-8")
    try:
        try:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as e:
            # Best-effort read meta for a friendlier error.
            try:
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
            except Exception:
                meta = {}
            raise RunLockError(
                f"lock busy: {lock_path} meta={json.dumps(meta, ensure_ascii=False)}"
            ) from e

        meta = {
            "run_id": run_id,
            "purpose": purpose,
            "holder": holder,
            "acquired_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(started)),
        }
        meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
        yield meta
    finally:
        try:
            import fcntl  # type: ignore

            fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        except Exception:
            pass
        try:
            f.close()
        except Exception:
            pass

