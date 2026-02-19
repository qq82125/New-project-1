from __future__ import annotations

from contextlib import contextmanager
from collections.abc import Generator

from sqlalchemy.orm import Session, sessionmaker

from app.db.engine import get_primary_engine, get_secondary_engine


PrimarySessionLocal = sessionmaker(bind=get_primary_engine(), autoflush=False, autocommit=False, expire_on_commit=False)
_secondary = get_secondary_engine()
SecondarySessionLocal = (
    sessionmaker(bind=_secondary, autoflush=False, autocommit=False, expire_on_commit=False)
    if _secondary is not None
    else None
)


@contextmanager
def session_scope(*, secondary: bool = False) -> Generator[Session, None, None]:
    factory = SecondarySessionLocal if secondary else PrimarySessionLocal
    if factory is None:
        raise RuntimeError("secondary database session requested but DATABASE_URL_SECONDARY is not configured")
    session: Session = factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
