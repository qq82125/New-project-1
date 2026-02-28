"""add event_type columns to raw_items/stories

Revision ID: 20260227_0007
Revises: 20260227_0006
Create Date: 2026-02-27
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260227_0007"
down_revision = "20260227_0006"
branch_labels = None
depends_on = None


def _has_column(table: str, col: str) -> bool:
    bind = op.get_bind()
    insp = sa.inspect(bind)
    try:
        cols = insp.get_columns(table)
    except Exception:
        return False
    return any(str(c.get("name")) == col for c in cols)


def upgrade() -> None:
    if not _has_column("raw_items", "event_type"):
        op.add_column("raw_items", sa.Column("event_type", sa.Text(), nullable=True))
    if not _has_column("stories", "event_type"):
        op.add_column("stories", sa.Column("event_type", sa.Text(), nullable=True))


def downgrade() -> None:
    if _has_column("stories", "event_type"):
        op.drop_column("stories", "event_type")
    if _has_column("raw_items", "event_type"):
        op.drop_column("raw_items", "event_type")

