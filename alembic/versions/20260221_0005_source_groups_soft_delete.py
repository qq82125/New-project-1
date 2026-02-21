"""source groups + source soft delete/overrides

Revision ID: 20260221_0005
Revises: 20260219_0004
Create Date: 2026-02-21
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260221_0005"
down_revision = "20260219_0004"
branch_labels = None
depends_on = None


def _has_table(table: str) -> bool:
    bind = op.get_bind()
    insp = sa.inspect(bind)
    return table in insp.get_table_names()


def _has_column(table: str, col: str) -> bool:
    bind = op.get_bind()
    insp = sa.inspect(bind)
    try:
        cols = insp.get_columns(table)
    except Exception:
        return False
    return any(str(c.get("name")) == col for c in cols)


def _existing_indexes(table: str) -> set[str]:
    bind = op.get_bind()
    insp = sa.inspect(bind)
    try:
        return {str(i.get("name")) for i in insp.get_indexes(table)}
    except Exception:
        return set()


def upgrade() -> None:
    if _has_table("sources"):
        if not _has_column("sources", "source_group"):
            op.add_column("sources", sa.Column("source_group", sa.Text(), nullable=False, server_default=sa.text("'media'")))
        if not _has_column("sources", "fetch_interval_minutes"):
            op.add_column("sources", sa.Column("fetch_interval_minutes", sa.Integer(), nullable=True))
        if not _has_column("sources", "deleted_at"):
            op.add_column("sources", sa.Column("deleted_at", sa.Text(), nullable=True))
        if "idx_sources_deleted_enabled" not in _existing_indexes("sources"):
            op.create_index("idx_sources_deleted_enabled", "sources", ["deleted_at", "enabled"])

    if not _has_table("source_groups"):
        op.create_table(
            "source_groups",
            sa.Column("group_key", sa.Text(), primary_key=True),
            sa.Column("display_name", sa.Text(), nullable=False),
            sa.Column("default_interval_minutes", sa.Integer(), nullable=True),
            sa.Column("enabled", sa.Integer(), nullable=False, server_default=sa.text("1")),
            sa.Column("updated_at", sa.Text(), nullable=False),
        )
    if "idx_source_groups_enabled" not in _existing_indexes("source_groups"):
        op.create_index("idx_source_groups_enabled", "source_groups", ["enabled", "group_key"])

    for key, name, interval in [
        ("regulatory", "Regulatory", 20),
        ("media", "Media", 60),
        ("evidence", "Evidence", 720),
        ("company", "Company", 240),
        ("procurement", "Procurement", 60),
    ]:
        op.execute(
            sa.text(
                """
                INSERT INTO source_groups(group_key, display_name, default_interval_minutes, enabled, updated_at)
                SELECT :k, :n, :i, 1, CURRENT_TIMESTAMP
                WHERE NOT EXISTS (SELECT 1 FROM source_groups WHERE group_key = :k)
                """
            ).bindparams(k=key, n=name, i=interval)
        )


def downgrade() -> None:
    if _has_table("source_groups"):
        if "idx_source_groups_enabled" in _existing_indexes("source_groups"):
            op.drop_index("idx_source_groups_enabled", table_name="source_groups")
        op.drop_table("source_groups")
    if _has_table("sources"):
        if "idx_sources_deleted_enabled" in _existing_indexes("sources"):
            op.drop_index("idx_sources_deleted_enabled", table_name="sources")
        if _has_column("sources", "deleted_at"):
            op.drop_column("sources", "deleted_at")
        if _has_column("sources", "fetch_interval_minutes"):
            op.drop_column("sources", "fetch_interval_minutes")
        if _has_column("sources", "source_group"):
            op.drop_column("sources", "source_group")
