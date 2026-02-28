"""add dual-feed tables: raw_items, stories, story_items

Revision ID: 20260227_0006
Revises: 20260221_0005
Create Date: 2026-02-27
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260227_0006"
down_revision = "20260221_0005"
branch_labels = None
depends_on = None


def _json_type() -> sa.TypeEngine:
    dialect_name = op.get_context().dialect.name
    if dialect_name == "postgresql":
        return postgresql.JSONB()
    return sa.Text()


def _has_table(table: str) -> bool:
    bind = op.get_bind()
    insp = sa.inspect(bind)
    return table in insp.get_table_names()


def _existing_indexes(table: str) -> set[str]:
    bind = op.get_bind()
    insp = sa.inspect(bind)
    try:
        return {str(i.get("name", "")) for i in insp.get_indexes(table)}
    except Exception:
        return set()


def upgrade() -> None:
    json_type = _json_type()

    if not _has_table("raw_items"):
        op.create_table(
            "raw_items",
            sa.Column("id", sa.Text(), primary_key=True),
            sa.Column("source_id", sa.Text(), nullable=False),
            sa.Column("fetched_at", sa.Text(), nullable=False),
            sa.Column("published_at", sa.Text(), nullable=True),
            sa.Column("title_raw", sa.Text(), nullable=False),
            sa.Column("title_norm", sa.Text(), nullable=False),
            sa.Column("url_raw", sa.Text(), nullable=False),
            sa.Column("canonical_url", sa.Text(), nullable=False),
            sa.Column("content_snippet", sa.Text(), nullable=True),
            sa.Column("raw_payload", json_type, nullable=False),
            sa.Column("source_group", sa.Text(), nullable=True),
            sa.Column("region", sa.Text(), nullable=True),
            sa.Column("trust_tier", sa.Text(), nullable=True),
            sa.Column("priority", sa.Integer(), nullable=False, server_default=sa.text("0")),
        )
    if "idx_raw_items_published_id" not in _existing_indexes("raw_items"):
        op.create_index("idx_raw_items_published_id", "raw_items", ["published_at", "id"], unique=False)
    if "idx_raw_items_canonical_url" not in _existing_indexes("raw_items"):
        op.create_index("idx_raw_items_canonical_url", "raw_items", ["canonical_url"], unique=False)
    if "idx_raw_items_source_id" not in _existing_indexes("raw_items"):
        op.create_index("idx_raw_items_source_id", "raw_items", ["source_id"], unique=False)

    if not _has_table("stories"):
        op.create_table(
            "stories",
            sa.Column("id", sa.Text(), primary_key=True),
            sa.Column("story_key", sa.Text(), nullable=False),
            sa.Column("title_best", sa.Text(), nullable=False),
            sa.Column("published_at", sa.Text(), nullable=True),
            sa.Column("source_group", sa.Text(), nullable=True),
            sa.Column("region", sa.Text(), nullable=True),
            sa.Column("trust_tier", sa.Text(), nullable=True),
            sa.Column("primary_raw_item_id", sa.Text(), nullable=True),
            sa.Column("sources_count", sa.Integer(), nullable=False, server_default=sa.text("1")),
            sa.UniqueConstraint("story_key", name="uq_stories_story_key"),
        )
    if "idx_stories_published_id" not in _existing_indexes("stories"):
        op.create_index("idx_stories_published_id", "stories", ["published_at", "id"], unique=False)
    if "idx_stories_story_key" not in _existing_indexes("stories"):
        op.create_index("idx_stories_story_key", "stories", ["story_key"], unique=False)

    if not _has_table("story_items"):
        op.create_table(
            "story_items",
            sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("story_id", sa.Text(), nullable=False),
            sa.Column("raw_item_id", sa.Text(), nullable=False),
            sa.Column("is_primary", sa.Integer(), nullable=False, server_default=sa.text("0")),
            sa.Column("rank", sa.Integer(), nullable=False, server_default=sa.text("0")),
            sa.UniqueConstraint("story_id", "raw_item_id", name="uq_story_items_story_raw"),
        )
    if "idx_story_items_story" not in _existing_indexes("story_items"):
        op.create_index("idx_story_items_story", "story_items", ["story_id", "rank"], unique=False)
    if "idx_story_items_raw" not in _existing_indexes("story_items"):
        op.create_index("idx_story_items_raw", "story_items", ["raw_item_id"], unique=False)


def downgrade() -> None:
    if _has_table("story_items"):
        if "idx_story_items_raw" in _existing_indexes("story_items"):
            op.drop_index("idx_story_items_raw", table_name="story_items")
        if "idx_story_items_story" in _existing_indexes("story_items"):
            op.drop_index("idx_story_items_story", table_name="story_items")
        op.drop_table("story_items")

    if _has_table("stories"):
        if "idx_stories_story_key" in _existing_indexes("stories"):
            op.drop_index("idx_stories_story_key", table_name="stories")
        if "idx_stories_published_id" in _existing_indexes("stories"):
            op.drop_index("idx_stories_published_id", table_name="stories")
        op.drop_table("stories")

    if _has_table("raw_items"):
        if "idx_raw_items_source_id" in _existing_indexes("raw_items"):
            op.drop_index("idx_raw_items_source_id", table_name="raw_items")
        if "idx_raw_items_canonical_url" in _existing_indexes("raw_items"):
            op.drop_index("idx_raw_items_canonical_url", table_name="raw_items")
        if "idx_raw_items_published_id" in _existing_indexes("raw_items"):
            op.drop_index("idx_raw_items_published_id", table_name="raw_items")
        op.drop_table("raw_items")
