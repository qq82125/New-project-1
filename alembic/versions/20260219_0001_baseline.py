"""baseline

Revision ID: 20260219_0001
Revises:
Create Date: 2026-02-19 00:00:00.000000

"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "20260219_0001"
down_revision = None
branch_labels = None
depends_on = None


def _json_type() -> sa.TypeEngine:
    dialect_name = op.get_context().dialect.name
    if dialect_name == "postgresql":
        return postgresql.JSONB()
    return sa.Text()


def _has_table(table_name: str) -> bool:
    bind = op.get_bind()
    insp = sa.inspect(bind)
    return table_name in set(insp.get_table_names())


def _existing_indexes(table_name: str) -> set[str]:
    bind = op.get_bind()
    insp = sa.inspect(bind)
    try:
        return {str(i.get("name", "")) for i in insp.get_indexes(table_name)}
    except Exception:
        return set()


def _create_index_if_missing(name: str, table_name: str, cols: list[str]) -> None:
    if name in _existing_indexes(table_name):
        return
    op.create_index(name, table_name, cols, unique=False)


def upgrade() -> None:
    json_type = _json_type()

    if not _has_table("email_rules_versions"):
        op.create_table(
            "email_rules_versions",
            sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("profile", sa.Text(), nullable=False),
            sa.Column("version", sa.Text(), nullable=False),
            sa.Column("config_json", json_type, nullable=False),
            sa.Column("created_at", sa.Text(), nullable=False),
            sa.Column("created_by", sa.Text(), nullable=False),
            sa.Column("is_active", sa.Integer(), nullable=False, server_default=sa.text("0")),
            sa.UniqueConstraint("profile", "version", name="uq_email_rules_profile_version"),
        )
    _create_index_if_missing("idx_email_rules_profile_active", "email_rules_versions", ["profile", "is_active", "id"])

    if not _has_table("content_rules_versions"):
        op.create_table(
            "content_rules_versions",
            sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("profile", sa.Text(), nullable=False),
            sa.Column("version", sa.Text(), nullable=False),
            sa.Column("config_json", json_type, nullable=False),
            sa.Column("created_at", sa.Text(), nullable=False),
            sa.Column("created_by", sa.Text(), nullable=False),
            sa.Column("is_active", sa.Integer(), nullable=False, server_default=sa.text("0")),
            sa.UniqueConstraint("profile", "version", name="uq_content_rules_profile_version"),
        )
    _create_index_if_missing("idx_content_rules_profile_active", "content_rules_versions", ["profile", "is_active", "id"])

    if not _has_table("qc_rules_versions"):
        op.create_table(
            "qc_rules_versions",
            sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("profile", sa.Text(), nullable=False),
            sa.Column("version", sa.Text(), nullable=False),
            sa.Column("config_json", json_type, nullable=False),
            sa.Column("created_at", sa.Text(), nullable=False),
            sa.Column("created_by", sa.Text(), nullable=False),
            sa.Column("is_active", sa.Integer(), nullable=False, server_default=sa.text("0")),
            sa.UniqueConstraint("profile", "version", name="uq_qc_rules_profile_version"),
        )
    _create_index_if_missing("idx_qc_rules_profile_active", "qc_rules_versions", ["profile", "is_active", "id"])

    if not _has_table("output_rules_versions"):
        op.create_table(
            "output_rules_versions",
            sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("profile", sa.Text(), nullable=False),
            sa.Column("version", sa.Text(), nullable=False),
            sa.Column("config_json", json_type, nullable=False),
            sa.Column("created_at", sa.Text(), nullable=False),
            sa.Column("created_by", sa.Text(), nullable=False),
            sa.Column("is_active", sa.Integer(), nullable=False, server_default=sa.text("0")),
            sa.UniqueConstraint("profile", "version", name="uq_output_rules_profile_version"),
        )
    _create_index_if_missing("idx_output_rules_profile_active", "output_rules_versions", ["profile", "is_active", "id"])

    if not _has_table("scheduler_rules_versions"):
        op.create_table(
            "scheduler_rules_versions",
            sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("profile", sa.Text(), nullable=False),
            sa.Column("version", sa.Text(), nullable=False),
            sa.Column("config_json", json_type, nullable=False),
            sa.Column("created_at", sa.Text(), nullable=False),
            sa.Column("created_by", sa.Text(), nullable=False),
            sa.Column("is_active", sa.Integer(), nullable=False, server_default=sa.text("0")),
            sa.UniqueConstraint("profile", "version", name="uq_scheduler_rules_profile_version"),
        )
    _create_index_if_missing("idx_scheduler_rules_profile_active", "scheduler_rules_versions", ["profile", "is_active", "id"])

    if not _has_table("rules_drafts"):
        op.create_table(
            "rules_drafts",
            sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("ruleset", sa.Text(), nullable=False),
            sa.Column("profile", sa.Text(), nullable=False),
            sa.Column("config_json", json_type, nullable=False),
            sa.Column("validation_json", json_type, nullable=False),
            sa.Column("created_at", sa.Text(), nullable=False),
            sa.Column("created_by", sa.Text(), nullable=False),
        )
    _create_index_if_missing("idx_rules_drafts_lookup", "rules_drafts", ["ruleset", "profile", "id"])

    if not _has_table("sources"):
        op.create_table(
            "sources",
            sa.Column("id", sa.Text(), primary_key=True),
            sa.Column("name", sa.Text(), nullable=False),
            sa.Column("connector", sa.Text(), nullable=False),
            sa.Column("url", sa.Text(), nullable=True),
            sa.Column("enabled", sa.Integer(), nullable=False, server_default=sa.text("1")),
            sa.Column("priority", sa.Integer(), nullable=False, server_default=sa.text("0")),
            sa.Column("trust_tier", sa.Text(), nullable=False),
            sa.Column("tags_json", json_type, nullable=False),
            sa.Column("rate_limit_json", json_type, nullable=False),
            sa.Column("fetch_json", json_type, nullable=False),
            sa.Column("parsing_json", json_type, nullable=False),
            sa.Column("created_at", sa.Text(), nullable=False),
            sa.Column("updated_at", sa.Text(), nullable=False),
            sa.Column("last_fetched_at", sa.Text(), nullable=True),
            sa.Column("last_fetch_status", sa.Text(), nullable=True),
            sa.Column("last_fetch_http_status", sa.Integer(), nullable=True),
            sa.Column("last_fetch_error", sa.Text(), nullable=True),
            sa.Column("last_success_at", sa.Text(), nullable=True),
            sa.Column("last_http_status", sa.Integer(), nullable=True),
            sa.Column("last_error", sa.Text(), nullable=True),
        )
    _create_index_if_missing("idx_sources_enabled_priority", "sources", ["enabled", "priority"])


def downgrade() -> None:
    # Baseline downgrade intentionally drops all managed tables.
    bind = op.get_bind()
    insp = sa.inspect(bind)
    tables = set(insp.get_table_names())

    if "sources" in tables:
        if "idx_sources_enabled_priority" in _existing_indexes("sources"):
            op.drop_index("idx_sources_enabled_priority", table_name="sources")
        op.drop_table("sources")

    if "rules_drafts" in tables:
        if "idx_rules_drafts_lookup" in _existing_indexes("rules_drafts"):
            op.drop_index("idx_rules_drafts_lookup", table_name="rules_drafts")
        op.drop_table("rules_drafts")

    if "scheduler_rules_versions" in tables:
        if "idx_scheduler_rules_profile_active" in _existing_indexes("scheduler_rules_versions"):
            op.drop_index("idx_scheduler_rules_profile_active", table_name="scheduler_rules_versions")
        op.drop_table("scheduler_rules_versions")

    if "output_rules_versions" in tables:
        if "idx_output_rules_profile_active" in _existing_indexes("output_rules_versions"):
            op.drop_index("idx_output_rules_profile_active", table_name="output_rules_versions")
        op.drop_table("output_rules_versions")

    if "qc_rules_versions" in tables:
        if "idx_qc_rules_profile_active" in _existing_indexes("qc_rules_versions"):
            op.drop_index("idx_qc_rules_profile_active", table_name="qc_rules_versions")
        op.drop_table("qc_rules_versions")

    if "content_rules_versions" in tables:
        if "idx_content_rules_profile_active" in _existing_indexes("content_rules_versions"):
            op.drop_index("idx_content_rules_profile_active", table_name="content_rules_versions")
        op.drop_table("content_rules_versions")

    if "email_rules_versions" in tables:
        if "idx_email_rules_profile_active" in _existing_indexes("email_rules_versions"):
            op.drop_index("idx_email_rules_profile_active", table_name="email_rules_versions")
        op.drop_table("email_rules_versions")
