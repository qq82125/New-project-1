"""dual/shadow observability tables

Revision ID: 20260219_0002
Revises: 20260219_0001
Create Date: 2026-02-19 00:30:00.000000

"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260219_0002"
down_revision = "20260219_0001"
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

    if not _has_table("dual_write_failures"):
        op.create_table(
            "dual_write_failures",
            sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("op_name", sa.Text(), nullable=False),
            sa.Column("payload_json", json_type, nullable=False),
            sa.Column("error", sa.Text(), nullable=False),
            sa.Column("created_at", sa.Text(), nullable=False),
        )
    _create_index_if_missing(
        "idx_dual_write_failures_created_at",
        "dual_write_failures",
        ["created_at", "id"],
    )

    if not _has_table("db_compare_log"):
        op.create_table(
            "db_compare_log",
            sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("query_name", sa.Text(), nullable=False),
            sa.Column("params_hash", sa.Text(), nullable=False),
            sa.Column("diff_summary", sa.Text(), nullable=False),
            sa.Column("created_at", sa.Text(), nullable=False),
        )
    _create_index_if_missing("idx_db_compare_log_created_at", "db_compare_log", ["created_at", "id"])
    _create_index_if_missing("idx_db_compare_log_query", "db_compare_log", ["query_name", "created_at"])


def downgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)
    tables = set(insp.get_table_names())

    if "db_compare_log" in tables:
        idx = _existing_indexes("db_compare_log")
        if "idx_db_compare_log_query" in idx:
            op.drop_index("idx_db_compare_log_query", table_name="db_compare_log")
        if "idx_db_compare_log_created_at" in idx:
            op.drop_index("idx_db_compare_log_created_at", table_name="db_compare_log")
        op.drop_table("db_compare_log")

    if "dual_write_failures" in tables:
        idx = _existing_indexes("dual_write_failures")
        if "idx_dual_write_failures_created_at" in idx:
            op.drop_index("idx_dual_write_failures_created_at", table_name="dual_write_failures")
        op.drop_table("dual_write_failures")
