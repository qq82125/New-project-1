"""add send_key/run_key/dedupe_key db-level constraints

Revision ID: 20260219_0004
Revises: 20260219_0003
Create Date: 2026-02-19 23:30:00.000000
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260219_0004"
down_revision = "20260219_0003"
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


def _has_column(table_name: str, col_name: str) -> bool:
    bind = op.get_bind()
    insp = sa.inspect(bind)
    try:
        cols = insp.get_columns(table_name)
    except Exception:
        return False
    return any(str(c.get("name")) == col_name for c in cols)


def _existing_indexes(table_name: str) -> set[str]:
    bind = op.get_bind()
    insp = sa.inspect(bind)
    try:
        return {str(i.get("name", "")) for i in insp.get_indexes(table_name)}
    except Exception:
        return set()


def _create_index_if_missing(name: str, table_name: str, cols: list[str], *, unique: bool = False) -> None:
    if name in _existing_indexes(table_name):
        return
    op.create_index(name, table_name, cols, unique=unique)


def upgrade() -> None:
    if _has_table("run_executions"):
        if not _has_column("run_executions", "run_key"):
            with op.batch_alter_table("run_executions") as batch_op:
                batch_op.add_column(sa.Column("run_key", sa.Text(), nullable=True))
        op.execute("UPDATE run_executions SET run_key = run_id WHERE run_key IS NULL OR run_key = ''")
        _create_index_if_missing("uq_run_executions_run_key", "run_executions", ["run_key"], unique=True)

    if not _has_table("send_attempts"):
        op.create_table(
            "send_attempts",
            sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("send_key", sa.Text(), nullable=False),
            sa.Column("date", sa.Text(), nullable=False),
            sa.Column("subject", sa.Text(), nullable=False),
            sa.Column("to_email", sa.Text(), nullable=False),
            sa.Column("status", sa.Text(), nullable=False),
            sa.Column("error", sa.Text(), nullable=True),
            sa.Column("created_at", sa.Text(), nullable=False),
            sa.Column("run_id", sa.Text(), nullable=True),
            sa.UniqueConstraint("send_key", name="uq_send_attempts_send_key"),
        )
    _create_index_if_missing("idx_send_attempts_lookup", "send_attempts", ["date", "subject", "to_email", "created_at"])
    _create_index_if_missing("idx_send_attempts_created_at", "send_attempts", ["created_at"])

    if not _has_table("dedupe_keys"):
        op.create_table(
            "dedupe_keys",
            sa.Column("dedupe_key", sa.Text(), primary_key=True),
            sa.Column("run_id", sa.Text(), nullable=True),
            sa.Column("created_at", sa.Text(), nullable=False),
            sa.UniqueConstraint("dedupe_key", name="uq_dedupe_keys_dedupe_key"),
        )

    # If legacy send_attempts existed without send_key, attempt non-destructive add.
    if _has_table("send_attempts") and not _has_column("send_attempts", "send_key"):
        with op.batch_alter_table("send_attempts") as batch_op:
            batch_op.add_column(sa.Column("send_key", sa.Text(), nullable=True))
        # fallback key to keep rows queryable; unique index is created after backfill
        op.execute(
            "UPDATE send_attempts "
            "SET send_key = COALESCE(run_id, '') || ':' || CAST(id AS TEXT) "
            "WHERE send_key IS NULL OR send_key = ''"
        )
        _create_index_if_missing("uq_send_attempts_send_key", "send_attempts", ["send_key"], unique=True)


def downgrade() -> None:
    if _has_table("dedupe_keys"):
        op.drop_table("dedupe_keys")

    if _has_table("send_attempts"):
        idx = _existing_indexes("send_attempts")
        if "idx_send_attempts_lookup" in idx:
            op.drop_index("idx_send_attempts_lookup", table_name="send_attempts")
        if "idx_send_attempts_created_at" in idx:
            op.drop_index("idx_send_attempts_created_at", table_name="send_attempts")
        if "uq_send_attempts_send_key" in idx:
            op.drop_index("uq_send_attempts_send_key", table_name="send_attempts")
        op.drop_table("send_attempts")

    if _has_table("run_executions"):
        idx = _existing_indexes("run_executions")
        if "uq_run_executions_run_key" in idx:
            op.drop_index("uq_run_executions_run_key", table_name="run_executions")
        if _has_column("run_executions", "run_key"):
            with op.batch_alter_table("run_executions") as batch_op:
                batch_op.drop_column("run_key")
