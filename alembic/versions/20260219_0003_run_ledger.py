"""run ledger tables for scheduler/dryrun/live

Revision ID: 20260219_0003
Revises: 20260219_0002
Create Date: 2026-02-19 01:10:00.000000
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "20260219_0003"
down_revision = "20260219_0002"
branch_labels = None
depends_on = None


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
    if not _has_table("run_executions"):
        op.create_table(
            "run_executions",
            sa.Column("run_id", sa.Text(), primary_key=True),
            sa.Column("profile", sa.Text(), nullable=False),
            sa.Column("triggered_by", sa.Text(), nullable=False),
            sa.Column("window", sa.Text(), nullable=False, server_default=""),
            sa.Column("status", sa.Text(), nullable=False),
            sa.Column("started_at", sa.Text(), nullable=False),
            sa.Column("ended_at", sa.Text(), nullable=True),
        )
    _create_index_if_missing("idx_run_executions_started_at", "run_executions", ["started_at", "run_id"])
    _create_index_if_missing("idx_run_executions_status", "run_executions", ["status", "started_at"])

    if not _has_table("source_fetch_events"):
        op.create_table(
            "source_fetch_events",
            sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("run_id", sa.Text(), nullable=False),
            sa.Column("source_id", sa.Text(), nullable=False),
            sa.Column("status", sa.Text(), nullable=False),
            sa.Column("http_status", sa.Integer(), nullable=True),
            sa.Column("items_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
            sa.Column("error", sa.Text(), nullable=True),
            sa.Column("duration_ms", sa.Integer(), nullable=False, server_default=sa.text("0")),
        )
    _create_index_if_missing("idx_source_fetch_events_run", "source_fetch_events", ["run_id", "id"])
    _create_index_if_missing("idx_source_fetch_events_source_status", "source_fetch_events", ["source_id", "status", "id"])

    if not _has_table("report_artifacts"):
        op.create_table(
            "report_artifacts",
            sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("run_id", sa.Text(), nullable=False),
            sa.Column("artifact_path", sa.Text(), nullable=False),
            sa.Column("artifact_type", sa.Text(), nullable=False),
            sa.Column("sha256", sa.Text(), nullable=False),
            sa.Column("created_at", sa.Text(), nullable=False),
        )
    _create_index_if_missing("idx_report_artifacts_run", "report_artifacts", ["run_id", "id"])
    _create_index_if_missing("idx_report_artifacts_type", "report_artifacts", ["artifact_type", "created_at"])


def downgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)
    tables = set(insp.get_table_names())

    if "report_artifacts" in tables:
        idx = _existing_indexes("report_artifacts")
        if "idx_report_artifacts_type" in idx:
            op.drop_index("idx_report_artifacts_type", table_name="report_artifacts")
        if "idx_report_artifacts_run" in idx:
            op.drop_index("idx_report_artifacts_run", table_name="report_artifacts")
        op.drop_table("report_artifacts")

    if "source_fetch_events" in tables:
        idx = _existing_indexes("source_fetch_events")
        if "idx_source_fetch_events_source_status" in idx:
            op.drop_index("idx_source_fetch_events_source_status", table_name="source_fetch_events")
        if "idx_source_fetch_events_run" in idx:
            op.drop_index("idx_source_fetch_events_run", table_name="source_fetch_events")
        op.drop_table("source_fetch_events")

    if "run_executions" in tables:
        idx = _existing_indexes("run_executions")
        if "idx_run_executions_status" in idx:
            op.drop_index("idx_run_executions_status", table_name="run_executions")
        if "idx_run_executions_started_at" in idx:
            op.drop_index("idx_run_executions_started_at", table_name="run_executions")
        op.drop_table("run_executions")
