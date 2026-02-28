from __future__ import annotations

from typing import Any, Optional

from sqlalchemy import Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.db.types import JSONText


class _RulesVersionMixin:
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    profile: Mapped[str] = mapped_column(String, nullable=False)
    version: Mapped[str] = mapped_column(String, nullable=False)
    config_json: Mapped[dict[str, Any]] = mapped_column(JSONText(), nullable=False)
    created_at: Mapped[str] = mapped_column(String, nullable=False)
    created_by: Mapped[str] = mapped_column(String, nullable=False)
    is_active: Mapped[int] = mapped_column(Integer, nullable=False, default=0)


class EmailRulesVersion(_RulesVersionMixin, Base):
    __tablename__ = "email_rules_versions"
    __table_args__ = (
        UniqueConstraint("profile", "version", name="uq_email_rules_profile_version"),
        Index("idx_email_rules_profile_active", "profile", "is_active", "id"),
    )


class ContentRulesVersion(_RulesVersionMixin, Base):
    __tablename__ = "content_rules_versions"
    __table_args__ = (
        UniqueConstraint("profile", "version", name="uq_content_rules_profile_version"),
        Index("idx_content_rules_profile_active", "profile", "is_active", "id"),
    )


class QcRulesVersion(_RulesVersionMixin, Base):
    __tablename__ = "qc_rules_versions"
    __table_args__ = (
        UniqueConstraint("profile", "version", name="uq_qc_rules_profile_version"),
        Index("idx_qc_rules_profile_active", "profile", "is_active", "id"),
    )


class OutputRulesVersion(_RulesVersionMixin, Base):
    __tablename__ = "output_rules_versions"
    __table_args__ = (
        UniqueConstraint("profile", "version", name="uq_output_rules_profile_version"),
        Index("idx_output_rules_profile_active", "profile", "is_active", "id"),
    )


class SchedulerRulesVersion(_RulesVersionMixin, Base):
    __tablename__ = "scheduler_rules_versions"
    __table_args__ = (
        UniqueConstraint("profile", "version", name="uq_scheduler_rules_profile_version"),
        Index("idx_scheduler_rules_profile_active", "profile", "is_active", "id"),
    )


class RulesDraft(Base):
    __tablename__ = "rules_drafts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ruleset: Mapped[str] = mapped_column(String, nullable=False)
    profile: Mapped[str] = mapped_column(String, nullable=False)
    config_json: Mapped[dict[str, Any]] = mapped_column(JSONText(), nullable=False)
    validation_json: Mapped[list[dict[str, Any]]] = mapped_column(JSONText(), nullable=False, default=list)
    created_at: Mapped[str] = mapped_column(String, nullable=False)
    created_by: Mapped[str] = mapped_column(String, nullable=False)

    __table_args__ = (
        Index("idx_rules_drafts_lookup", "ruleset", "profile", "id"),
    )


class Source(Base):
    __tablename__ = "sources"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    connector: Mapped[str] = mapped_column(String, nullable=False)
    url: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    enabled: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    trust_tier: Mapped[str] = mapped_column(String, nullable=False)
    tags_json: Mapped[list[Any]] = mapped_column(JSONText(), nullable=False, default=list)
    rate_limit_json: Mapped[dict[str, Any]] = mapped_column(JSONText(), nullable=False, default=dict)
    fetch_json: Mapped[dict[str, Any]] = mapped_column(JSONText(), nullable=False, default=dict)
    parsing_json: Mapped[dict[str, Any]] = mapped_column(JSONText(), nullable=False, default=dict)
    created_at: Mapped[str] = mapped_column(String, nullable=False)
    updated_at: Mapped[str] = mapped_column(String, nullable=False)

    last_fetched_at: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    last_fetch_status: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    last_fetch_http_status: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    last_fetch_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    last_success_at: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    last_http_status: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    last_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    __table_args__ = (
        Index("idx_sources_enabled_priority", "enabled", "priority"),
    )


class DualWriteFailure(Base):
    __tablename__ = "dual_write_failures"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    op_name: Mapped[str] = mapped_column(String, nullable=False)
    payload_json: Mapped[dict[str, Any]] = mapped_column(JSONText(), nullable=False, default=dict)
    error: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[str] = mapped_column(String, nullable=False)

    __table_args__ = (
        Index("idx_dual_write_failures_created_at", "created_at", "id"),
    )


class DBCompareLog(Base):
    __tablename__ = "db_compare_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    query_name: Mapped[str] = mapped_column(String, nullable=False)
    params_hash: Mapped[str] = mapped_column(String, nullable=False)
    diff_summary: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[str] = mapped_column(String, nullable=False)

    __table_args__ = (
        Index("idx_db_compare_log_created_at", "created_at", "id"),
        Index("idx_db_compare_log_query", "query_name", "created_at"),
    )


class RunExecution(Base):
    __tablename__ = "run_executions"

    run_id: Mapped[str] = mapped_column(String, primary_key=True)
    run_key: Mapped[str] = mapped_column(String, nullable=False)
    profile: Mapped[str] = mapped_column(String, nullable=False)
    triggered_by: Mapped[str] = mapped_column(String, nullable=False)
    window: Mapped[str] = mapped_column(String, nullable=False, default="")
    status: Mapped[str] = mapped_column(String, nullable=False)
    started_at: Mapped[str] = mapped_column(String, nullable=False)
    ended_at: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    __table_args__ = (
        UniqueConstraint("run_key", name="uq_run_executions_run_key"),
        Index("idx_run_executions_started_at", "started_at", "run_id"),
        Index("idx_run_executions_status", "status", "started_at"),
    )


class SourceFetchEvent(Base):
    __tablename__ = "source_fetch_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(String, nullable=False)
    source_id: Mapped[str] = mapped_column(String, nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False)
    http_status: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    items_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    duration_ms: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    __table_args__ = (
        Index("idx_source_fetch_events_run", "run_id", "id"),
        Index("idx_source_fetch_events_source_status", "source_id", "status", "id"),
    )


class ReportArtifact(Base):
    __tablename__ = "report_artifacts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(String, nullable=False)
    artifact_path: Mapped[str] = mapped_column(Text, nullable=False)
    artifact_type: Mapped[str] = mapped_column(String, nullable=False)
    sha256: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[str] = mapped_column(String, nullable=False)

    __table_args__ = (
        Index("idx_report_artifacts_run", "run_id", "id"),
        Index("idx_report_artifacts_type", "artifact_type", "created_at"),
    )


class DedupeKey(Base):
    __tablename__ = "dedupe_keys"

    dedupe_key: Mapped[str] = mapped_column(String, primary_key=True)
    run_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    created_at: Mapped[str] = mapped_column(String, nullable=False)

    __table_args__ = (
        UniqueConstraint("dedupe_key", name="uq_dedupe_keys_dedupe_key"),
    )


class SendAttempt(Base):
    __tablename__ = "send_attempts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    send_key: Mapped[str] = mapped_column(String, nullable=False)
    date: Mapped[str] = mapped_column(String, nullable=False)
    subject: Mapped[str] = mapped_column(String, nullable=False)
    to_email: Mapped[str] = mapped_column(String, nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False)
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[str] = mapped_column(String, nullable=False)
    run_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    __table_args__ = (
        UniqueConstraint("send_key", name="uq_send_attempts_send_key"),
        Index("idx_send_attempts_lookup", "date", "subject", "to_email", "created_at"),
        Index("idx_send_attempts_created_at", "created_at"),
    )


class RawItem(Base):
    __tablename__ = "raw_items"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    source_id: Mapped[str] = mapped_column(String, nullable=False)
    fetched_at: Mapped[str] = mapped_column(String, nullable=False)
    published_at: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    title_raw: Mapped[str] = mapped_column(Text, nullable=False)
    title_norm: Mapped[str] = mapped_column(Text, nullable=False)
    url_raw: Mapped[str] = mapped_column(Text, nullable=False)
    canonical_url: Mapped[str] = mapped_column(Text, nullable=False)
    content_snippet: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    raw_payload: Mapped[dict[str, Any]] = mapped_column(JSONText(), nullable=False, default=dict)
    source_group: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    region: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    trust_tier: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    event_type: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    __table_args__ = (
        Index("idx_raw_items_published_id", "published_at", "id"),
        Index("idx_raw_items_canonical_url", "canonical_url"),
        Index("idx_raw_items_source_id", "source_id"),
    )


class Story(Base):
    __tablename__ = "stories"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    story_key: Mapped[str] = mapped_column(Text, nullable=False)
    title_best: Mapped[str] = mapped_column(Text, nullable=False)
    published_at: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    source_group: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    region: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    trust_tier: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    event_type: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    primary_raw_item_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    sources_count: Mapped[int] = mapped_column(Integer, nullable=False, default=1)

    __table_args__ = (
        UniqueConstraint("story_key", name="uq_stories_story_key"),
        Index("idx_stories_published_id", "published_at", "id"),
        Index("idx_stories_story_key", "story_key"),
    )


class StoryItem(Base):
    __tablename__ = "story_items"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    story_id: Mapped[str] = mapped_column(String, nullable=False)
    raw_item_id: Mapped[str] = mapped_column(String, nullable=False)
    is_primary: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    rank: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    __table_args__ = (
        UniqueConstraint("story_id", "raw_item_id", name="uq_story_items_story_raw"),
        Index("idx_story_items_story", "story_id", "rank"),
        Index("idx_story_items_raw", "raw_item_id"),
    )
