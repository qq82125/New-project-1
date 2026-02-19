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
