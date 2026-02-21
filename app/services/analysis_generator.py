from __future__ import annotations

import datetime as dt
import json
import os
from typing import Any


def _to_iso_utc(value: dt.datetime | None = None) -> str:
    d = value or dt.datetime.now(dt.timezone.utc)
    if d.tzinfo is None:
        d = d.replace(tzinfo=dt.timezone.utc)
    return d.astimezone(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


class AnalysisGenerator:
    """
    Local generator wrapper used by digest path.
    Designed for future LLM integration while keeping deterministic fallback.
    """

    def __init__(
        self,
        *,
        model: str = "",
        prompt_version: str = "v2",
        primary_model: str = "",
        fallback_model: str = "",
        model_policy: str = "tiered",
        core_model: str = "primary",
        frontier_model: str = "fallback",
        temperature: float = 0.2,
        retries: int = 1,
        timeout_seconds: int = 20,
        backoff_seconds: float = 0.5,
    ) -> None:
        env_primary = str(os.environ.get("MODEL_PRIMARY", "")).strip()
        env_fallback = str(os.environ.get("MODEL_FALLBACK", "")).strip()
        env_policy = str(os.environ.get("MODEL_POLICY", "")).strip()
        default_model = str(model or "local-heuristic-v1").strip()
        self.primary_model = str(primary_model or env_primary or default_model or "local-heuristic-v1").strip()
        self.fallback_model = str(fallback_model or env_fallback or "local-lite-v1").strip()
        self.model_policy = str(model_policy or env_policy or "tiered").strip().lower()
        self.core_model = str(core_model or "primary").strip().lower()
        self.frontier_model = str(frontier_model or "fallback").strip().lower()
        self.temperature = max(0.0, min(1.0, float(temperature)))
        self.retries = max(0, int(retries))
        self.timeout_seconds = max(1, int(timeout_seconds))
        self.backoff_seconds = max(0.0, float(backoff_seconds))
        self.prompt_version = prompt_version

    def _resolve_model_name(self, alias: str) -> str:
        a = str(alias or "").strip().lower()
        if a == "primary":
            return self.primary_model
        if a == "fallback":
            return self.fallback_model
        return str(alias or "").strip() or self.fallback_model

    def _pick_preferred_model(self, item: dict[str, Any]) -> str:
        track = str(item.get("track", "")).strip().lower()
        level = int(item.get("relevance_level", 0) or 0)
        if self.model_policy != "tiered":
            return self.primary_model
        if track == "core" and level >= 3:
            return self._resolve_model_name(self.core_model)
        return self._resolve_model_name(self.frontier_model)

    def _simulate_model_text(self, *, model_name: str, item: dict[str, Any], attempt: int) -> str:
        fail_models = {x.strip() for x in str(os.environ.get("ANALYSIS_FAIL_MODELS", "")).split(",") if x.strip()}
        if model_name in fail_models:
            raise RuntimeError(f"model_failed:{model_name}")
        if str(os.environ.get("ANALYSIS_GENERATOR_FORCE_FAIL", "")).strip().lower() in {"1", "true", "yes", "on"}:
            raise RuntimeError("forced_analysis_failure")

        if attempt == 0 and str(os.environ.get("ANALYSIS_FORCE_BAD_JSON", "")).strip().lower() in {"1", "true", "yes", "on"}:
            return "{bad json"

        title = str(item.get("title", "")).strip()
        source = str(item.get("source", "")).strip()
        event_type = str(item.get("event_type", "")).strip() or "行业动态"
        level = int(item.get("relevance_level", 0) or 0)
        summary = f"摘要：{title[:120]}。"
        if source:
            summary = f"{summary[:-1]} 来源：{source}。"
        impact = f"影响：{event_type}，相关性等级 L{level}。"
        action = "建议：关注原始公告与准入进展，必要时补充次日跟踪。"
        return json.dumps(
            {
                "summary": summary,
                "impact": impact,
                "action": action,
            },
            ensure_ascii=False,
        )

    def _run_model(self, *, model_name: str, item: dict[str, Any]) -> dict[str, Any]:
        last_err = ""
        for attempt in range(self.retries + 1):
            try:
                raw = self._simulate_model_text(model_name=model_name, item=item, attempt=attempt)
                data = json.loads(raw)
                summary = str(data.get("summary", "")).strip()
                impact = str(data.get("impact", "")).strip()
                action = str(data.get("action", "")).strip()
                if not summary:
                    raise RuntimeError("empty_summary")
                text = f"{item.get('title','')} {item.get('event_type','')} {item.get('source','')}".strip()
                return {
                    "summary": summary,
                    "impact": impact,
                    "action": action,
                    "used_model": model_name,
                    "model": model_name,
                    "prompt_version": self.prompt_version,
                    "temperature": self.temperature,
                    "token_usage": {
                        "prompt_tokens": max(1, len(text) // 4),
                        "completion_tokens": max(8, len(summary + impact + action) // 4),
                        "total_tokens": max(9, (len(text) + len(summary + impact + action)) // 4),
                    },
                    "generated_at": _to_iso_utc(),
                    "degraded": False,
                    "degraded_reason": "",
                    "ok": True,
                }
            except Exception as e:
                last_err = str(e)
        raise RuntimeError(last_err or f"model_failed:{model_name}")

    def generate(self, item: dict[str, Any], rules: dict[str, Any] | None = None) -> dict[str, Any]:
        rules = rules or {}
        preferred = self._pick_preferred_model(item)
        fallback = self.fallback_model
        try:
            return self._run_model(model_name=preferred, item=item)
        except Exception:
            if preferred == fallback:
                raise
            out = self._run_model(model_name=fallback, item=item)
            out["fallback_from"] = preferred
            return out


def degraded_analysis(item: dict[str, Any], reason: str) -> dict[str, Any]:
    title = str(item.get("title", "")).strip()
    source = str(item.get("source", "")).strip()
    summary = f"摘要：{title[:100]}。"
    if source:
        summary = f"{summary[:-1]} 来源：{source}。"
    return {
        "summary": summary,
        "impact": "影响：分析服务不可用，已降级为粗摘要。",
        "action": "建议：稍后重试分析缓存生成。",
        "model": "degraded-fallback",
        "prompt_version": "fallback-v1",
        "token_usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
        "generated_at": _to_iso_utc(),
        "degraded": True,
        "degraded_reason": str(reason or "analysis_generation_failed"),
        "ok": False,
    }
