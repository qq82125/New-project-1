from __future__ import annotations

import io
import json
from pathlib import Path

from app.services.zh_enricher import ZhEnricher


def test_enrich_heuristic_without_evidence(tmp_path: Path) -> None:
    z = ZhEnricher(tmp_path)
    out = z.enrich(
        title="Company reports quarterly earnings",
        snippet="",
        source_id="globenewswire-press-rss",
        url="https://example.com/a",
        event_type="earnings_noise",
    )
    assert "业绩" in str(out.get("title_zh", ""))
    assert "建议打开原文" in str(out.get("summary_zh", ""))
    assert str(out.get("degraded_reason", "")) in {"missing_evidence", "llm_unavailable_or_failed"}


def test_enrich_cache_hit(tmp_path: Path) -> None:
    z = ZhEnricher(tmp_path)
    kwargs = dict(
        title="BigCo to acquire LabTech",
        snippet="The company announced a merger agreement and expects closing in Q2.",
        source_id="prnewswire-press-rss",
        url="https://example.com/b",
        event_type="m_and_a",
    )
    out1 = z.enrich(**kwargs)
    out2 = z.enrich(**kwargs)
    assert bool(out1.get("cache_hit")) is False
    assert bool(out2.get("cache_hit")) is True
    assert str(out2.get("title_zh", "")).strip()
    assert str(out2.get("summary_zh", "")).strip()


def test_enrich_preserves_cjk_snippet(tmp_path: Path) -> None:
    z = ZhEnricher(tmp_path)
    out = z.enrich(
        title="English title only",
        snippet="该公司宣布新一代分子诊断产品将于下季度上市。",
        source_id="example-source",
        url="https://example.com/c",
        event_type="product_launch",
    )
    assert "该公司宣布新一代分子诊断产品" in str(out.get("summary_zh", ""))


class _DummyResponse(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_enrich_uses_gemini_provider(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("FEED_ZH_LLM_ENABLED", "1")
    monkeypatch.setenv("FEED_ZH_PROVIDER", "gemini")
    monkeypatch.setenv("FEED_ZH_LLM_URL", "https://generativelanguage.googleapis.com/v1beta")
    monkeypatch.setenv("FEED_ZH_LLM_API_KEY", "k")
    monkeypatch.setenv("FEED_ZH_LLM_MODEL", "gemini-1.5-flash")

    def _fake_urlopen(req, timeout=15):
        assert "generateContent" in str(req.full_url)
        payload = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {
                                "text": json.dumps(
                                    {"title_zh": "中文标题", "summary_zh": "中文摘要"},
                                    ensure_ascii=False,
                                )
                            }
                        ]
                    }
                }
            ]
        }
        return _DummyResponse(json.dumps(payload).encode("utf-8"))

    monkeypatch.setattr("app.services.zh_enricher.request.urlopen", _fake_urlopen)
    z = ZhEnricher(tmp_path)
    z.begin_request()
    out = z.enrich(
        title="Test title",
        snippet="This is enough evidence for model calling.",
        source_id="test-source",
        url="https://example.com/g",
        event_type="regulatory",
    )
    assert out.get("title_zh") == "中文标题"
    assert out.get("summary_zh") == "中文摘要"
    assert out.get("used_model") == "gemini-1.5-flash"


def test_enrich_uses_anthropic_provider(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("FEED_ZH_LLM_ENABLED", "1")
    monkeypatch.setenv("FEED_ZH_PROVIDER", "anthropic")
    monkeypatch.setenv("FEED_ZH_LLM_URL", "https://api.anthropic.com/v1/messages")
    monkeypatch.setenv("FEED_ZH_LLM_API_KEY", "k")
    monkeypatch.setenv("FEED_ZH_LLM_MODEL", "claude-3-5-haiku-latest")

    def _fake_urlopen(req, timeout=15):
        payload = {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps({"title_zh": "克劳德标题", "summary_zh": "克劳德摘要"}, ensure_ascii=False),
                }
            ]
        }
        return _DummyResponse(json.dumps(payload).encode("utf-8"))

    monkeypatch.setattr("app.services.zh_enricher.request.urlopen", _fake_urlopen)
    z = ZhEnricher(tmp_path)
    z.begin_request()
    out = z.enrich(
        title="Test title",
        snippet="This is enough evidence for model calling.",
        source_id="test-source",
        url="https://example.com/a",
        event_type="regulatory",
    )
    assert out.get("title_zh") == "克劳德标题"
    assert out.get("summary_zh") == "克劳德摘要"


def test_openai_compatible_supports_x_api_key_header(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("FEED_ZH_LLM_ENABLED", "1")
    monkeypatch.setenv("FEED_ZH_PROVIDER", "openai")
    monkeypatch.setenv("FEED_ZH_LLM_URL", "https://example-proxy/v1/chat/completions")
    monkeypatch.setenv("FEED_ZH_LLM_API_KEY", "proxy-key")
    monkeypatch.setenv("FEED_ZH_LLM_MODEL", "gemini-3-flash-preview")
    monkeypatch.setenv("FEED_ZH_LLM_AUTH_HEADER", "x-api-key")
    monkeypatch.setenv("FEED_ZH_LLM_AUTH_SCHEME", "raw")

    def _fake_urlopen(req, timeout=15):
        headers = {str(k).lower(): str(v) for k, v in req.header_items()}
        assert headers.get("x-api-key") == "proxy-key"
        payload = {
            "choices": [
                {
                    "message": {
                        "content": json.dumps({"title_zh": "代理标题", "summary_zh": "代理摘要"}, ensure_ascii=False)
                    }
                }
            ]
        }
        return _DummyResponse(json.dumps(payload).encode("utf-8"))

    monkeypatch.setattr("app.services.zh_enricher.request.urlopen", _fake_urlopen)
    z = ZhEnricher(tmp_path)
    z.begin_request()
    out = z.enrich(
        title="Proxy title",
        snippet="This is enough evidence for proxy call.",
        source_id="proxy-source",
        url="https://example.com/proxy",
        event_type="regulatory",
    )
    assert out.get("title_zh") == "代理标题"
    assert out.get("summary_zh") == "代理摘要"
