from __future__ import annotations

import hashlib
import json
import os
import re
import threading
from pathlib import Path
from typing import Any
from urllib import request

from app.utils.url_norm import url_norm


def _contains_cjk(text: str) -> bool:
    s = str(text or "")
    for ch in s:
        if "\u4e00" <= ch <= "\u9fff":
            return True
    return False


def _compact_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def _first_sentence(text: str, *, max_chars: int = 180) -> str:
    s = _compact_spaces(text)
    if not s:
        return ""
    parts = re.split(r"(?<=[\.\!\?。！？；;])\s+", s)
    first = parts[0] if parts else s
    return first[: max(30, int(max_chars))]


def _heuristic_title_zh(title: str, event_type: str = "") -> str:
    t = _compact_spaces(title)
    if not t:
        return ""
    if _contains_cjk(t):
        return t

    repl = [
        (r"\breports?\s+results?\b", "发布业绩"),
        (r"\bfinancial\s+results?\b", "财务结果"),
        (r"\bquarterly\s+earnings\b|\bearnings\b", "业绩"),
        (r"\breceives?\b", "收到"),
        (r"\bnotification\b", "通知"),
        (r"\bannounces?\b", "宣布"),
        (r"\blaunch(es|ed)?\b", "发布"),
        (r"\bacquires?\b|\bacquisition\b", "并购"),
        (r"\bapproval\b|\bapproved\b|\bclearance\b|\bcleared\b", "获批"),
        (r"\bclinical\s+trial\b|\bclinical\s+study\b", "临床研究"),
        (r"\brecall\b", "召回"),
    ]
    out = t
    for pat, zh in repl:
        out = re.sub(pat, zh, out, flags=re.IGNORECASE)
    if out != t:
        return out

    et = str(event_type or "").strip().lower()
    prefix = {
        "regulatory": "监管动态",
        "approval_clearance": "审批进展",
        "procurement": "招采动态",
        "company_update": "企业动态",
        "funding": "融资动态",
        "m_and_a": "并购动态",
        "clinical_trial": "临床进展",
        "product_launch": "产品发布",
        "market_report": "市场报告",
    }.get(et, "行业动态")
    return f"{prefix}：{t}"


def _heuristic_summary_zh(title: str, snippet: str, source_id: str = "", event_type: str = "") -> str:
    sn = _compact_spaces(snippet)
    if _contains_cjk(sn):
        return _first_sentence(sn, max_chars=180)
    if sn:
        lead = _first_sentence(sn, max_chars=180)
        if lead:
            return f"要点：{lead}（原文英文，建议点开原文核查细节）"
    tz = _heuristic_title_zh(title, event_type=event_type)
    src = _compact_spaces(source_id)
    if tz and src:
        return f"该条来自 {src}，核心主题为“{tz}”。建议打开原文核对关键数据与时间点。"
    if tz:
        return f"核心主题为“{tz}”。建议打开原文核对关键数据与时间点。"
    return "暂无可用摘要，建议打开原文查看。"


class ZhEnricher:
    def __init__(self, project_root: Path, *, prompt_version: str = "zh-v1") -> None:
        self.project_root = project_root
        self.prompt_version = prompt_version
        self.cache_file = (project_root / "artifacts" / "zh_enrichment" / "cache.jsonl").resolve()
        self.cache_file.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._cache: dict[str, dict[str, Any]] = {}
        self._loaded = False
        self.llm_enabled = str(os.environ.get("FEED_ZH_LLM_ENABLED", "")).strip().lower() in {"1", "true", "yes", "on"}
        self.llm_provider = str(os.environ.get("FEED_ZH_PROVIDER", "openai")).strip().lower() or "openai"
        self.llm_url = str(os.environ.get("FEED_ZH_LLM_URL", "")).strip()
        self.llm_api_key = str(os.environ.get("FEED_ZH_LLM_API_KEY", "")).strip()
        self.llm_model = str(os.environ.get("FEED_ZH_LLM_MODEL", "")).strip() or "gpt-4o-mini"
        self.llm_auth_header = str(os.environ.get("FEED_ZH_LLM_AUTH_HEADER", "Authorization")).strip() or "Authorization"
        self.llm_auth_scheme = str(os.environ.get("FEED_ZH_LLM_AUTH_SCHEME", "bearer")).strip().lower() or "bearer"
        self.anthropic_version = str(os.environ.get("FEED_ZH_ANTHROPIC_VERSION", "2023-06-01")).strip() or "2023-06-01"
        self.max_per_request = max(1, min(100, int(os.environ.get("FEED_ZH_LLM_MAX_PER_REQUEST", "30") or 30)))
        self.min_snippet_chars = max(1, min(500, int(os.environ.get("FEED_ZH_MIN_SNIPPET_CHARS", "20") or 20)))
        self.min_title_chars = max(1, min(200, int(os.environ.get("FEED_ZH_MIN_TITLE_CHARS", "12") or 12)))
        self._request_budget = 0

    def begin_request(self) -> None:
        self._request_budget = int(self.max_per_request)

    def _load_cache(self) -> None:
        if self._loaded:
            return
        self._loaded = True
        if not self.cache_file.exists():
            return
        try:
            with self.cache_file.open("r", encoding="utf-8") as f:
                for ln in f:
                    ln = ln.strip()
                    if not ln:
                        continue
                    try:
                        row = json.loads(ln)
                    except Exception:
                        continue
                    k = str(row.get("cache_key", "")).strip()
                    if not k:
                        continue
                    self._cache[k] = row
        except Exception:
            return

    def _put_cache(self, key: str, payload: dict[str, Any]) -> None:
        row = dict(payload or {})
        row["cache_key"] = key
        with self._lock:
            self._cache[key] = row
            try:
                with self.cache_file.open("a", encoding="utf-8") as f:
                    f.write(json.dumps(row, ensure_ascii=False) + "\n")
            except Exception:
                pass

    def _cache_key(self, *, title: str, snippet: str, url: str) -> str:
        u = url_norm(str(url or ""))
        content_hash = hashlib.sha1((str(title or "") + "|" + str(snippet or "")).encode("utf-8")).hexdigest()
        seed = f"{u}|{content_hash}|{self.prompt_version}"
        return hashlib.sha1(seed.encode("utf-8")).hexdigest()

    def _extract_json(self, raw_text: str) -> dict[str, Any] | None:
        content = str(raw_text or "").strip()
        if not content:
            return None
        if content.startswith("```"):
            content = re.sub(r"^```[a-zA-Z]*\n?", "", content)
            content = re.sub(r"\n?```$", "", content)
        try:
            data = json.loads(content)
            if isinstance(data, dict):
                return data
        except Exception:
            pass
        m = re.search(r"\{.*\}", content, flags=re.S)
        if m:
            try:
                data = json.loads(m.group(0))
                if isinstance(data, dict):
                    return data
            except Exception:
                return None
        return None

    def _call_openai(self, user_prompt: str) -> dict[str, str] | None:
        if not self.llm_url:
            return None
        body = {
            "model": self.llm_model,
            "temperature": 0.2,
            "messages": [
                {"role": "system", "content": "你是严谨的医疗资讯编辑，只输出合法 JSON。"},
                {"role": "user", "content": user_prompt},
            ],
        }
        auth_val = self.llm_api_key
        if self.llm_auth_scheme in {"bearer", "token"}:
            auth_val = f"Bearer {self.llm_api_key}"
        req = request.Request(
            self.llm_url,
            method="POST",
            data=json.dumps(body).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                self.llm_auth_header: auth_val,
            },
        )
        with request.urlopen(req, timeout=15) as resp:
            raw = resp.read().decode("utf-8", errors="ignore")
        obj = json.loads(raw)
        content = obj.get("choices", [{}])[0].get("message", {}).get("content", "") if isinstance(obj, dict) else ""
        data = self._extract_json(str(content))
        if not data:
            return None
        title_zh = _compact_spaces(str(data.get("title_zh", "")).strip())
        summary_zh = _compact_spaces(str(data.get("summary_zh", "")).strip())
        if not title_zh and not summary_zh:
            return None
        return {"title_zh": title_zh, "summary_zh": summary_zh}

    def _call_anthropic(self, user_prompt: str) -> dict[str, str] | None:
        if not self.llm_url:
            return None
        body = {
            "model": self.llm_model,
            "temperature": 0.2,
            "max_tokens": 300,
            "system": "你是严谨的医疗资讯编辑，只输出合法 JSON。",
            "messages": [{"role": "user", "content": user_prompt}],
        }
        req = request.Request(
            self.llm_url,
            method="POST",
            data=json.dumps(body).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "x-api-key": self.llm_api_key,
                "anthropic-version": self.anthropic_version,
            },
        )
        with request.urlopen(req, timeout=15) as resp:
            raw = resp.read().decode("utf-8", errors="ignore")
        obj = json.loads(raw)
        blocks = obj.get("content", []) if isinstance(obj, dict) else []
        text = ""
        if isinstance(blocks, list):
            for block in blocks:
                if isinstance(block, dict) and str(block.get("type", "")).strip() == "text":
                    text = str(block.get("text", "")).strip()
                    if text:
                        break
        data = self._extract_json(text)
        if not data:
            return None
        title_zh = _compact_spaces(str(data.get("title_zh", "")).strip())
        summary_zh = _compact_spaces(str(data.get("summary_zh", "")).strip())
        if not title_zh and not summary_zh:
            return None
        return {"title_zh": title_zh, "summary_zh": summary_zh}

    def _gemini_url(self) -> str:
        base = str(self.llm_url or "").strip()
        if not base:
            return ""
        if ":generateContent" in base:
            if "key=" in base:
                return base
            sep = "&" if "?" in base else "?"
            return f"{base}{sep}key={self.llm_api_key}"
        base = base.rstrip("/")
        return f"{base}/models/{self.llm_model}:generateContent?key={self.llm_api_key}"

    def _call_gemini(self, user_prompt: str) -> dict[str, str] | None:
        url = self._gemini_url()
        if not url:
            return None
        body = {
            "generationConfig": {"temperature": 0.2},
            "contents": [{"role": "user", "parts": [{"text": user_prompt}]}],
        }
        req = request.Request(
            url,
            method="POST",
            data=json.dumps(body).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        with request.urlopen(req, timeout=15) as resp:
            raw = resp.read().decode("utf-8", errors="ignore")
        obj = json.loads(raw)
        cands = obj.get("candidates", []) if isinstance(obj, dict) else []
        text = ""
        if isinstance(cands, list) and cands:
            parts = cands[0].get("content", {}).get("parts", [])
            if isinstance(parts, list):
                for part in parts:
                    if isinstance(part, dict):
                        text = str(part.get("text", "")).strip()
                        if text:
                            break
        data = self._extract_json(text)
        if not data:
            return None
        title_zh = _compact_spaces(str(data.get("title_zh", "")).strip())
        summary_zh = _compact_spaces(str(data.get("summary_zh", "")).strip())
        if not title_zh and not summary_zh:
            return None
        return {"title_zh": title_zh, "summary_zh": summary_zh}

    def _call_llm(self, *, title: str, snippet: str, source_id: str, event_type: str) -> dict[str, str] | None:
        if not self.llm_enabled or not self.llm_url or not self.llm_api_key:
            return None
        if self._request_budget <= 0:
            return None
        self._request_budget -= 1

        user_prompt = (
            "请将以下英文医疗资讯转换为中文展示内容。\n"
            "要求：\n"
            "1) title_zh：中文标题，简洁准确，不夸张，不添加原文没有的信息。\n"
            "2) summary_zh：2句以内，事实摘要，中文输出。\n"
            "3) 若证据不足，summary_zh 必须写“证据不足，需打开原文核查”。\n"
            "仅返回 JSON：{\"title_zh\":\"...\",\"summary_zh\":\"...\"}\n\n"
            f"event_type: {event_type}\nsource_id: {source_id}\n"
            f"title: {title}\n"
            f"snippet: {snippet}\n"
        )
        try:
            if self.llm_provider == "anthropic":
                return self._call_anthropic(user_prompt)
            if self.llm_provider == "gemini":
                return self._call_gemini(user_prompt)
            return self._call_openai(user_prompt)
        except Exception:
            return None

    def enrich(
        self,
        *,
        title: str,
        snippet: str,
        source_id: str,
        url: str,
        event_type: str,
    ) -> dict[str, Any]:
        self._load_cache()
        t = _compact_spaces(title)
        sn = _compact_spaces(snippet)
        sid = _compact_spaces(source_id)
        key = self._cache_key(title=t, snippet=sn, url=url)
        cached = self._cache.get(key)
        if isinstance(cached, dict):
            cached_model = str(cached.get("used_model", "")).strip().lower()
            # If historical cache was heuristic and LLM is available now, allow opportunistic refresh.
            if cached_model in {"", "heuristic"} and self.llm_enabled and self._request_budget > 0:
                evidence_ok_cached = len(sn) >= int(self.min_snippet_chars) or (not sn and len(t) >= int(self.min_title_chars))
                if evidence_ok_cached:
                    llm_out_cached = self._call_llm(title=t, snippet=sn, source_id=sid, event_type=event_type)
                    if llm_out_cached:
                        refreshed = {
                            "title_zh": llm_out_cached.get("title_zh", "") or _heuristic_title_zh(t, event_type=event_type),
                            "summary_zh": llm_out_cached.get("summary_zh", "") or _heuristic_summary_zh(t, sn, sid, event_type),
                            "used_model": self.llm_model,
                            "degraded_reason": "",
                            "prompt_version": self.prompt_version,
                            "source_id": sid,
                            "url_norm": url_norm(url),
                        }
                        self._put_cache(key, refreshed)
                        return {**refreshed, "cache_hit": False}
            return {
                "title_zh": str(cached.get("title_zh", "")).strip() or _heuristic_title_zh(t, event_type=event_type),
                "summary_zh": str(cached.get("summary_zh", "")).strip() or _heuristic_summary_zh(t, sn, sid, event_type),
                "used_model": str(cached.get("used_model", "cache")).strip() or "cache",
                "degraded_reason": str(cached.get("degraded_reason", "")).strip(),
                "cache_hit": True,
            }

        # Evidence gate: prefer snippet evidence; if missing but title is sufficiently informative,
        # allow a lightweight title-only model pass to improve zh readability.
        evidence_ok = len(sn) >= int(self.min_snippet_chars) or (not sn and len(t) >= int(self.min_title_chars))
        llm_out: dict[str, str] | None = None
        degraded_reason = ""
        used_model = "heuristic"
        if evidence_ok:
            llm_out = self._call_llm(title=t, snippet=sn, source_id=sid, event_type=event_type)
            if llm_out:
                used_model = self.llm_model
            else:
                degraded_reason = "llm_unavailable_or_failed"
        else:
            degraded_reason = "missing_evidence"

        if llm_out:
            title_zh = llm_out.get("title_zh", "") or _heuristic_title_zh(t, event_type=event_type)
            summary_zh = llm_out.get("summary_zh", "") or _heuristic_summary_zh(t, sn, sid, event_type)
        else:
            title_zh = _heuristic_title_zh(t, event_type=event_type)
            summary_zh = _heuristic_summary_zh(t, sn, sid, event_type)

        payload = {
            "title_zh": title_zh,
            "summary_zh": summary_zh,
            "used_model": used_model,
            "degraded_reason": degraded_reason,
            "prompt_version": self.prompt_version,
            "source_id": sid,
            "url_norm": url_norm(url),
        }
        self._put_cache(key, payload)
        return {**payload, "cache_hit": False}
