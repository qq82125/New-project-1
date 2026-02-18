from __future__ import annotations

import json
import datetime as dt
import os
import re
import subprocess
import uuid
from pathlib import Path

from app.rules.engine import RuleEngine


def _parse_items_from_report(text: str) -> list[dict]:
    lines = text.splitlines()
    items: list[dict] = []
    in_a = False
    current: dict | None = None

    for raw in lines:
        line = raw.strip()
        if not line:
            continue

        if line.startswith("A. 今日要点"):
            in_a = True
            continue
        if in_a and re.match(r"^[B-G]\.\s", line):
            if current:
                items.append(current)
                current = None
            break
        if not in_a:
            continue

        m = re.match(r"^(\d+)\)\s*\[(.*?)\]\s*(.*)$", line)
        if m:
            if current:
                items.append(current)
            current = {
                "index": int(m.group(1)),
                "window_tag": m.group(2),
                "title": m.group(3),
                "summary": "",
                "published": "",
                "source": "",
                "link": "",
                "region": "",
                "lane": "",
                "event_type": "",
                "platform": "",
            }
            continue

        if not current:
            continue

        if line.startswith("摘要："):
            current["summary"] = line.replace("摘要：", "", 1).strip()
        elif line.startswith("发布日期："):
            current["published"] = line.replace("发布日期：", "", 1).strip()
        elif line.startswith("来源："):
            src = line.replace("来源：", "", 1).strip()
            if "|" in src:
                left, right = src.split("|", 1)
                current["source"] = left.strip()
                current["link"] = right.strip()
            else:
                current["source"] = src
        elif line.startswith("地区："):
            current["region"] = line.replace("地区：", "", 1).strip()
        elif line.startswith("赛道："):
            current["lane"] = line.replace("赛道：", "", 1).strip()
        elif line.startswith("事件类型："):
            current["event_type"] = line.replace("事件类型：", "", 1).strip()
        elif line.startswith("技术平台："):
            current["platform"] = line.replace("技术平台：", "", 1).strip()
        elif not current.get("summary"):
            # 兼容历史格式（摘要行可能不以“摘要：”开头）
            current["summary"] = line

    if in_a and current:
        items.append(current)

    return items


def _split_sections(text: str) -> dict[str, str]:
    lines = text.splitlines()
    buckets: dict[str, list[str]] = {}
    current: str | None = None
    for raw in lines:
        line = raw.rstrip("\n")
        m = re.match(r"^([A-G])\.\s+", line.strip())
        if m:
            current = m.group(1)
            buckets.setdefault(current, [])
            buckets[current].append(line)
            continue
        if current:
            buckets[current].append(line)
    return {k: ("\n".join(v).rstrip() + "\n") for k, v in buckets.items()}


def _required_sources_hits(required: list[str], items: list[dict]) -> tuple[str, list[str]]:
    # Explainable v1: match checklist entries against source_id/source_group primarily,
    # with a small legacy fallback on source/link text for robustness.
    patterns = {
        "NMPA": ["nmpa", "nmpa.gov.cn"],
        "CMDE": ["cmde"],
        "UDI数据库": ["udi", "udi.nmpa.gov.cn"],
        "CCGP": ["ccgp", "ccgp.gov.cn"],
        "中国招采网": ["chinabidding", "zhaocai", "招采"],
        "TGA": ["tga", "tga.gov.au"],
        "HSA": ["hsa", "hsa.gov.sg"],
        "PMDA/MHLW": ["pmda", "pmda.go.jp", "mhlw.go.jp"],
        "MFDS": ["mfds", "mfds.go.kr"],
        "GenomeWeb": ["genomeweb", "genomeweb.com"],
        "Reuters Healthcare": ["reuters", "reutersagency.com"],
    }

    hit = {str(x): False for x in required}
    for it in items:
        tokens = []
        for k in ("source_id", "source_group", "source"):
            v = str(it.get(k, "")).strip().lower()
            if v:
                tokens.append(v)
        # Keep legacy matching on URL text too.
        tokens.append(str(it.get("link", "")).strip().lower())
        tokens.append(str(it.get("url", "")).strip().lower())
        hay = " ".join([t for t in tokens if t])

        for key in required:
            key_s = str(key)
            if hit.get(key_s):
                continue
            pats = patterns.get(key_s, [key_s.lower()])
            if any(p.lower() in hay for p in pats):
                hit[key_s] = True

    missing = [k for k, v in hit.items() if not v]
    display = "；".join([f"{k}:{'命中' if hit.get(k) else '未命中'}" for k in required])
    return display, missing


def _norm_title_fp(title: str) -> str:
    s = str(title or "").strip().lower()
    s = re.sub(r"^(update|breaking|exclusive)\s*[:：]\s*", "", s)
    s = re.sub(r"[\W_]+", " ", s, flags=re.U)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _summary_sentence_count(summary: str) -> int:
    s = str(summary or "").strip()
    if not s:
        return 0
    # Split on common sentence punctuation for CN/EN.
    parts = re.split(r"[。！？!?]+|\\.(\\s+|$)", s)
    toks = [p.strip() for p in parts if p and str(p).strip()]
    return len(toks) if toks else (1 if s else 0)


def _load_report_items(project_root: Path, date_str: str) -> list[dict]:
    p = project_root / "reports" / f"ivd_morning_{date_str}.txt"
    if not p.exists():
        return []
    try:
        return _parse_items_from_report(p.read_text(encoding="utf-8", errors="ignore"))
    except Exception:
        return []


def _calc_repeat_rates(project_root: Path, report_date: str | None, items: list[dict]) -> dict:
    if not report_date:
        return {
            "repeat_rate_yesterday": 0.0,
            "repeat_rate_7d_max": 0.0,
            "repeat_yesterday_count": 0,
            "repeat_7d_max_date": None,
        }
    try:
        d0 = dt.date.fromisoformat(str(report_date))
    except Exception:
        return {
            "repeat_rate_yesterday": 0.0,
            "repeat_rate_7d_max": 0.0,
            "repeat_yesterday_count": 0,
            "repeat_7d_max_date": None,
        }

    today_keys = set()
    for it in items:
        link = str(it.get("link", "")).strip()
        if link:
            today_keys.add(link)
        else:
            fp = _norm_title_fp(str(it.get("title", "")))
            if fp:
                today_keys.add(fp)

    denom = max(1, len(today_keys))

    yday = d0 - dt.timedelta(days=1)
    y_items = _load_report_items(project_root, yday.isoformat())
    y_keys = set()
    for it in y_items:
        link = str(it.get("link", "")).strip()
        if link:
            y_keys.add(link)
        else:
            fp = _norm_title_fp(str(it.get("title", "")))
            if fp:
                y_keys.add(fp)
    y_overlap = len(today_keys & y_keys) if y_keys else 0
    y_rate = y_overlap / denom

    max_rate = 0.0
    max_date: str | None = None
    for off in range(1, 8):
        di = d0 - dt.timedelta(days=off)
        di_items = _load_report_items(project_root, di.isoformat())
        if not di_items:
            continue
        di_keys = set()
        for it in di_items:
            link = str(it.get("link", "")).strip()
            if link:
                di_keys.add(link)
            else:
                fp = _norm_title_fp(str(it.get("title", "")))
                if fp:
                    di_keys.add(fp)
        if not di_keys:
            continue
        overlap = len(today_keys & di_keys)
        rate = overlap / denom
        if rate > max_rate:
            max_rate = rate
            max_date = di.isoformat()

    return {
        "repeat_rate_yesterday": y_rate,
        "repeat_rate_7d_max": max_rate,
        "repeat_yesterday_count": y_overlap,
        "repeat_7d_max_date": max_date,
    }


def _calc_completeness(items: list[dict], policy: dict) -> dict:
    required = policy.get("required_fields", []) if isinstance(policy.get("required_fields"), list) else []
    required_fields = [str(x) for x in required if str(x).strip()]
    min_sum = int(policy.get("summary_sentences_min") or 0)
    max_sum = int(policy.get("summary_sentences_max") or 0)
    missing_counts = {k: 0 for k in required_fields}
    bad_summary = 0
    complete = 0

    for it in items:
        ok = True
        for f in required_fields:
            v = it.get(f, None)
            if v is None or (isinstance(v, str) and not v.strip()):
                missing_counts[f] = missing_counts.get(f, 0) + 1
                ok = False
        sc = _summary_sentence_count(str(it.get("summary", "")))
        if (min_sum and sc < min_sum) or (max_sum and sc > max_sum):
            bad_summary += 1
            ok = False
        if ok:
            complete += 1

    share = (complete / len(items)) if items else 0.0
    return {
        "enabled": bool(policy.get("enabled", False)),
        "required_fields": required_fields,
        "complete_items": complete,
        "total_items": len(items),
        "complete_share": share,
        "complete_share_pct": f"{share:.0%}",
        "missing_field_counts": missing_counts,
        "summary_sentence_violations": bad_summary,
    }


def _calc_qc_panel(project_root: Path, report_date: str | None, items: list[dict], qc_decision: dict) -> dict:
    n24 = len([i for i in items if i.get("window_tag") == "24小时内"])
    n7 = len([i for i in items if i.get("window_tag") == "7天补充"])
    apac = len([i for i in items if i.get("region") in ("中国", "亚太")])
    apac_share = (apac / len(items)) if items else 0.0
    # Event mix
    by_event: dict[str, int] = {}
    for it in items:
        et = str(it.get("event_type", "")).strip() or "未标注"
        by_event[et] = by_event.get(et, 0) + 1
    qp = qc_decision.get("quality_policy", {}) if isinstance(qc_decision.get("quality_policy"), dict) else {}
    event_groups = qp.get("event_groups", {}) if isinstance(qp.get("event_groups"), dict) else {}
    reg_types = set(str(x) for x in (event_groups.get("regulatory", []) if isinstance(event_groups.get("regulatory"), list) else []) if str(x).strip())
    if not reg_types:
        reg_types = {"监管审批与指南"}
    regulatory = sum(v for k, v in by_event.items() if k in reg_types)
    commercial = len(items) - regulatory
    event_mix = {
        "by_event_type": dict(sorted(by_event.items(), key=lambda kv: (-kv[1], kv[0]))),
        "regulatory": regulatory,
        "commercial": commercial,
        "regulatory_share": (regulatory / len(items)) if items else 0.0,
        "commercial_share": (commercial / len(items)) if items else 0.0,
    }

    required = []
    # v2 preferred field
    if isinstance(qp.get("required_sources_checklist"), list):
        required = qp.get("required_sources_checklist", [])
    if not required:
        required = qc_decision.get("required_sources_checklist", [])
    required_list = [str(x) for x in required] if isinstance(required, list) else []
    hits, missing = _required_sources_hits(required_list, items)
    comp_policy = qc_decision.get("completeness_policy", {})
    completeness = _calc_completeness(items, comp_policy if isinstance(comp_policy, dict) else {})
    repeat = _calc_repeat_rates(project_root, report_date, items)
    return {
        "n24": n24,
        "n7": n7,
        "apac_share": apac_share,
        "apac_share_pct": f"{apac_share:.0%}",
        "event_mix": event_mix,
        "required_sources_hits": hits,
        "required_sources_missing": missing,
        "repeat": repeat,
        "completeness": completeness,
    }


def _render_section_a(items: list[dict]) -> str:
    lines: list[str] = ["A. 今日要点（8-15条，按重要性排序）"]
    for idx, i in enumerate(items, 1):
        lines.append(f"{idx}) [{i.get('window_tag', '')}] {i.get('title', '')}".rstrip())
        if i.get("summary"):
            lines.append(str(i.get("summary")))
        if i.get("published"):
            lines.append(f"发布日期：{i.get('published')}")
        src = str(i.get("source", "")).strip()
        link = str(i.get("link", "")).strip()
        if src and link:
            lines.append(f"来源：{src} | {link}")
        elif src:
            lines.append(f"来源：{src}")
        if i.get("region"):
            lines.append(f"地区：{i.get('region')}")
        if i.get("lane"):
            lines.append(f"赛道：{i.get('lane')}")
        if i.get("event_type"):
            lines.append(f"事件类型：{i.get('event_type')}")
        if i.get("platform"):
            lines.append(f"技术平台：{i.get('platform')}")
        lines.append("")
    if len(lines) == 1:
        lines.extend(
            [
                "1) [7天补充] 今日未抓取到足够的可用条目（RSS源空/网络异常/关键词过滤过严）。",
                "摘要：建议检查 GitHub Actions 日志与源站可访问性，并补充中国/亚太官方源抓取。",
                "发布日期：（以当日抓取为准）",
                "来源：自动生成",
                "地区：全球",
                "赛道：其他",
                "事件类型：政策与市场动态",
                "技术平台：跨平台/未标注",
            ]
        )
    return "\n".join(lines).rstrip() + "\n\n"


def _render_section_g(panel: dict) -> str:
    mix = panel.get("event_mix", {}) if isinstance(panel.get("event_mix"), dict) else {}
    reg = int(mix.get("regulatory", panel.get("regulatory", 0)) or 0)
    com = int(mix.get("commercial", panel.get("commercial", 0)) or 0)
    return (
        "G. 质量指标 (Quality Audit)\n"
        f"24H条目数 / 7D补充数：{panel.get('n24', 0)} / {panel.get('n7', 0)} | "
        f"亚太占比：{panel.get('apac_share_pct', '0%')} | "
        f"商业与监管事件比：{com}:{reg} | "
        f"必查信源命中清单：{panel.get('required_sources_hits', '')}\n"
    )


def _qc_fail_reasons(items: list[dict], qc_decision: dict, qc_panel: dict) -> list[str]:
    fail_reasons: list[str] = []
    min_24h = int(qc_decision.get("min_24h_items") or 0)
    apac_min = float(qc_decision.get("apac_min_share") or 0.0)
    china_min = float(qc_decision.get("china_min_share") or 0.0)
    daily_max = float(qc_decision.get("daily_repeat_rate_max") or 0.0)
    recent_max = float(qc_decision.get("recent_7d_repeat_rate_max") or 0.0)

    if min_24h and qc_panel.get("n24", 0) < min_24h:
        fail_reasons.append(f"24小时内条目数不足：{qc_panel.get('n24', 0)} < {min_24h}")
    if apac_min and float(qc_panel.get("apac_share") or 0.0) < apac_min:
        fail_reasons.append(f"亚太占比不足：{qc_panel.get('apac_share_pct', '0%')} < {apac_min:.0%}")
    if china_min:
        china_n = len([i for i in items if i.get("region") == "中国"])
        china_share = (china_n / len(items)) if items else 0.0
        if china_share < china_min:
            fail_reasons.append(f"中国占比不足：{china_share:.0%} < {china_min:.0%}")

    mix = qc_decision.get("regulatory_vs_commercial_mix", {})
    if isinstance(mix, dict) and mix.get("enabled"):
        reg_min = float(mix.get("regulatory_min") or 0.0)
        com_min = float(mix.get("commercial_min") or 0.0)
        total = max(1, int(qc_panel.get("regulatory", 0)) + int(qc_panel.get("commercial", 0)))
        reg_share = int(qc_panel.get("regulatory", 0)) / total
        com_share = int(qc_panel.get("commercial", 0)) / total
        if reg_min and reg_share < reg_min:
            fail_reasons.append(f"监管事件占比不足：{reg_share:.0%} < {reg_min:.0%}")
        if com_min and com_share < com_min:
            fail_reasons.append(f"商业事件占比不足：{com_share:.0%} < {com_min:.0%}")

    rep = qc_panel.get("repeat", {}) if isinstance(qc_panel.get("repeat"), dict) else {}
    y_rate = float(rep.get("repeat_rate_yesterday") or 0.0)
    r7 = float(rep.get("repeat_rate_7d_max") or 0.0)
    if daily_max and y_rate > daily_max:
        fail_reasons.append(f"昨日报重复率过高：{y_rate:.0%} > {daily_max:.0%}")
    if recent_max and r7 > recent_max:
        fail_reasons.append(f"近7日峰值重复率过高：{r7:.0%} > {recent_max:.0%}")

    comp = qc_panel.get("completeness", {}) if isinstance(qc_panel.get("completeness"), dict) else {}
    cp = qc_decision.get("completeness_policy", {}) if isinstance(qc_decision.get("completeness_policy"), dict) else {}
    if cp.get("enabled"):
        min_share = float(cp.get("min_complete_share") or 0.0)
        share = float(comp.get("complete_share") or 0.0)
        if min_share and share < min_share:
            fail_reasons.append(f"条目字段齐全占比不足：{share:.0%} < {min_share:.0%}")
        if int(comp.get("summary_sentence_violations") or 0) > 0:
            fail_reasons.append(f"摘要句数不合规：{int(comp.get('summary_sentence_violations') or 0)} 条")

    missing = qc_panel.get("required_sources_missing", []) if isinstance(qc_panel.get("required_sources_missing"), list) else []
    if missing:
        fail_reasons.append(f"必查信源未命中：{','.join([str(x) for x in missing])}")

    return fail_reasons


def run_dryrun(profile: str = "legacy", report_date: str | None = None) -> dict:
    engine = RuleEngine()
    run_id = f"dryrun-{uuid.uuid4().hex[:10]}"
    decision = engine.build_decision(profile=profile, run_id=run_id)

    project_root = engine.project_root
    artifacts_dir = project_root / "artifacts" / run_id
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()
    env["REPORT_TZ"] = str(
        decision.get("email_decision", {}).get("schedule", {}).get("timezone", "Asia/Shanghai")
    )
    if report_date:
        env["REPORT_DATE"] = report_date
    env["REPORT_RUN_ID"] = run_id
    env["DRYRUN_ARTIFACTS_DIR"] = str(artifacts_dir)
    if profile == "enhanced":
        env["ENHANCED_RULES_PROFILE"] = "enhanced"
    else:
        env.pop("ENHANCED_RULES_PROFILE", None)

    proc = subprocess.run(
        ["python3", "scripts/generate_ivd_report.py"],
        cwd=project_root,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    raw_preview_text = proc.stdout
    sections = _split_sections(raw_preview_text)
    items = _parse_items_from_report(raw_preview_text)
    cluster_explain_file = artifacts_dir / "cluster_explain.json"
    clustered_items_file = artifacts_dir / "clustered_items.json"
    source_stats_file = artifacts_dir / "source_stats.json"
    event_explain_file = artifacts_dir / "event_type_explain.json"
    platform_explain_file = artifacts_dir / "platform_explain.json"
    lane_explain_file = artifacts_dir / "lane_explain.json"
    platform_diag_file = artifacts_dir / "platform_diag.json"
    cluster_payload = {}
    if cluster_explain_file.exists():
        try:
            cluster_payload = json.loads(cluster_explain_file.read_text(encoding="utf-8"))
        except Exception:
            cluster_payload = {}
    source_payload = {}
    if source_stats_file.exists():
        try:
            source_payload = json.loads(source_stats_file.read_text(encoding="utf-8"))
        except Exception:
            source_payload = {}
    event_explain_payload = {}
    if event_explain_file.exists():
        try:
            event_explain_payload = json.loads(event_explain_file.read_text(encoding="utf-8"))
        except Exception:
            event_explain_payload = {}

    # Event-type diagnostics: focus on fallback heuristic usage (mapping coverage) and provide samples.
    event_diag: dict[str, Any] = {"fallback_count": 0, "reasons": {}, "samples": []}
    try:
        rows = (
            event_explain_payload.get("items", [])
            if isinstance(event_explain_payload, dict)
            else []
        )
        if isinstance(rows, list):
            reasons: dict[str, int] = {}
            samples: list[dict[str, str]] = []
            fallback = 0
            for r in rows:
                if not isinstance(r, dict):
                    continue
                et = str(r.get("event_type", "")).strip()
                explain = r.get("explain", {}) if isinstance(r.get("explain"), dict) else {}
                matched = bool(explain.get("matched", False))
                if matched:
                    continue
                fallback += 1
                reason = str(explain.get("matched_keyword", "")).strip() or "fallback"
                reasons[reason] = reasons.get(reason, 0) + 1
                if len(samples) < 10:
                    samples.append(
                        {
                            "title": str(r.get("title", ""))[:200],
                            "url": str(r.get("url", ""))[:500],
                            "event_type": et[:80],
                            "source_id": str(r.get("source_id", ""))[:120],
                            "matched_field": str(explain.get("matched_field", ""))[:40],
                            "reason": reason[:80],
                        }
                    )
            event_diag = {
                "fallback_count": fallback,
                "reasons": dict(sorted(reasons.items(), key=lambda kv: (-kv[1], kv[0]))),
                "samples": samples,
                "mapping_source": (
                    event_explain_payload.get("mapping_source")
                    if isinstance(event_explain_payload, dict)
                    else None
                ),
            }
    except Exception:
        event_diag = {"fallback_count": 0, "reasons": {}, "samples": []}
    platform_explain_payload = {}
    if platform_explain_file.exists():
        try:
            platform_explain_payload = json.loads(platform_explain_file.read_text(encoding="utf-8"))
        except Exception:
            platform_explain_payload = {}
    lane_explain_payload = {}
    if lane_explain_file.exists():
        try:
            lane_explain_payload = json.loads(lane_explain_file.read_text(encoding="utf-8"))
        except Exception:
            lane_explain_payload = {}

    # Lane diagnostics: focus on "其他" cases to improve lane_mapping iteratively.
    lane_diag: dict[str, Any] = {"other_count": 0, "reasons": {}, "samples": []}
    try:
        rows = (
            lane_explain_payload.get("items", [])
            if isinstance(lane_explain_payload, dict)
            else []
        )
        if isinstance(rows, list):
            reasons: dict[str, int] = {}
            samples: list[dict[str, str]] = []
            other = 0
            for r in rows:
                if not isinstance(r, dict):
                    continue
                lane = str(r.get("lane", "")).strip()
                if lane != "其他":
                    continue
                other += 1
                explain = r.get("explain", {}) if isinstance(r.get("explain"), dict) else {}
                reason = str(explain.get("reason", "")).strip() or "unknown"
                reasons[reason] = reasons.get(reason, 0) + 1
                if len(samples) < 10:
                    samples.append(
                        {
                            "title": str(r.get("title", ""))[:200],
                            "url": str(r.get("url", ""))[:500],
                            "event_type": str(r.get("event_type", ""))[:80],
                            "source_id": str(r.get("source_id", ""))[:120],
                            "reason": reason[:80],
                        }
                    )
            lane_diag = {
                "other_count": other,
                "reasons": dict(sorted(reasons.items(), key=lambda kv: (-kv[1], kv[0]))),
                "samples": samples,
            }
    except Exception:
        lane_diag = {"other_count": 0, "reasons": {}, "samples": []}

    # UI-friendly diagnostics for platform tagging (focus on "未标注" cases).
    platform_diag: dict[str, Any] = {"unlabeled_count": 0, "reasons": {}, "samples": []}
    try:
        rows = (
            platform_explain_payload.get("items", [])
            if isinstance(platform_explain_payload, dict)
            else []
        )
        if isinstance(rows, list):
            reasons: dict[str, int] = {}
            samples: list[dict[str, str]] = []
            unlabeled = 0
            for r in rows:
                if not isinstance(r, dict):
                    continue
                plat = str(r.get("platform", "")).strip()
                if plat != "未标注":
                    continue
                unlabeled += 1
                explain = r.get("explain", {}) if isinstance(r.get("explain"), dict) else {}
                reason = str(explain.get("reason", "")).strip() or "unknown"
                reasons[reason] = reasons.get(reason, 0) + 1
                if len(samples) < 10:
                    samples.append(
                        {
                            "title": str(r.get("title", ""))[:200],
                            "url": str(r.get("url", ""))[:500],
                            "event_type": str(r.get("event_type", ""))[:80],
                            "source_id": str(r.get("source_id", ""))[:120],
                            "reason": reason[:80],
                        }
                    )
            platform_diag = {
                "unlabeled_count": unlabeled,
                "reasons": dict(sorted(reasons.items(), key=lambda kv: (-kv[1], kv[0]))),
                "samples": samples,
            }
            platform_diag_file.write_text(
                json.dumps(platform_diag, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
    except Exception:
        platform_diag = {"unlabeled_count": 0, "reasons": {}, "samples": []}

    clustered_items: list[dict] = []
    if clustered_items_file.exists():
        try:
            clustered_items = json.loads(clustered_items_file.read_text(encoding="utf-8"))
        except Exception:
            clustered_items = []
    # Enrich parsed items with source_id/source_group when possible (for explainable checklist).
    url2meta: dict[str, dict] = {}
    for it in clustered_items:
        if not isinstance(it, dict):
            continue
        u = str(it.get("url", "")).strip()
        if not u:
            continue
        url2meta[u] = {
            "source_id": it.get("source_id", ""),
            "source_group": it.get("source_group", ""),
            "source": it.get("source", ""),
        }
    for it in items:
        u = str(it.get("link", "")).strip()
        if u and u in url2meta:
            it.setdefault("source_id", url2meta[u].get("source_id", ""))
            it.setdefault("source_group", url2meta[u].get("source_group", ""))

    qc_decision = decision.get("qc_decision", {}) if isinstance(decision.get("qc_decision"), dict) else {}
    output_decision = decision.get("output_decision", {}) if isinstance(decision.get("output_decision"), dict) else {}
    email_decision = decision.get("email_decision", {}) if isinstance(decision.get("email_decision"), dict) else {}

    a_cfg = output_decision.get("A", {}) if isinstance(output_decision.get("A"), dict) else {}
    items_min = int(((a_cfg.get("items_range") or {}).get("min")) or 8)
    items_max = int(((a_cfg.get("items_range") or {}).get("max")) or 15)

    qc_panel = _calc_qc_panel(project_root, report_date, items, qc_decision)
    fail_reasons = _qc_fail_reasons(items, qc_decision, qc_panel)
    passed = len(fail_reasons) == 0
    fail_policy = qc_decision.get("fail_policy", {}) if isinstance(qc_decision.get("fail_policy"), dict) else {}
    mode = str(fail_policy.get("mode", "only_warn"))
    action_taken = "none"

    if (not passed) and mode == "auto_topup":
        action_taken = "auto_topup"
        pool_7d = [x for x in clustered_items if str(x.get("window_tag")) == "7天补充"]
        seen = {str(i.get("link", "")).strip() for i in items}
        topup_limit = int(qc_decision.get("7d_topup_limit") or 0)
        added = 0
        for x in pool_7d:
            if len(items) >= items_max:
                break
            if topup_limit and added >= topup_limit:
                break
            url = str(x.get("url", "")).strip()
            if not url or url in seen:
                continue
            items.append(
                {
                    "index": len(items) + 1,
                    "window_tag": str(x.get("window_tag", "7天补充")),
                    "title": str(x.get("title", "")),
                    "summary": str(x.get("summary_cn", "")),
                    "published": str(x.get("published_at", "")),
                    "source": str(x.get("source", "")),
                    "link": url,
                    "region": str(x.get("region", "")),
                    "lane": str(x.get("lane", "")),
                    "event_type": str(x.get("event_type", "")),
                    "platform": str(x.get("platform", "")),
                }
            )
            seen.add(url)
            added += 1
            if len(items) >= items_min:
                break
        qc_panel = _calc_qc_panel(project_root, report_date, items, qc_decision)
        fail_reasons = _qc_fail_reasons(items, qc_decision, qc_panel)
        passed = len(fail_reasons) == 0
    elif not passed:
        action_taken = mode

    qc_report = {
        "ok": True,
        "ruleset": "qc_rules",
        "profile": profile,
        "pass": passed,
        "fail_reasons": fail_reasons,
        "fail_policy": {"mode": mode, "action_taken": action_taken},
        "panel": qc_panel,
    }

    rendered_sections = {
        "A": _render_section_a(items[:items_max]),
        "B": sections.get("B", ""),
        "C": sections.get("C", ""),
        "D": sections.get("D", ""),
        "E": sections.get("E", ""),
        "F": sections.get("F", ""),
        "G": _render_section_g(qc_panel),
    }
    quality_markers = ["质量指标", "Quality Audit", "24H条目数", "7D补充数", "亚太占比", "必查信源"]
    for sec in ["A", "B", "C", "D", "E", "F"]:
        txt = rendered_sections.get(sec, "")
        if any(m in txt for m in quality_markers):
            cleaned = [ln for ln in txt.splitlines() if not any(m in ln for m in quality_markers)]
            rendered_sections[sec] = ("\n".join(cleaned).rstrip() + "\n\n") if cleaned else ""

    # Build a readable preview with consistent spacing: exactly one blank line between sections.
    title_line = raw_preview_text.splitlines()[0] if raw_preview_text.strip() else "全球IVD晨报"
    parts: list[str] = [title_line, ""]
    for sec in ["A", "B", "C", "D", "E", "F", "G"]:
        block = str(rendered_sections.get(sec, "") or "").rstrip()
        if not block:
            continue
        parts.append(block)
        parts.append("")  # blank line separator
    preview_text = ("\n".join(parts).rstrip() + "\n") if parts else (title_line + "\n")

    explain_payload = {
        "run_id": run_id,
        "mode": "dryrun",
        "profile": profile,
        "date": report_date,
        "decision_explain": decision.get("explain", {}),
        "rules_version": decision.get("rules_version", {}),
        "event_type_classifier": {
            "artifact": "event_type_explain.json" if event_explain_payload else None,
            "mapping_source": (event_explain_payload.get("mapping_source") if isinstance(event_explain_payload, dict) else None),
            "items_count": (len(event_explain_payload.get("items", [])) if isinstance(event_explain_payload, dict) and isinstance(event_explain_payload.get("items"), list) else 0),
        },
        "platform_classifier": {
            "artifact": "platform_explain.json" if platform_explain_payload else None,
            "items_count": (
                len(platform_explain_payload.get("items", []))
                if isinstance(platform_explain_payload, dict) and isinstance(platform_explain_payload.get("items"), list)
                else 0
            ),
        },
        "lane_classifier": {
            "artifact": "lane_explain.json" if lane_explain_payload else None,
            "items_count": (
                len(lane_explain_payload.get("items", []))
                if isinstance(lane_explain_payload, dict) and isinstance(lane_explain_payload.get("items"), list)
                else 0
            ),
        },
        "notes": ["Dry-run only: no DB write, no email send."],
    }
    run_meta = {
        "run_id": run_id,
        "mode": "dryrun",
        "profile": profile,
        "date": report_date,
        "rules_version": decision.get("rules_version", {}),
        "rulesets": (decision.get("explain", {}) or {}).get("rulesets", []),
    }

    (artifacts_dir / "run_id.json").write_text(
        json.dumps(explain_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (artifacts_dir / "newsletter_preview.md").write_text(preview_text, encoding="utf-8")
    (artifacts_dir / "items.json").write_text(
        json.dumps(items, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (artifacts_dir / "qc_report.json").write_text(
        json.dumps(qc_report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (artifacts_dir / "output_render.json").write_text(
        json.dumps(
            {
                "ok": True,
                "run_id": run_id,
                "profile": profile,
                "sections_order": ["A", "B", "C", "D", "E", "F", "G"],
                "sections": rendered_sections,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    (artifacts_dir / "run_meta.json").write_text(
        json.dumps(run_meta, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    return {
        "run_id": run_id,
        "mode": "dryrun",
        "profile": profile,
        "date": report_date,
        "artifacts_dir": str(artifacts_dir),
        "artifacts": {
            "explain": str(artifacts_dir / "run_id.json"),
            "preview": str(artifacts_dir / "newsletter_preview.md"),
            "items": str(artifacts_dir / "items.json"),
            "clustered_items": str(clustered_items_file),
            "cluster_explain": str(cluster_explain_file),
            "source_stats": str(source_stats_file),
            "qc_report": str(artifacts_dir / "qc_report.json"),
            "output_render": str(artifacts_dir / "output_render.json"),
            "run_meta": str(artifacts_dir / "run_meta.json"),
            "platform_diag": str(platform_diag_file),
        },
        "items_count": len(items),
        "items_before_count": int(cluster_payload.get("items_before_count", len(items))),
        "items_after_count": int(cluster_payload.get("items_after_count", len(items))),
        "top_clusters": cluster_payload.get("top_clusters", []),
        "source_stats": source_payload.get("sources", []),
        "platform_diag": platform_diag,
        "lane_diag": lane_diag,
        "event_diag": event_diag,
        "sent": False,
        "qc": qc_report,
        "email_preview": {
            "subject_template": str(email_decision.get("subject_template", "")),
            "subject": str(email_decision.get("subject", "")),
            "recipients": email_decision.get("recipients", []),
            "preview_text": preview_text,
            "preview_html": f"<pre>{preview_text}</pre>",
        },
        "decision": {
            "content_decision": decision.get("content_decision", {}),
            "qc_decision": qc_decision,
            "output_decision": output_decision,
            "email_decision": decision.get("email_decision", {}),
        },
    }


def main(profile: str = "legacy", report_date: str | None = None) -> int:
    print(json.dumps(run_dryrun(profile=profile, report_date=report_date), ensure_ascii=False, indent=2))
    return 0
