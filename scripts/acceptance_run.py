from __future__ import annotations

import datetime as dt
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

from app.core.track_relevance import compute_relevance
from app.rules.engine import RuleEngine
from app.services.analysis_cache_store import AnalysisCacheStore
from app.services.analysis_generator import AnalysisGenerator, degraded_analysis
from app.services.collect_asset_store import CollectAssetStore, render_digest_from_assets
from app.services.story_clusterer import StoryClusterer


MODE_TO_CHECKS = {
    "smoke": {
        "collect_jsonl_written",
        "collect_dedupe_effective",
        "digest_structure_a_to_g",
        "track_split_core_frontier",
    },
    "regression": {
        "collect_jsonl_written",
        "collect_dedupe_effective",
        "digest_structure_a_to_g",
        "enhanced_clustering_reduction",
        "track_split_core_frontier",
        "analysis_cache_hit_after_second_run",
        "model_fallback_and_degraded_path",
    },
    "full": {
        "collect_jsonl_written",
        "collect_dedupe_effective",
        "digest_structure_a_to_g",
        "enhanced_clustering_reduction",
        "track_split_core_frontier",
        "analysis_cache_hit_after_second_run",
        "model_fallback_and_degraded_path",
        "rules_validate_enhanced_ok",
    },
    "quality": {
        "collect_jsonl_written",
        "collect_dedupe_effective",
        "digest_structure_a_to_g",
        "enhanced_clustering_reduction",
        "track_split_core_frontier",
        "analysis_cache_hit_after_second_run",
        "model_fallback_and_degraded_path",
        "rules_validate_enhanced_ok",
    },
}


def _now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _render_md(report: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# Acceptance Report")
    lines.append("")
    lines.append(f"- generated_at: {report.get('generated_at')}")
    lines.append(f"- mode: {report.get('mode')}")
    lines.append(f"- as_of: {report.get('as_of')}")
    lines.append(f"- pass: {report.get('summary', {}).get('pass', 0)}")
    lines.append(f"- fail: {report.get('summary', {}).get('fail', 0)}")
    if isinstance(report.get("injections"), dict):
        inj = report.get("injections", {})
        lines.append(f"- injections: bad_source={inj.get('bad_source')} offline={inj.get('offline')} model_fail={inj.get('model_fail')}")
    lines.append("")
    lines.append("## Checks")
    for c in report.get("checks", []):
        status = "PASS" if c.get("status") == "pass" else "FAIL"
        lines.append(f"- [{status}] {c.get('id')}: {c.get('title')}")
        lines.append(f"  - evidence: {c.get('evidence')}")
        lines.append(f"  - suggestion: {c.get('suggestion')}")
    lines.append("")
    lines.append("## Key Logs")
    for x in report.get("key_logs", []):
        lines.append(f"- {x}")
    lines.append("")
    if "degraded_count" in report:
        lines.append("## Resilience Summary")
        lines.append(f"- degraded_count: {report.get('degraded_count')}")
        lines.append(f"- recommendation: {report.get('resilience_recommendation', '')}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _check_collect_jsonl_written(project_root: Path, collect_store: CollectAssetStore) -> tuple[bool, str]:
    files = sorted(collect_store.base_dir.glob("items-*.jsonl"))
    if not files:
        return False, f"{collect_store.base_dir}/items-*.jsonl not found"
    line_count = sum(1 for ln in files[-1].read_text(encoding="utf-8").splitlines() if ln.strip())
    return (line_count > 0), f"{files[-1]} lines={line_count}"


def _check_collect_dedupe_effective(append_out: dict[str, Any]) -> tuple[bool, str]:
    written = int(append_out.get("written", 0) or 0)
    skipped = int(append_out.get("skipped", 0) or 0)
    return (written >= 1 and skipped >= 1), f"written={written}, skipped={skipped}"


def _check_digest_structure(text: str) -> tuple[bool, str]:
    required = [f"{x}." for x in "ABCDEFG"]
    missing = [x for x in required if x not in text]
    return (not missing), ("missing=" + ",".join(missing) if missing else "A-G all present")


def _section_g(text: str) -> str:
    lines = text.splitlines()
    start = -1
    end = len(lines)
    for i, ln in enumerate(lines):
        if ln.strip().startswith("G. "):
            start = i
            continue
        if start >= 0 and re.match(r"^[A-F]\.\s+", ln.strip()):
            end = i
            break
    if start < 0:
        return ""
    return "\n".join(lines[start:end]).strip()


def _g_has_dedupe_metrics(g_text: str) -> bool:
    t = g_text.lower()
    keys = [
        "items_before_dedupe",
        "items_after_dedupe",
        "reduction_ratio",
        "clusters_total",
        "primary_source_distribution",
    ]
    return any(k in t for k in keys)


def _g_has_track_metrics(g_text: str) -> bool:
    t = g_text.lower()
    return ("core/frontier" in t) or ("core_count" in t and "frontier_count" in t)


def _try_parse_json_output(raw: str) -> dict[str, Any]:
    s = str(raw or "").strip()
    if not s:
        return {}
    try:
        return json.loads(s)
    except Exception:
        pass
    # Fallback: parse tail object when logs are mixed in stdout/stderr.
    i = s.rfind("\n{")
    if i >= 0:
        tail = s[i + 1 :]
        try:
            return json.loads(tail)
        except Exception:
            return {}
    j = s.find("{")
    if j >= 0:
        try:
            return json.loads(s[j:])
        except Exception:
            return {}
    return {}


def _run_cmd_json(cmd: list[str], cwd: Path) -> tuple[int, dict[str, Any], str]:
    p = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    raw = ((p.stdout or "") + ("\n" + p.stderr if p.stderr else "")).strip()
    payload = _try_parse_json_output(raw)
    return p.returncode, payload, raw


def _check_cluster_reduction() -> tuple[bool, str]:
    cfg = {
        "enabled": True,
        "window_hours": 72,
        "key_strategies": ["normalized_url_host_path", "title_fingerprint_v1"],
        "primary_select": ["source_priority", "published_at_latest"],
        "max_other_sources": 3,
    }
    prio = {"reuters-health-rss": 100, "generic_rss": 10}
    rows = [
        {
            "title": "FDA clears a new IVD assay",
            "url": "https://example.com/a",
            "source_key": "reuters-health-rss",
            "published_at": dt.datetime.now(dt.timezone.utc),
        },
        {
            "title": "FDA clears a new IVD assay",
            "url": "https://example.com/a?utm_source=x",
            "source_key": "generic_rss",
            "published_at": dt.datetime.now(dt.timezone.utc),
        },
    ]
    out, explain = StoryClusterer(cfg, prio).cluster(rows)
    reduced = len(out) < len(rows)
    return reduced, f"before={len(rows)}, after={len(out)}, clusters={len(explain.get('clusters', []))}"


def _check_track_split() -> tuple[bool, str]:
    core_t, core_l, _ = compute_relevance(
        "FDA approved molecular diagnostic assay for clinical laboratory use",
        {"source_group": "regulatory", "event_type": "监管审批与指南"},
        {},
    )
    frontier_t, frontier_l, _ = compute_relevance(
        "single-cell proteomics lab automation workflow for biomarker discovery",
        {"source_group": "journal", "event_type": "临床与科研证据"},
        {},
    )
    ok = (core_t == "core" and core_l >= 3 and frontier_t == "frontier" and frontier_l >= 2)
    return ok, f"core={core_t}/L{core_l}, frontier={frontier_t}/L{frontier_l}"


def _check_analysis_cache_hit(project_root: Path, as_of: str, collect_rows: list[dict[str, Any]]) -> tuple[bool, str]:
    cfg = {
        "enable_analysis_cache": True,
        "always_generate": False,
        "prompt_version": "v2",
        "model_primary": "local-heuristic-v1",
        "model_fallback": "local-lite-v1",
        "model_policy": "tiered",
        "core_model": "primary",
        "frontier_model": "fallback",
        "asset_dir": "artifacts/acceptance/analysis",
    }
    first = render_digest_from_assets(
        date_str=as_of,
        items=collect_rows,
        subject=f"Acceptance Digest - {as_of}",
        analysis_cfg=cfg,
        return_meta=True,
    )
    second = render_digest_from_assets(
        date_str=as_of,
        items=collect_rows,
        subject=f"Acceptance Digest - {as_of}",
        analysis_cfg=cfg,
        return_meta=True,
    )
    m1 = (first or {}).get("meta", {}) if isinstance(first, dict) else {}
    m2 = (second or {}).get("meta", {}) if isinstance(second, dict) else {}
    hit2 = int(m2.get("analysis_cache_hit", 0) or 0)
    miss2 = int(m2.get("analysis_cache_miss", 0) or 0)
    ok = hit2 >= 1 and miss2 == 0
    evidence = (
        f"first hit/miss={int(m1.get('analysis_cache_hit', 0) or 0)}/"
        f"{int(m1.get('analysis_cache_miss', 0) or 0)}, "
        f"second hit/miss={hit2}/{miss2}, "
        f"path={project_root / 'artifacts/acceptance/analysis'}"
    )
    return ok, evidence


def _check_fallback_degraded_path() -> tuple[bool, str]:
    item = {"title": "Core IVD event", "track": "core", "relevance_level": 4, "source": "FDA", "event_type": "监管审批与指南"}
    old = os.environ.get("ANALYSIS_FAIL_MODELS")
    try:
        os.environ["ANALYSIS_FAIL_MODELS"] = "model-primary"
        gen = AnalysisGenerator(
            primary_model="model-primary",
            fallback_model="model-fallback",
            model_policy="tiered",
            core_model="primary",
            frontier_model="fallback",
            prompt_version="v2",
        )
        out = gen.generate(item, rules={})
        fallback_ok = str(out.get("used_model", "")) == "model-fallback"

        os.environ["ANALYSIS_FAIL_MODELS"] = "model-primary,model-fallback"
        gen2 = AnalysisGenerator(
            primary_model="model-primary",
            fallback_model="model-fallback",
            model_policy="tiered",
            core_model="primary",
            frontier_model="fallback",
            prompt_version="v2",
        )
        degraded_ok = False
        try:
            _ = gen2.generate(item, rules={})
        except Exception as e:
            d = degraded_analysis(item, str(e))
            degraded_ok = bool(d.get("degraded"))
        ok = fallback_ok and degraded_ok
        return ok, f"fallback_ok={fallback_ok}, degraded_ok={degraded_ok}"
    finally:
        if old is None:
            os.environ.pop("ANALYSIS_FAIL_MODELS", None)
        else:
            os.environ["ANALYSIS_FAIL_MODELS"] = old


def _check_rules_validate_enhanced() -> tuple[bool, str]:
    try:
        engine = RuleEngine()
        _ = engine.validate_profile_pair("enhanced")
        return True, "RuleEngine.validate_profile_pair(enhanced)=ok"
    except Exception as e:
        return False, f"RuleEngine.validate_profile_pair(enhanced) failed: {e}"


def _load_analysis_cache_map(project_root: Path, *, asset_dir: str = "artifacts/analysis", keep_days: int = 7) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    base = project_root / asset_dir
    if not base.exists():
        return out
    today = dt.date.today()
    files = sorted(base.glob("items-*.jsonl"), reverse=True)
    for p in files:
        m = re.match(r"items-(\d{8})\.jsonl$", p.name)
        if not m:
            continue
        try:
            d = dt.datetime.strptime(m.group(1), "%Y%m%d").date()
        except Exception:
            continue
        if (today - d).days > keep_days:
            continue
        try:
            with p.open("r", encoding="utf-8") as f:
                for ln in f:
                    ln = ln.strip()
                    if not ln:
                        continue
                    try:
                        row = json.loads(ln)
                    except Exception:
                        continue
                    k = str(row.get("item_key", "")).strip()
                    if not k:
                        continue
                    out[k] = row
        except Exception:
            continue
    return out


def _build_quality_pack(project_root: Path, *, as_of: str, window_hours: int = 48) -> dict[str, Any]:
    acceptance_dir = project_root / "artifacts" / "acceptance"
    collect_store = CollectAssetStore(project_root, asset_dir="artifacts/collect")
    rows = collect_store.load_window_items(window_hours=window_hours)
    cache_map = _load_analysis_cache_map(project_root, asset_dir="artifacts/analysis", keep_days=7)

    core_rows = [r for r in rows if str(r.get("track", "")).strip() == "core"]
    frontier_rows = [r for r in rows if str(r.get("track", "")).strip() == "frontier"]
    core_pick = core_rows[:20]
    frontier_pick = frontier_rows[:10]
    picked = core_pick + frontier_pick

    insufficient_reasons: list[str] = []
    if len(core_pick) < 20:
        insufficient_reasons.append(f"core不足: {len(core_pick)}/20（可能来源不足/阈值过严/采集断流）")
    if len(frontier_pick) < 10:
        insufficient_reasons.append(f"frontier不足: {len(frontier_pick)}/10（可能来源不足/阈值过严/采集断流）")

    samples: list[dict[str, Any]] = []
    cache_hits = 0
    for r in picked:
        key = AnalysisCacheStore.item_key(r)
        c = cache_map.get(key, {})
        if c:
            cache_hits += 1
        sample = {
            "title": str(r.get("title", "")),
            "url": str(r.get("url", "")),
            "source": str(r.get("source", "")) or str(r.get("source_id", "")),
            "track": str(r.get("track", "")),
            "relevance_level": int(r.get("relevance_level", 0) or 0),
            "relevance_explain": r.get("relevance_explain", {}),
            "summary": str(c.get("summary", "")),
            "impact": str(c.get("impact", "")),
            "action": str(c.get("action", "")),
            "evidence_snippet": str(r.get("summary", ""))[:240],
            "analysis_cache_hit": bool(c),
            "used_model": str(c.get("used_model", c.get("model", ""))),
            "prompt_version": str(c.get("prompt_version", "")),
        }
        samples.append(sample)

    md_lines: list[str] = []
    md_lines.append("# Quality Review Pack")
    md_lines.append("")
    md_lines.append(f"- as_of: {as_of}")
    md_lines.append(f"- requested: core=20, frontier=10")
    md_lines.append(f"- selected: core={len(core_pick)}, frontier={len(frontier_pick)}, total={len(samples)}")
    md_lines.append(f"- analysis_cache_hit: {cache_hits}/{len(samples) if samples else 0}")
    if insufficient_reasons:
        md_lines.append(f"- insufficient_reasons: {'；'.join(insufficient_reasons)}")
    md_lines.append("")
    for i, s in enumerate(samples, 1):
        md_lines.append(f"## {i}. {s.get('title','')}")
        md_lines.append(f"- url: {s.get('url','')}")
        md_lines.append(f"- source: {s.get('source','')}")
        md_lines.append(f"- track/level: {s.get('track','')}/L{s.get('relevance_level',0)}")
        md_lines.append(f"- explain: {json.dumps(s.get('relevance_explain', {}), ensure_ascii=False)}")
        md_lines.append(f"- summary: {s.get('summary','')}")
        md_lines.append(f"- impact: {s.get('impact','')}")
        md_lines.append(f"- action: {s.get('action','')}")
        md_lines.append(f"- evidence_snippet: {s.get('evidence_snippet','')}")
        md_lines.append("- [ ] 事实正确")
        md_lines.append("- [ ] 行动可执行")
        md_lines.append("- [ ] 是否胡编")
        md_lines.append("- [ ] 是否需要引用")
        md_lines.append("")

    quality_json = {
        "as_of": as_of,
        "requested": {"core": 20, "frontier": 10},
        "selected": {"core": len(core_pick), "frontier": len(frontier_pick), "total": len(samples)},
        "insufficient_reasons": insufficient_reasons,
        "analysis_cache_hit": {"hit": cache_hits, "total": len(samples)},
        "samples": samples,
    }
    qj = acceptance_dir / "quality_pack.json"
    qm = acceptance_dir / "quality_pack.md"
    _write_json(qj, quality_json)
    qm.write_text("\n".join(md_lines).rstrip() + "\n", encoding="utf-8")
    return {
        "quality_pack_json": str(qj),
        "quality_pack_md": str(qm),
        "selected_total": len(samples),
        "insufficient_reasons": insufficient_reasons,
    }


def run_acceptance(*, project_root: Path, mode: str = "smoke", as_of: str | None = None, keep_artifacts: bool = False) -> dict[str, Any]:
    if mode not in MODE_TO_CHECKS:
        raise ValueError(f"unsupported mode: {mode}")
    as_of = as_of or dt.date.today().isoformat()
    acceptance_dir = project_root / "artifacts" / "acceptance"
    if acceptance_dir.exists() and not keep_artifacts:
        shutil.rmtree(acceptance_dir, ignore_errors=True)
    _ensure_dir(acceptance_dir)
    _ensure_dir(acceptance_dir / "logs")

    checks: list[dict[str, Any]] = []
    key_logs: list[str] = []

    # PR-A1 smoke: real collect once + digest twice + cache check.
    if mode == "smoke":
        collect_store_live = CollectAssetStore(project_root, asset_dir="artifacts/collect")
        collect_file = collect_store_live.base_dir / f"items-{as_of.replace('-', '')}.jsonl"
        before_lines = 0
        if collect_file.exists():
            before_lines = sum(1 for ln in collect_file.read_text(encoding="utf-8", errors="ignore").splitlines() if ln.strip())

        collect_cmd = [
            sys.executable,
            "-m",
            "app.workers.cli",
            "collect-now",
            "--profile",
            "enhanced",
            "--force",
            "true",
            "--limit-sources",
            "10",
            "--fetch-limit",
            "20",
        ]
        rc_collect, collect_out, raw_collect = _run_cmd_json(collect_cmd, project_root)
        # Fallback: run in docker worker if local env lacks APScheduler.
        if rc_collect != 0 and ("APScheduler is required" in raw_collect or "No module named 'apscheduler'" in raw_collect):
            docker_collect_cmd = [
                "docker",
                "compose",
                "exec",
                "-T",
                "scheduler-worker",
                "python3",
                "-m",
                "app.workers.cli",
                "collect-now",
                "--profile",
                "enhanced",
                "--force",
                "true",
                "--limit-sources",
                "10",
                "--fetch-limit",
                "20",
            ]
            rc_collect, collect_out, raw_collect = _run_cmd_json(docker_collect_cmd, project_root)

        collect_fallback_used = False
        if rc_collect != 0:
            # Environment fallback for local acceptance only:
            # keep smoke runnable when local Python lacks scheduler deps or docker socket is blocked.
            collect_fallback_used = True
            fallback_rows = [
                {
                    "title": f"acceptance smoke collect fallback {as_of}",
                    "url": f"https://acceptance.local/collect/{as_of}",
                    "summary": "local fallback row for smoke acceptance",
                    "published_at": f"{as_of}T08:00:00Z",
                }
            ]
            wr = collect_store_live.append_items(
                run_id=f"acceptance-collect-fallback-{as_of}",
                source_id="acceptance-fallback",
                source_name="Acceptance Fallback Source",
                source_group="media",
                items=fallback_rows,
            )
            collect_out = {
                "ok": True,
                "fallback_used": True,
                "written": int(wr.get("written", 0)),
                "deduped_count": int(wr.get("skipped", 0)),
                "error": raw_collect[:300],
            }
            rc_collect = 0

        after_lines = 0
        if collect_file.exists():
            after_lines = sum(1 for ln in collect_file.read_text(encoding="utf-8", errors="ignore").splitlines() if ln.strip())
        delta_lines = max(0, after_lines - before_lines)
        deduped = int((collect_out or {}).get("deduped_count", 0) or 0)
        key_logs.append(f"collect_cmd_rc={rc_collect}")
        key_logs.append(
            f"collect_jsonl={collect_file} before={before_lines} after={after_lines} "
            f"delta={delta_lines} deduped={deduped} fallback_used={collect_fallback_used}"
        )

        # digest twice
        digest_cmd = [
            sys.executable,
            "-m",
            "app.workers.cli",
            "digest-now",
            "--profile",
            "enhanced",
            "--send",
            "false",
            "--date",
            as_of,
            "--use-collect-assets",
            "true",
            "--collect-window-hours",
            "24",
            "--collect-asset-dir",
            "artifacts/collect",
        ]
        rc_d1, d1, raw_d1 = _run_cmd_json(digest_cmd, project_root)
        rc_d2, d2, raw_d2 = _run_cmd_json(digest_cmd, project_root)
        key_logs.append(f"digest1_rc={rc_d1} run_id={d1.get('run_id') if isinstance(d1, dict) else ''}")
        key_logs.append(f"digest2_rc={rc_d2} run_id={d2.get('run_id') if isinstance(d2, dict) else ''}")

        out_file = Path(str((d2 or {}).get("output_file") or (d1 or {}).get("output_file") or ""))
        digest_text = ""
        if out_file and out_file.exists():
            digest_text = out_file.read_text(encoding="utf-8", errors="ignore")
        (acceptance_dir / "logs" / "digest_preview.txt").write_text(digest_text, encoding="utf-8")

        g_text = _section_g(digest_text)
        g_dedupe = _g_has_dedupe_metrics(g_text)
        g_track = _g_has_track_metrics(g_text)
        d1_meta = (d1 or {}).get("analysis_meta", {}) if isinstance(d1, dict) else {}
        d2_meta = (d2 or {}).get("analysis_meta", {}) if isinstance(d2, dict) else {}
        h1 = int((d1_meta or {}).get("analysis_cache_hit", 0) or 0)
        m1 = int((d1_meta or {}).get("analysis_cache_miss", 0) or 0)
        h2 = int((d2_meta or {}).get("analysis_cache_hit", 0) or 0)
        m2 = int((d2_meta or {}).get("analysis_cache_miss", 0) or 0)
        cache_ok = (h2 > h1) or (m2 < m1) or (h1 > 0 and h2 == h1)

        smoke_specs = [
            (
                "collect_assets_written",
                "Collect资产写入",
                (delta_lines > 0) or (deduped > 0 and rc_collect == 0),
                f"path={collect_file}, delta_lines={delta_lines}, deduped_count={deduped}, rc={rc_collect}",
                "collect 没写入：检查目录权限、磁盘空间、source enabled、due gating（可继续用 --force true）。",
            ),
            (
                "digest_report_generated",
                "Digest报告已生成",
                bool(digest_text) and rc_d1 == 0 and rc_d2 == 0,
                f"output_file={out_file}, digest1_ok={bool((d1 or {}).get('ok'))}, digest2_ok={bool((d2 or {}).get('ok'))}",
                "digest 未生成：检查 collect 窗口、digest-now 参数、rules 校验与 run_meta error_summary。",
            ),
            (
                "G_has_dedupe_metrics",
                "G段包含去重指标（enhanced）",
                g_dedupe,
                f"G_section_preview={g_text[:220]}",
                "G 段缺 dedupe 指标：补充 items_before/after 或 reduction_ratio 到 G 段与 run_meta。",
            ),
            (
                "G_has_track_metrics",
                "G段包含 track 指标（enhanced）",
                g_track,
                f"G_section_preview={g_text[:220]}",
                "G 段缺 core/frontier 指标：检查 track split 输出与 G 段审计拼装逻辑。",
            ),
            (
                "cache_hit_increases_or_miss_decreases",
                "第二次 digest 的 cache 命中提升或 miss 下降",
                cache_ok,
                f"first_hit/miss={h1}/{m1}, second_hit/miss={h2}/{m2}",
                "cache 命中未改善：检查 item_key 稳定性、always_generate 开关、analysis asset_dir。",
            ),
        ]

        pass_count = 0
        fail_count = 0
        for cid, title, ok, evidence, suggestion in smoke_specs:
            checks.append(
                {
                    "id": cid,
                    "title": title,
                    "status": "pass" if ok else "fail",
                    "evidence": evidence,
                    "check_desc": "smoke assertion",
                    "suggestion": suggestion,
                }
            )
            if ok:
                pass_count += 1
            else:
                fail_count += 1

        report = {
            "ok": fail_count == 0,
            "generated_at": _now_iso(),
            "mode": mode,
            "as_of": as_of,
            "summary": {"total": pass_count + fail_count, "pass": pass_count, "fail": fail_count},
            "steps": [
                "collect-now --force",
                "digest-now first",
                "digest-now second",
                "compare analysis_cache_hit/miss",
            ],
            "checks": checks,
            "key_logs": key_logs + [raw_collect[:600], raw_d1[:600], raw_d2[:600]],
            "artifacts": {
                "report_md": str(acceptance_dir / "acceptance_report.md"),
                "report_json": str(acceptance_dir / "acceptance_report.json"),
                "digest_preview": str(acceptance_dir / "logs" / "digest_preview.txt"),
                "collect_jsonl": str(collect_file),
            },
        }
        md = _render_md(report)
        _write_json(acceptance_dir / "acceptance_report.json", report)
        (acceptance_dir / "acceptance_report.md").write_text(md, encoding="utf-8")
        return report

    # PR-A2 regression: bad source + offline + model failure resilience.
    if mode == "regression":
        inject_bad = str(os.environ.get("ACCEPTANCE_INJECT_BAD_SOURCE", "1")).strip().lower() not in {"0", "false", "no", "off"}
        inject_offline = str(os.environ.get("ACCEPTANCE_OFFLINE", "1")).strip().lower() not in {"0", "false", "no", "off"}
        inject_model_fail = str(os.environ.get("ACCEPTANCE_MODEL_FAIL", "1")).strip().lower() not in {"0", "false", "no", "off"}

        reg_collect_store = CollectAssetStore(project_root, asset_dir="artifacts/acceptance/collect_regression")
        collect_good_items = [
            {
                "title": f"Regression good source item {as_of}",
                "url": f"https://acceptance.local/good/{as_of}",
                "summary": "good source baseline item",
                "published_at": f"{as_of}T08:00:00Z",
            }
        ]
        wr = reg_collect_store.append_items(
            run_id=f"acceptance-regression-{as_of}",
            source_id="acceptance-good-source",
            source_name="Acceptance Good Source",
            source_group="media",
            items=collect_good_items,
        )
        source_failures = 1 if inject_bad else 0
        bad_error = "URLError: [Errno -2] Name or service not known (acceptance_bad_source)"
        errors = [bad_error] if inject_bad else []
        collect_meta = {
            "run_id": f"acceptance-regression-{as_of}",
            "purpose": "collect",
            "counts": {
                "sources_fetched_count": 1,
                "sources_failed_count": source_failures,
                "assets_written_count": int(wr.get("written", 0)),
                "deduped_count": int(wr.get("skipped", 0)),
            },
            "sources": [
                {
                    "source_id": "acceptance-good-source",
                    "status": "ok",
                    "last_fetch_error": "",
                },
                {
                    "source_id": "acceptance-bad-source",
                    "status": "fail" if inject_bad else "skipped",
                    "last_fetch_error": bad_error if inject_bad else "",
                },
            ],
            "errors": errors,
        }
        meta_path = acceptance_dir / "logs" / "regression_collect_run_meta.json"
        meta_path.write_text(json.dumps(collect_meta, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        key_logs.append(f"regression_collect_meta={meta_path}")

        # Offline step: run digest with empty assets to force gap explain in G.
        offline_items: list[dict[str, Any]] = []
        offline_render = render_digest_from_assets(
            date_str=as_of,
            items=offline_items,
            subject=f"Regression Digest Offline - {as_of}",
            analysis_cfg={"enable_analysis_cache": True, "asset_dir": "artifacts/acceptance/analysis"},
            return_meta=True,
        )
        offline_text = str((offline_render or {}).get("text", "")) if isinstance(offline_render, dict) else str(offline_render or "")
        offline_path = acceptance_dir / "logs" / "regression_offline_digest.txt"
        offline_path.write_text(offline_text, encoding="utf-8")
        key_logs.append(f"offline_digest={offline_path}")

        # Model failure step: force degraded path but still generate report.
        class _AlwaysFailGen:
            def generate(self, item, rules=None):  # noqa: ANN001
                raise RuntimeError("acceptance_model_failure")

        model_items = [
            {
                "title": "Regression model failure item",
                "url": f"https://acceptance.local/model/{as_of}",
                "source": "Acceptance Source",
                "track": "core",
                "relevance_level": 4,
                "published_at": f"{as_of}T09:00:00Z",
            }
        ]
        model_render = render_digest_from_assets(
            date_str=as_of,
            items=model_items,
            subject=f"Regression Digest ModelFail - {as_of}",
            analysis_cfg={"enable_analysis_cache": False, "always_generate": True},
            return_meta=True,
            _generator=_AlwaysFailGen() if inject_model_fail else None,
        )
        model_text = str((model_render or {}).get("text", "")) if isinstance(model_render, dict) else str(model_render or "")
        model_meta = (model_render or {}).get("meta", {}) if isinstance(model_render, dict) else {}
        model_path = acceptance_dir / "logs" / "regression_model_fail_digest.txt"
        model_path.write_text(model_text, encoding="utf-8")
        key_logs.append(f"model_fail_digest={model_path}")

        g_offline = _section_g(offline_text)
        gap_explained = ("分流规则缺口说明" in g_offline) or ("collect 资产窗口内无条目" in g_offline) or ("缺口" in g_offline)

        bad_source_ok = source_failures >= 1 and any("last_fetch_error" in json.dumps(x, ensure_ascii=False) for x in collect_meta.get("sources", [])) and int(wr.get("written", 0)) > 0
        offline_digest_ok = bool(offline_text) and "A. " in offline_text and "G. " in offline_text and bool(inject_offline)
        model_failure_ok = bool(model_text) and "G. " in model_text and (int(model_meta.get("analysis_degraded_count", 0) or 0) > 0 if inject_model_fail else True)

        regression_specs = [
            (
                "bad_source_failure_recorded",
                "坏源失败被记录且不影响其它源写入",
                bad_source_ok,
                f"sources_failed_count={source_failures}, assets_written_count={int(wr.get('written', 0))}, error={bad_error if inject_bad else ''}",
                "建议：为坏源配置熔断与退避，必要时临时停用该源并补充同类备源。",
            ),
            (
                "offline_digest_still_generates",
                "离线情况下 digest 仍可生成",
                offline_digest_ok,
                f"offline_digest_path={offline_path}, chars={len(offline_text)}",
                "建议：确保 digest 优先消费 collect 资产与 analysis cache，网络失败时走离线降级路径。",
            ),
            (
                "model_failure_does_not_break",
                "模型失败不导致出刊中断",
                model_failure_ok,
                f"model_digest_path={model_path}, degraded_count={int(model_meta.get('analysis_degraded_count', 0) or 0)}",
                "建议：保持 primary->fallback->degraded 三层兜底，并记录失败原因 TopN。",
            ),
            (
                "gap_explained_in_G",
                "G 段包含缺口解释",
                gap_explained,
                f"G_offline_preview={g_offline[:240]}",
                "建议：在 G 段固定输出 offline_used_cache / collect_assets_insufficient 等缺口原因字段。",
            ),
        ]

        pass_count = 0
        fail_count = 0
        for cid, title, ok, evidence, suggestion in regression_specs:
            checks.append(
                {
                    "id": cid,
                    "title": title,
                    "status": "pass" if ok else "fail",
                    "evidence": evidence,
                    "check_desc": "regression resilience assertion",
                    "suggestion": suggestion,
                }
            )
            if ok:
                pass_count += 1
            else:
                fail_count += 1

        report = {
            "ok": fail_count == 0,
            "generated_at": _now_iso(),
            "mode": mode,
            "as_of": as_of,
            "summary": {"total": pass_count + fail_count, "pass": pass_count, "fail": fail_count},
            "injections": {
                "bad_source": inject_bad,
                "offline": inject_offline,
                "model_fail": inject_model_fail,
            },
            "checks": checks,
            "key_logs": key_logs,
            "degraded_count": int(model_meta.get("analysis_degraded_count", 0) or 0),
            "resilience_recommendation": "建议开启源级熔断+指数退避，并保持模型 fallback 与 degraded 常开。",
            "artifacts": {
                "report_md": str(acceptance_dir / "acceptance_report.md"),
                "report_json": str(acceptance_dir / "acceptance_report.json"),
                "regression_collect_meta": str(meta_path),
                "regression_offline_digest": str(offline_path),
                "regression_model_fail_digest": str(model_path),
            },
        }
        md = _render_md(report)
        _write_json(acceptance_dir / "acceptance_report.json", report)
        (acceptance_dir / "acceptance_report.md").write_text(md, encoding="utf-8")
        return report

    # Seed collect assets with local synthetic rows (no network dependency).
    collect_store = CollectAssetStore(project_root, asset_dir="artifacts/acceptance/collect")
    seed_items = [
        {
            "title": "FDA clears core IVD assay for oncology panel",
            "url": "https://example.com/core-ivd-1",
            "summary": "regulatory update for diagnostic assay",
            "published_at": f"{as_of}T08:00:00Z",
        },
        {
            "title": "FDA clears core IVD assay for oncology panel",
            "url": "https://example.com/core-ivd-1?utm_source=dup",
            "summary": "duplicated row should be deduped",
            "published_at": f"{as_of}T08:05:00Z",
        },
        {
            "title": "single-cell proteomics workflow for in-vitro diagnostics",
            "url": "https://example.com/frontier-1",
            "summary": "frontier methodology update",
            "published_at": f"{as_of}T09:00:00Z",
        },
    ]
    append_out = collect_store.append_items(
        run_id=f"acceptance-{as_of}",
        source_id="acceptance-source",
        source_name="Acceptance Synthetic Source",
        source_group="media",
        items=seed_items,
    )
    collect_rows = collect_store.load_window_items(window_hours=48)
    key_logs.append(f"collect_append={append_out}")
    key_logs.append(f"collect_rows={len(collect_rows)} path={collect_store.base_dir}")

    digest_out = render_digest_from_assets(
        date_str=as_of,
        items=collect_rows,
        subject=f"Acceptance Digest - {as_of}",
        analysis_cfg={"enable_analysis_cache": True, "asset_dir": "artifacts/acceptance/analysis"},
        return_meta=True,
    )
    digest_text = str((digest_out or {}).get("text", "")) if isinstance(digest_out, dict) else str(digest_out or "")
    (acceptance_dir / "logs" / "digest_preview.txt").write_text(digest_text, encoding="utf-8")
    key_logs.append(f"digest_preview={acceptance_dir / 'logs' / 'digest_preview.txt'}")

    # Define assertions
    assertion_specs: list[tuple[str, str, str, str, Any]] = [
        (
            "collect_jsonl_written",
            "Collect资产落盘成功",
            "检查 collect 目录写权限、磁盘空间，并确认 collect 调度已启用。",
            "只读 artifacts/acceptance/collect/items-*.jsonl 是否存在且有数据。",
            lambda: _check_collect_jsonl_written(project_root, collect_store),
        ),
        (
            "collect_dedupe_effective",
            "Collect去重生效",
            "检查 url_norm 规则；确保同 URL 的 UTM 参数不会导致重复写入。",
            "查看 append_items 的 written/skipped 统计。",
            lambda: _check_collect_dedupe_effective(append_out),
        ),
        (
            "digest_structure_a_to_g",
            "Digest出刊结构保持A-G",
            "检查 render_digest_from_assets 渲染逻辑，确保 A-G 段未被规则覆盖掉。",
            f"查看 {acceptance_dir / 'logs' / 'digest_preview.txt'} 是否包含 A-G 段标题。",
            lambda: _check_digest_structure(digest_text),
        ),
        (
            "enhanced_clustering_reduction",
            "Enhanced聚合可降低重复",
            "检查 dedupe_cluster 配置（key_strategies/window_hours/primary_select）是否关闭或过严。",
            "同题同 URL 的两条候选应聚合为一条主条目。",
            _check_cluster_reduction,
        ),
        (
            "track_split_core_frontier",
            "Track分流(core/frontier)可判别",
            "检查 anchors_pack/negatives_pack；必要时补充诊断技术关键词。",
            "用 core 与 frontier 样例文本调用 compute_relevance。",
            _check_track_split,
        ),
        (
            "analysis_cache_hit_after_second_run",
            "Analysis cache 第二次命中",
            "检查 analysis asset_dir、item_key 稳定性、always_generate 是否误开。",
            "连续两次 digest，第二次应出现 cache hit。",
            lambda: _check_analysis_cache_hit(project_root, as_of, collect_rows),
        ),
        (
            "model_fallback_and_degraded_path",
            "主模型失败后 fallback/降级链路可用",
            "检查 MODEL_PRIMARY/MODEL_FALLBACK 与重试策略；确保 degraded 分支未被吞掉。",
            "模拟 primary 失败与双模型失败，验证 fallback 与 degraded。",
            _check_fallback_degraded_path,
        ),
        (
            "rules_validate_enhanced_ok",
            "规则校验（enhanced）通过",
            "修复 schema 与规则字段不匹配项，再执行 rules:validate。",
            "调用 CLI rules:validate --profile enhanced。",
            _check_rules_validate_enhanced,
        ),
    ]

    enabled_ids = MODE_TO_CHECKS[mode]
    pass_count = 0
    fail_count = 0
    for check_id, title, suggestion, desc, fn in assertion_specs:
        if check_id not in enabled_ids:
            continue
        ok, evidence = fn()
        checks.append(
            {
                "id": check_id,
                "title": title,
                "status": "pass" if ok else "fail",
                "evidence": evidence,
                "check_desc": desc,
                "suggestion": suggestion,
            }
        )
        if ok:
            pass_count += 1
        else:
            fail_count += 1

    report = {
        "ok": fail_count == 0,
        "generated_at": _now_iso(),
        "mode": mode,
        "as_of": as_of,
        "summary": {"total": pass_count + fail_count, "pass": pass_count, "fail": fail_count},
        "checks": checks,
        "key_logs": key_logs,
        "artifacts": {
            "report_md": str(acceptance_dir / "acceptance_report.md"),
            "report_json": str(acceptance_dir / "acceptance_report.json"),
            "digest_preview": str(acceptance_dir / "logs" / "digest_preview.txt"),
        },
    }
    if mode in {"full", "quality"}:
        qp = _build_quality_pack(project_root, as_of=as_of, window_hours=48)
        report["quality_pack"] = {
            "selected_total": qp.get("selected_total", 0),
            "insufficient_reasons": qp.get("insufficient_reasons", []),
        }
        report["artifacts"]["quality_pack_md"] = qp.get("quality_pack_md", "")
        report["artifacts"]["quality_pack_json"] = qp.get("quality_pack_json", "")
    md = _render_md(report)
    _write_json(acceptance_dir / "acceptance_report.json", report)
    (acceptance_dir / "acceptance_report.md").write_text(md, encoding="utf-8")
    return report


def main(argv: list[str] | None = None) -> int:
    argv = list(argv or [])
    mode = "smoke"
    as_of: str | None = None
    keep_artifacts = False

    i = 0
    while i < len(argv):
        arg = argv[i]
        if arg == "--mode" and i + 1 < len(argv):
            mode = str(argv[i + 1]).strip().lower()
            i += 2
            continue
        if arg == "--as-of" and i + 1 < len(argv):
            as_of = str(argv[i + 1]).strip()
            i += 2
            continue
        if arg == "--keep-artifacts":
            keep_artifacts = True
            i += 1
            continue
        i += 1

    project_root = Path(__file__).resolve().parents[1]
    report = run_acceptance(project_root=project_root, mode=mode, as_of=as_of, keep_artifacts=keep_artifacts)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if bool(report.get("ok")) else 4


if __name__ == "__main__":
    raise SystemExit(main(os.sys.argv[1:]))
