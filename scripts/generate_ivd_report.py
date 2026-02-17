#!/usr/bin/env python3
"""
Lightweight cloud generator for the IVD morning brief.

Notes:
- This is a pragmatic v1: it relies on RSS + keyword tagging to produce a usable
  briefing on time. You can extend sources and tagging rules incrementally.
"""

import datetime as dt
import math
import os
import re
import sys
from dataclasses import dataclass
from http.client import RemoteDisconnected
from pathlib import Path
from socket import timeout as SocketTimeout
from typing import Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

import feedparser

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.adapters.rule_bridge import load_runtime_rules


@dataclass(frozen=True)
class Item:
    title: str
    link: str
    published: dt.datetime
    source: str
    region: str
    lane: str
    platform: str
    event_type: str
    window_tag: str  # "24小时内" or "7天补充"
    summary_cn: str


RUNTIME_CONTENT = {
    "title_similarity_threshold": 0.78,
    "apac_min_share": 0.40,
    "min_items": 8,
    "max_items": 15,
    "topup_if_24h_lt": 10,
    "daily_max_repeat_rate": 0.25,
    "recent_7d_max_repeat_rate": 0.40,
    "include_keywords": [],
    "exclude_keywords": [],
    "lane_mapping": {},
    "platform_mapping": {},
    "event_mapping": {},
}


def env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def now_in_tz(tz_name: str) -> dt.datetime:
    try:
        return dt.datetime.now(ZoneInfo(tz_name))
    except Exception:
        return dt.datetime.utcnow()


def to_dt(entry) -> Optional[dt.datetime]:
    t = None
    if getattr(entry, "published_parsed", None):
        t = entry.published_parsed
    elif getattr(entry, "updated_parsed", None):
        t = entry.updated_parsed
    if not t:
        return None
    return dt.datetime(*t[:6], tzinfo=dt.timezone.utc)


def has_term(text_lc: str, term_lc: str) -> bool:
    """
    Reduce false positives from substring matches (e.g., 'test' matching 'latest').
    Uses word-boundary matching for short ASCII tokens.
    """
    if not term_lc:
        return False
    if " " in term_lc or "-" in term_lc or "/" in term_lc:
        return term_lc in text_lc
    if term_lc.isascii() and term_lc.isalpha() and len(term_lc) <= 5:
        return re.search(rf"\\b{re.escape(term_lc)}\\b", text_lc) is not None
    return term_lc in text_lc


def _map_label_by_keywords(text_lc: str, mapping: dict) -> Optional[str]:
    if not isinstance(mapping, dict):
        return None
    for label, kws in mapping.items():
        if not isinstance(kws, list):
            continue
        for kw in kws:
            k = str(kw).strip().lower()
            if k and has_term(text_lc, k):
                return str(label)
    return None


def cn_lane(text: str) -> str:
    t = text.lower()
    dynamic_lane = _map_label_by_keywords(t, RUNTIME_CONTENT.get("lane_mapping", {}))
    if dynamic_lane:
        return dynamic_lane
    if any(k in t for k in ["肿瘤", "癌", "肿瘤标志物"]):
        return "肿瘤检测"
    if any(k in t for k in ["感染", "病原", "病毒", "流感", "新冠", "呼吸道"]):
        return "感染检测"
    if any(k in t for k in ["生殖", "遗传", "产前", "nipt"]):
        return "生殖与遗传检测"
    if any(has_term(t, k) for k in ["cancer", "tumor", "oncology", "carcinoma", "pd-l1", "biomarker"]):
        return "肿瘤检测"
    if any(has_term(t, k) for k in ["infect", "virus", "covid", "flu", "influenza", "pathogen", "sepsis"]):
        return "感染检测"
    if any(has_term(t, k) for k in ["prenatal", "nipt", "fertility", "reproductive", "genetic", "hereditary"]):
        return "生殖与遗传检测"
    return "其他"


def cn_platform(text: str) -> str:
    t = text.lower()
    dynamic_platform = _map_label_by_keywords(t, RUNTIME_CONTENT.get("platform_mapping", {}))
    if dynamic_platform:
        return dynamic_platform
    if any(k in t for k in ["体外诊断", "化学发光", "免疫诊断"]):
        return "免疫诊断（化学发光/ELISA/IHC等）"
    if any(k in t for k in ["核酸", "pcr", "聚合酶链式反应"]):
        return "PCR"
    if "数字pcr" in t:
        return "数字PCR"
    if any(k in t for k in ["流式"]):
        return "流式细胞"
    if any(k in t for k in ["质谱"]):
        return "质谱"
    # Guardrails: avoid mis-tagging obvious pharma/business items as IVD platforms.
    if any(
        k in t
        for k in [
            "earnings",
            "quarter",
            "revenue",
            "sales",
            "layoff",
            "restructur",
            "acquisition",
            "takeover",
        ]
    ):
        if not any(
            k in t
            for k in [
                "diagnostic",
                "assay",
                "test",
                "ivd",
                "immunoassay",
                "pcr",
                "sequencing",
                "ngs",
                "poct",
                "pathology",
                "laboratory",
            ]
        ):
            return "跨平台/未标注"
    if any(has_term(t, k) for k in ["ngs", "sequencing", "whole genome", "wgs", "rna-seq"]):
        return "NGS"
    if has_term(t, "digital pcr") or has_term(t, "ddpcr"):
        return "数字PCR"
    if has_term(t, "pcr"):
        return "PCR"
    if any(has_term(t, k) for k in ["mass spec", "lc-ms", "ms/ms"]):
        return "质谱"
    if any(has_term(t, k) for k in ["flow cytometry", "cytometry"]):
        return "流式细胞"
    if any(has_term(t, k) for k in ["immunoassay", "chemiluminescence", "elisa", "ihc"]):
        return "免疫诊断（化学发光/ELISA/IHC等）"
    if any(has_term(t, k) for k in ["point-of-care", "poc", "poct", "self-test", "rapid test"]):
        return "POCT/分子POCT"
    if any(has_term(t, k) for k in ["microfluidic", "lab-on-a-chip", "single molecule", "single-molecule"]):
        return "微流控/单分子"
    return "跨平台/未标注"


def detect_event_type(text: str, source: str, link: str) -> str:
    t = (text + " " + source + " " + link).lower()
    dynamic_event = _map_label_by_keywords(t, RUNTIME_CONTENT.get("event_mapping", {}))
    if dynamic_event:
        return dynamic_event
    if any(k in t for k in ["nmpa", "cmde", "fda", "pmda", "mfds", "hsa.gov.sg", "tga.gov.au", "guideline", "guidance", "recall", "safety alert", "field safety", "approved", "审批", "指导原则", "通告", "召回"]):
        return "监管审批与指南"
    if any(k in t for k in ["acquisition", "acquire", "merger", "ipo", "funding", "financing", "raise", "partnership", "collaboration", "deal"]):
        return "并购融资/IPO与合作"
    if any(k in t for k in ["launch", "introduce", "new test", "new assay", "registered", "clearance", "ce mark"]):
        return "注册上市/产品发布"
    if any(k in t for k in ["study", "clinical", "data", "evidence", "validation", "trial", "publication"]):
        return "临床与科研证据"
    if any(k in t for k in ["tender", "procurement", "reimbursement", "payment", "bid", "ccgp", "招标", "采购"]):
        return "支付与招采"
    return "政策与市场动态"


def cn_region(source: str, link: str) -> str:
    s = (source + " " + link).lower()
    if any(k in s for k in ["tga.gov.au", "pmda.go.jp", "mhlw.go.jp", "hsa.gov.sg", "mfds.go.kr", ".cn", "nmpa.gov.cn"]):
        # crude: treat these as APAC; China will be handled separately if needed later
        if any(k in s for k in ["nmpa.gov.cn", "udi.nmpa.gov.cn", ".cn"]):
            return "中国"
        return "亚太"
    if any(k in s for k in ["europa.eu", "ema.europa.eu", "medtecheurope.org", ".eu", ".uk"]):
        return "欧洲"
    return "北美"


def score_ivd(text: str) -> int:
    """
    Heuristic relevance scoring to reduce noise from pharma/business-only news.
    """
    t = text.lower()

    anchors = [
        "diagnostic",
        "diagnostics",
        "assay",
        "test",
        "testing",
        "ivd",
        "immunoassay",
        "chemiluminescence",
        "elisa",
        "ihc",
        "pcr",
        "ddpcr",
        "digital pcr",
        "ngs",
        "sequencing",
        "poct",
        "point-of-care",
        "rapid test",
        "pathology",
        "laboratory",
        "reagent",
        "analyzer",
        "companion diagnostic",
        "cdx",
        "labcorp",
        "quest",
    ]

    strong = [
        "diagnostic",
        "diagnostics",
        "assay",
        "test",
        "testing",
        "ivd",
        "ldt",
        "clia",
        "pathology",
        "laboratory",
        "immunoassay",
        "chemiluminescence",
        "elisa",
        "ihc",
        "pcr",
        "ddpcr",
        "digital pcr",
        "ngs",
        "sequencing",
        "poct",
        "point-of-care",
        "rapid test",
        "mass spec",
        "lc-ms",
        "flow cytometry",
        "reagent",
        "analyzer",
        "companion diagnostic",
        "cdx",
        "udi",
        "nmpa",
        "pmda",
        "tga",
        "mfds",
        "hsa",
        "labcorp",
        "quest",
    ]
    weak = ["biomarker", "screening", "clinical lab", "lab"]
    negative = [
        "earnings",
        "revenue",
        "sales",
        "layoff",
        "restructur",
        "phase ",
        "trial",
        "drug",
        "therapy",
        "vaccine",
        "glp-1",
    ]

    s = 0
    for k in strong:
        if has_term(t, k):
            s += 2
    for k in weak:
        if has_term(t, k):
            s += 1
    for k in negative:
        if has_term(t, k):
            s -= 1
    # Add an extra point if any anchor term is present.
    if any(has_term(t, a) for a in anchors):
        s += 1
    return s


def is_ivd_relevant(text: str) -> bool:
    t = text.lower()
    for x in RUNTIME_CONTENT.get("exclude_keywords", []):
        kw = str(x).strip().lower()
        if kw and has_term(t, kw):
            return False
    # Require at least one anchor term to avoid pharma-only/business-only noise.
    anchors = [
        "diagnostic",
        "diagnostics",
        "assay",
        "test",
        "ivd",
        "immunoassay",
        "pcr",
        "sequencing",
        "ngs",
        "poct",
        "pathology",
        "laboratory",
        "reagent",
        "analyzer",
        "companion diagnostic",
        "cdx",
        "labcorp",
        "quest",
    ]
    # Company-name anchors: allow IVD-relevant news even when generic wording is used.
    ivd_companies = [
        "roche",
        "abbott",
        "danaher",
        "beckman coulter",
        "cepheid",
        "thermo fisher",
        "siemens healthineers",
        "bio-rad",
        "qiagen",
        "illumina",
        "hologic",
        "bio mérieux",
        "biomerieux",
        "becton dickinson",
        "bd ",
        "agilent",
        "guardant",
        "natera",
        "exact sciences",
        "gilead diagnostics",
        "myndray",
        "mindray",
        "snibe",
        "mgi",
        "bgi",
    ]
    has_anchor = any(has_term(t, a) for a in anchors)
    dynamic_include = [str(x).strip().lower() for x in RUNTIME_CONTENT.get("include_keywords", [])]
    if dynamic_include and any(has_term(t, a) for a in dynamic_include):
        has_anchor = True
    has_company = any(c in t for c in ivd_companies)
    if not (has_anchor or has_company):
        return False
    # Keep recall high, rely on dedupe/source caps for noise suppression.
    return score_ivd(text) >= 0


def is_regulatory_ivd_relevant(text: str) -> bool:
    """
    Regulatory feeds (FDA/TGA) are device-wide. Keep only diagnostics/IVD-adjacent
    items to avoid flooding the briefing with non-IVD devices.
    """
    t = text.lower()
    keep_terms = [
        "ivd",
        "in vitro",
        "diagnostic",
        "assay",
        "test",
        "field safety",
        "medical device",
        "medtech",
        "device",
        "laboratory",
        "pathology",
        "glucose",
        "cgm",
        "monitor",
        "sensor",
    ]
    return any(has_term(t, k) for k in keep_terms)


def is_relaxed_relevant(text: str) -> bool:
    """
    Fallback when strict filtering yields too few items (<8). Keeps out the
    noisiest pharma-only content while allowing general IVD-adjacent updates.
    """
    t = text.lower()
    hard_exclude = [
        "earnings",
        "drug",
        "therapy",
        "vaccine",
        "phase ",
        "trial",
        "glp-1",
        "food",
        "insurance",
        "hospital staffing",
    ]
    if any(has_term(t, k) for k in hard_exclude):
        return False
    anchors = [
        "diagnostic",
        "diagnostics",
        "assay",
        "ivd",
        "laboratory",
        "pathology",
        "immunoassay",
        "pcr",
        "sequencing",
        "ngs",
        "poct",
        "test",
        "testing",
        "medical device",
        "medtech",
        "device",
        "reagent",
        "analyzer",
    ]
    return any(has_term(t, a) for a in anchors)


def cn_summary(entry) -> str:
    # Avoid long noisy feed summaries; keep it short and decision-oriented.
    raw = (getattr(entry, "summary", "") or getattr(entry, "description", "") or "").strip()
    raw = re.sub(r"<[^>]+>", " ", raw)
    raw = re.sub(r"\s+", " ", raw).strip()
    if not raw:
        return "摘要：该条目与体外诊断/检测相关。建议优先核对监管状态、商业化路径与时间节点。"
    if len(raw) > 240:
        raw = raw[:240].rstrip() + "…"
    return f"摘要：{raw} 建议结合原文确认对收入与准入节奏的直接影响。"


def normalize_title(title: str) -> str:
    t = title.lower()
    t = re.sub(r"https?://\S+", " ", t)
    t = re.sub(r"[^0-9a-z\u4e00-\u9fff]+", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def title_tokens(title: str) -> set[str]:
    n = normalize_title(title)
    toks = {x for x in n.split(" ") if len(x) >= 3}
    # Keep Chinese chunks too.
    for zh in re.findall(r"[\u4e00-\u9fff]{2,}", title):
        toks.add(zh)
    return toks


def title_similarity(a: str, b: str) -> float:
    ta = title_tokens(a)
    tb = title_tokens(b)
    if not ta or not tb:
        return 0.0
    inter = len(ta & tb)
    union = len(ta | tb)
    if union == 0:
        return 0.0
    return inter / union


def source_rank(source: str, link: str) -> int:
    s = (source + " " + link).lower()
    # 1 = highest trust / priority
    if any(k in s for k in ["nmpa.gov.cn", "cmde", "tga.gov.au", "hsa.gov.sg", "pmda.go.jp", "mhlw.go.jp", "mfds.go.kr", "ccgp.gov.cn"]):
        return 1
    if any(k in s for k in ["reuters", "raps", "genomeweb", "statnews"]):
        return 2
    if any(k in s for k in ["fierce", "medtechdive", "biopharmadive"]):
        return 3
    return 4


def window_tag(published: dt.datetime, now_utc: dt.datetime) -> str:
    delta = now_utc - published.astimezone(dt.timezone.utc)
    if delta <= dt.timedelta(hours=24):
        return "24小时内"
    return "7天补充"


def fetch_text(url: str, timeout: int = 15) -> str:
    req = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; IVDMorningBot/1.0)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
    )
    try:
        with urlopen(req, timeout=timeout) as r:
            data = r.read()
            return data.decode("utf-8", errors="ignore")
    except (HTTPError, URLError, SocketTimeout, TimeoutError, RemoteDisconnected):
        return ""


def fetch_bytes(url: str, timeout: int = 15) -> bytes:
    req = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; IVDMorningBot/1.0)",
            "Accept": "application/rss+xml,application/xml,text/xml;q=0.9,*/*;q=0.8",
        },
    )
    try:
        with urlopen(req, timeout=timeout) as r:
            return r.read()
    except (HTTPError, URLError, SocketTimeout, TimeoutError, RemoteDisconnected):
        return b""


def collect_nmpa_udi(now_utc: dt.datetime, tz_name: str) -> list[Item]:
    """
    Add NMPA UDI day/week updates as high-confidence China signals.
    """
    url = "https://udi.nmpa.gov.cn/download.html"
    html = fetch_text(url)
    if not html:
        return []

    day = re.findall(r"UDID_DAY_UPDATE_(\d{8})\.zip", html)
    week = re.findall(r"UDID_WEEKLY_UPDATE_(\d{8})_(\d{8})\.zip", html)
    out: list[Item] = []

    def mk_pub(yyyymmdd: str) -> dt.datetime:
        d = dt.datetime.strptime(yyyymmdd, "%Y%m%d").replace(tzinfo=ZoneInfo(tz_name))
        return d.astimezone(dt.timezone.utc)

    if day:
        # Include several recent day updates to stabilize China/APAC coverage.
        for yyyymmdd in sorted(set(day), reverse=True)[:3]:
            pub = mk_pub(yyyymmdd)
            if now_utc - pub > dt.timedelta(days=7):
                continue
            out.append(
                Item(
                    title=f"NMPA UDI数据库日更包：UDID_DAY_UPDATE_{yyyymmdd}.zip（含IVD类目）",
                    link=url,
                    published=pub,
                    source="NMPA UDI",
                    region="中国",
                    lane="其他",
                    platform="跨平台（UDI/追溯）",
                    event_type="监管审批与指南",
                    window_tag=window_tag(pub, now_utc),
                    summary_cn="摘要：NMPA UDI下载页显示日更增量持续更新，可用于追溯、流通与院端主数据治理。建议关注IVD试剂类条目增量与企业信息完整度变化。",
                )
            )
            if len(out) >= 3:
                break

    if week:
        a, b = sorted({(x, y) for x, y in week}, key=lambda t: t[1])[-1]
        pub = mk_pub(b)
        if now_utc - pub <= dt.timedelta(days=7):
            out.append(
                Item(
                    title=f"NMPA UDI数据库周更包覆盖 {a[:4]}-{a[4:6]}-{a[6:]} 至 {b[:4]}-{b[4:6]}-{b[6:]}",
                    link=url,
                    published=pub,
                    source="NMPA UDI",
                    region="中国",
                    lane="其他",
                    platform="跨平台（UDI/数据共享）",
                    event_type="监管审批与指南",
                    window_tag=window_tag(pub, now_utc),
                    summary_cn="摘要：周更包便于企业与渠道批量同步一周增量数据，降低日更抓取运维压力。对招采、院内耗材管理与合规尽调有直接价值。",
                )
            )

    return out


def collect_nmpa_site_updates(now_utc: dt.datetime, tz_name: str) -> list[Item]:
    """
    Collect dated NMPA website items as China regulatory supplements.
    """
    url = "https://www.nmpa.gov.cn/"
    html = fetch_text(url)
    if not html:
        return []

    out: list[Item] = []
    seen: set[str] = set()
    for m in re.finditer(r'<a[^>]+href="([^"]+)"[^>]*>(.*?)</a>', html, flags=re.I | re.S):
        href = m.group(1).strip()
        title = re.sub(r"<[^>]+>", " ", m.group(2))
        title = re.sub(r"\s+", " ", title).strip()
        if not href or not title:
            continue
        if title in seen:
            continue
        if not any(k in title for k in ["体外", "诊断", "试剂", "检测", "医疗器械"]):
            continue
        dm = re.search(r"(\d{8})\d{0,9}\.html", href)
        if not dm:
            continue
        d = dt.datetime.strptime(dm.group(1), "%Y%m%d").replace(tzinfo=ZoneInfo(tz_name))
        pub = d.astimezone(dt.timezone.utc)
        if now_utc - pub > dt.timedelta(days=7):
            continue

        if href.startswith("http"):
            link = href
        else:
            link = "https://www.nmpa.gov.cn/" + href.lstrip("/")

        text = title
        out.append(
            Item(
                title=title,
                link=link,
                published=pub,
                source="NMPA",
                region="中国",
                lane=cn_lane(text),
                platform=cn_platform(text),
                event_type="监管审批与指南",
                window_tag=window_tag(pub, now_utc),
                summary_cn="摘要：该条来自NMPA官网公开信息，反映监管与器械动态更新。建议结合原文核对适用范围、执行日期与合规影响。",
            )
        )
        seen.add(title)
        if len(out) >= 6:
            break
    return out


def collect_pmda_updates(now_utc: dt.datetime, tz_name: str) -> list[Item]:
    """
    Parse PMDA English homepage dated updates and keep device/IVD-relevant items.
    """
    url = "https://www.pmda.go.jp/english/index.html"
    html = fetch_text(url, timeout=25)
    if not html:
        return []

    out: list[Item] = []
    seen: set[str] = set()
    li_pat = re.compile(
        r'<li>\s*<a href="([^"]+)">.*?<p class="date">([^<]+)</p>.*?<p class="category[^"]*">([^<]+)</p>.*?<p class="title">([^<]+)</p>.*?</a>\s*</li>',
        flags=re.I | re.S,
    )
    for href, date_str, category, title in li_pat.findall(html):
        title = re.sub(r"\s+", " ", title).strip()
        category = re.sub(r"\s+", " ", category).strip()
        if not title or title in seen:
            continue
        tl = (title + " " + category).lower()
        if not any(k in tl for k in ["ivd", "device", "diagnostic", "medical", "companion diagnostics"]):
            continue
        try:
            d = dt.datetime.strptime(date_str.strip(), "%B %d, %Y").replace(tzinfo=ZoneInfo(tz_name))
        except Exception:
            continue
        pub = d.astimezone(dt.timezone.utc)
        if now_utc - pub > dt.timedelta(days=7):
            continue

        if href.startswith("http"):
            link = href
        else:
            link = "https://www.pmda.go.jp" + (href if href.startswith("/") else "/" + href)

        out.append(
            Item(
                title=f"{date_str.strip()} {category}: {title}",
                link=link,
                published=pub,
                source="PMDA",
                region="亚太",
                lane=cn_lane(title),
                platform=cn_platform(title),
                event_type="监管审批与指南",
                window_tag=window_tag(pub, now_utc),
                summary_cn="摘要：该条来自PMDA英文官方公告，属于亚太监管动态。建议核对适用品类、生效时间与对跨境注册/上市路径的影响。",
            )
        )
        seen.add(title)
        if len(out) >= 4:
            break
    return out


def dedupe_items(items: list[Item]) -> list[Item]:
    """
    Keep the best source for the same event title.
    """
    best: dict[str, Item] = {}
    sim_th = float(RUNTIME_CONTENT.get("title_similarity_threshold", 0.78))
    for it in items:
        key = normalize_title(it.title)
        if not key:
            continue
        old_key = key
        old = best.get(old_key)
        if not old:
            for k, v in best.items():
                if title_similarity(it.title, v.title) >= sim_th:
                    old_key = k
                    old = v
                    break
        if not old:
            best[old_key] = it
            continue
        old_rank = source_rank(old.source, old.link)
        new_rank = source_rank(it.source, it.link)
        if new_rank < old_rank:
            best[old_key] = it
            continue
        if new_rank == old_rank and len(it.summary_cn) > len(old.summary_cn):
            best[old_key] = it
    return list(best.values())


def parse_titles_from_report(path: Path) -> set[str]:
    out: set[str] = set()
    try:
        txt = path.read_text(encoding="utf-8")
    except Exception:
        return out
    for ln in txt.splitlines():
        m = re.match(r"^\d+\)\s+\[[^\]]+\]\s+(.+)$", ln.strip())
        if m:
            out.add(normalize_title(m.group(1)))
    return out


def load_history_titles(reports_dir: Path, today: str) -> tuple[set[str], dict[str, set[str]]]:
    files = sorted(reports_dir.glob("ivd_morning_*.txt"))
    by_day: dict[str, set[str]] = {}
    for p in files:
        m = re.search(r"ivd_morning_(\d{4}-\d{2}-\d{2})\.txt$", p.name)
        if not m:
            continue
        ds = m.group(1)
        if ds >= today:
            continue
        by_day[ds] = parse_titles_from_report(p)
    days = sorted(by_day.keys(), reverse=True)[:7]
    recent = {d: by_day[d] for d in days}
    yday = recent[days[0]] if days else set()
    return yday, recent


def choose_top_items(candidates: list[Item], yday_titles: set[str], recent_titles: dict[str, set[str]]) -> list[Item]:
    # Prefer low repetition; only use repeated items when needed to reach minimum count.
    fresh: list[Item] = []
    not_yday: list[Item] = []
    repeated: list[Item] = []
    src_cap = {}
    src_used: dict[str, int] = {}

    union7: set[str] = set()
    for s in recent_titles.values():
        union7 |= s

    for it in candidates:
        key = normalize_title(it.title)
        if key and key not in union7:
            fresh.append(it)
        elif key and key not in yday_titles:
            not_yday.append(it)
        else:
            repeated.append(it)

    def is_intl(it: Item) -> bool:
        return it.region in ("北美", "欧洲")

    top: list[Item] = []
    intl_target = 5
    for bucket in (fresh, not_yday, repeated):
        for it in bucket:
            if len(top) >= 15:
                break
            if not is_intl(it):
                continue
            cap = src_cap.get(it.source, 3)
            if src_used.get(it.source, 0) >= cap:
                continue
            top.append(it)
            src_used[it.source] = src_used.get(it.source, 0) + 1
            if len([x for x in top if is_intl(x)]) >= intl_target:
                break

    for bucket in (fresh, not_yday, repeated):
        for it in bucket:
            if len(top) >= 15:
                break
            if it.link in {x.link for x in top}:
                continue
            cap = src_cap.get(it.source, 3)
            if src_used.get(it.source, 0) >= cap:
                continue
            top.append(it)
            src_used[it.source] = src_used.get(it.source, 0) + 1
        if len(top) >= 10:
            break
    return top[:15]


def enforce_apac_share(top: list[Item], all_items: list[Item]) -> list[Item]:
    if not top:
        return top
    apac_target = math.ceil(len(top) * float(RUNTIME_CONTENT.get("apac_min_share", 0.40)))
    apac_now = sum(1 for i in top if i.region in ("中国", "亚太"))
    if apac_now >= apac_target:
        return top

    top_links = {i.link for i in top}
    apac_pool = [i for i in all_items if i.link not in top_links and i.region in ("中国", "亚太")]
    replace_idx = [idx for idx in range(len(top) - 1, -1, -1) if top[idx].region not in ("中国", "亚太")]

    for cand in apac_pool:
        if apac_now >= apac_target or not replace_idx:
            break
        idx = replace_idx.pop(0)
        top[idx] = cand
        apac_now += 1
    return top


def enforce_international_primary(top: list[Item], all_items: list[Item]) -> list[Item]:
    if not top:
        return top
    intl_target = math.ceil(len(top) * 0.5)
    intl_now = sum(1 for i in top if i.region in ("北美", "欧洲"))
    if intl_now >= intl_target:
        return top

    top_links = {i.link for i in top}
    intl_pool = [i for i in all_items if i.link not in top_links and i.region in ("北美", "欧洲")]
    replace_idx = [idx for idx in range(len(top) - 1, -1, -1) if top[idx].region not in ("北美", "欧洲")]

    for cand in intl_pool:
        if intl_now >= intl_target or not replace_idx:
            break
        idx = replace_idx.pop(0)
        top[idx] = cand
        intl_now += 1
    return top


def calc_dup_rate(top: list[Item], history_titles: set[str]) -> float:
    if not top:
        return 0.0
    overlap = 0
    for it in top:
        if normalize_title(it.title) in history_titles:
            overlap += 1
    return overlap / len(top)


def must_source_hits(items: list[Item]) -> str:
    checks = {
        "NMPA": False,
        "CMDE": False,
        "CCGP": False,
        "TGA": False,
        "HSA": False,
        "PMDA/MHLW": False,
        "MFDS": False,
    }
    for it in items:
        s = (it.source + " " + it.link).lower()
        if "nmpa.gov.cn" in s:
            checks["NMPA"] = True
        if "cmde" in s:
            checks["CMDE"] = True
        if "ccgp.gov.cn" in s:
            checks["CCGP"] = True
        if "tga.gov.au" in s:
            checks["TGA"] = True
        if "hsa.gov.sg" in s:
            checks["HSA"] = True
        if "pmda.go.jp" in s or "mhlw.go.jp" in s:
            checks["PMDA/MHLW"] = True
        if "mfds.go.kr" in s:
            checks["MFDS"] = True
    return "；".join([f"{k}:{'命中' if v else '未命中'}" for k, v in checks.items()])


def main() -> int:
    tz_name = env("REPORT_TZ", "Asia/Shanghai")
    forced_date = env("REPORT_DATE", "")
    if forced_date:
        # Replay mode: keep deterministic date header while preserving legacy default path.
        now_local = dt.datetime.strptime(forced_date, "%Y-%m-%d").replace(
            tzinfo=ZoneInfo(tz_name), hour=8, minute=30, second=0, microsecond=0
        )
    else:
        now_local = now_in_tz(tz_name)
    now_utc = now_local.astimezone(dt.timezone.utc)
    date_str = now_local.strftime("%Y-%m-%d")
    root_dir = Path(__file__).resolve().parent.parent
    reports_dir = root_dir / "reports"

    runtime_rules = load_runtime_rules(date_str=date_str)
    if runtime_rules.get("enabled"):
        content_cfg = runtime_rules.get("content", {})
        RUNTIME_CONTENT["title_similarity_threshold"] = float(
            content_cfg.get("title_similarity_threshold", RUNTIME_CONTENT["title_similarity_threshold"])
        )
        RUNTIME_CONTENT["apac_min_share"] = float(
            content_cfg.get("apac_min_share", RUNTIME_CONTENT["apac_min_share"])
        )
        RUNTIME_CONTENT["min_items"] = int(content_cfg.get("min_items", RUNTIME_CONTENT["min_items"]))
        RUNTIME_CONTENT["max_items"] = int(content_cfg.get("max_items", RUNTIME_CONTENT["max_items"]))
        RUNTIME_CONTENT["topup_if_24h_lt"] = int(
            content_cfg.get("topup_if_24h_lt", RUNTIME_CONTENT["topup_if_24h_lt"])
        )
        RUNTIME_CONTENT["daily_max_repeat_rate"] = float(
            content_cfg.get("daily_max_repeat_rate", RUNTIME_CONTENT["daily_max_repeat_rate"])
        )
        RUNTIME_CONTENT["recent_7d_max_repeat_rate"] = float(
            content_cfg.get("recent_7d_max_repeat_rate", RUNTIME_CONTENT["recent_7d_max_repeat_rate"])
        )
        RUNTIME_CONTENT["include_keywords"] = content_cfg.get("include_keywords", [])
        RUNTIME_CONTENT["exclude_keywords"] = content_cfg.get("exclude_keywords", [])
        RUNTIME_CONTENT["lane_mapping"] = content_cfg.get("lane_mapping", {})
        RUNTIME_CONTENT["platform_mapping"] = content_cfg.get("platform_mapping", {})
        RUNTIME_CONTENT["event_mapping"] = content_cfg.get("event_mapping", {})

    # Media + official feeds, with APAC/China reinforcement.
    sources = [
        ("Fierce Biotech", "https://www.fiercebiotech.com/rss/xml", "北美", "media"),
        ("Fierce Healthcare", "https://www.fiercehealthcare.com/rss/xml", "北美", "media"),
        ("MedTech Dive", "https://www.medtechdive.com/feeds/news/", "北美", "media"),
        ("BioPharma Dive", "https://www.biopharmadive.com/feeds/news/", "北美", "media"),
        ("GenomeWeb", "https://www.genomeweb.com/rss.xml", "北美", "media"),
        ("360Dx", "https://www.360dx.com/rss.xml", "北美", "media"),
        ("STAT", "https://www.statnews.com/feed/", "北美", "media"),
        ("MedTech Europe", "https://www.medtecheurope.org/feed/", "欧洲", "media"),
        ("MHRA Alerts", "https://www.gov.uk/drug-device-alerts.atom", "欧洲", "regulatory"),
        ("Reuters Healthcare", "https://www.reutersagency.com/feed/?best-topics=healthcare-pharmaceuticals", "北美", "media"),
        ("RAPS", "https://www.raps.org/news-and-articles/news-articles/feed", "北美", "media"),
        ("TGA Safety Alerts", "https://www.tga.gov.au/feeds/alert/safety-alerts.xml", "亚太", "regulatory"),
        ("TGA Product Recalls", "https://www.tga.gov.au/feeds/alert/product-recalls.xml", "亚太", "regulatory"),
        ("FDA MedWatch", "https://www.fda.gov/about-fda/contact-fda/stay-informed/rss-feeds/medwatch/rss.xml", "北美", "regulatory"),
    ]
    if runtime_rules.get("enabled"):
        source_override = runtime_rules.get("content", {}).get("sources", [])
        if source_override:
            sources = source_override

    items: list[Item] = []
    seen_links: set[str] = set()
    relaxed_pool: list[Item] = []

    # China official: baseline high-confidence China signal.
    items.extend(collect_nmpa_site_updates(now_utc, tz_name))
    items.extend(collect_pmda_updates(now_utc, tz_name))

    for src_name, url, default_region, kind in sources:
        data = fetch_bytes(url, timeout=15)
        if not data:
            continue
        feed = feedparser.parse(data)
        for e in feed.entries[:50]:
            title = (getattr(e, "title", "") or "").strip()
            link = (getattr(e, "link", "") or "").strip()
            if not title or not link:
                continue
            if link in seen_links:
                continue
            fallback_ok = False
            combined = title + " " + (getattr(e, "summary", "") or getattr(e, "description", "") or "")
            if kind == "regulatory":
                if not is_regulatory_ivd_relevant(combined):
                    continue
            else:
                if not is_ivd_relevant(combined):
                    # keep as a fallback candidate if it's not obviously pharma-only
                    if not is_relaxed_relevant(combined):
                        continue
                    fallback_ok = True
            pub = to_dt(e)
            if not pub:
                continue
            age = now_utc - pub
            if age > dt.timedelta(days=7):
                continue

            wt = window_tag(pub, now_utc)
            region = cn_region(src_name, link) or default_region
            lane = cn_lane(combined)
            platform = cn_platform(combined)
            event_type = detect_event_type(combined, src_name, link)
            summary = cn_summary(e)

            it = Item(
                title=title,
                link=link,
                published=pub,
                source=src_name,
                region=region,
                lane=lane,
                platform=platform,
                event_type=event_type,
                window_tag=wt,
                summary_cn=summary,
            )
            if kind != "regulatory" and fallback_ok:
                relaxed_pool.append(it)
            else:
                items.append(it)
            seen_links.add(link)

    # Pull a small number of relaxed items to improve international/source diversity.
    if relaxed_pool:
        relaxed_pool.sort(key=lambda x: x.published, reverse=True)
        src_used: dict[str, int] = {}
        for i in items:
            src_used[i.source] = src_used.get(i.source, 0) + 1
        seen_links = {x.link for x in items}
        for it in relaxed_pool:
            if len(items) >= 18:
                break
            if it.link in seen_links:
                continue
            if src_used.get(it.source, 0) >= 3:
                continue
            # prioritize international补充；当条目仍不足时放宽。
            if len(items) >= 10 and it.region not in ("北美", "欧洲"):
                continue
            items.append(it)
            seen_links.add(it.link)
            src_used[it.source] = src_used.get(it.source, 0) + 1

    items = dedupe_items(items)
    items.sort(key=lambda x: x.published, reverse=True)

    # Build candidates: strict 24h first, then 7d补充.
    within_24h = [i for i in items if i.window_tag == "24小时内"]
    within_7d = [i for i in items if i.window_tag == "7天补充"]
    candidates = within_24h[:20]
    if len(candidates) < int(RUNTIME_CONTENT.get("topup_if_24h_lt", 10)):
        candidates.extend(within_7d[:30])
    else:
        candidates = candidates[: int(RUNTIME_CONTENT.get("max_items", 15))]

    yday_titles, recent_titles = load_history_titles(reports_dir, date_str)
    top = choose_top_items(candidates, yday_titles, recent_titles)
    if len(top) < int(RUNTIME_CONTENT.get("min_items", 8)):
        seen = {i.link for i in top}
        used: dict[str, int] = {}
        for i in top:
            used[i.source] = used.get(i.source, 0) + 1
        for it in items:
            if it.link in seen:
                continue
            cap = 3
            if used.get(it.source, 0) >= cap:
                continue
            top.append(it)
            seen.add(it.link)
            used[it.source] = used.get(it.source, 0) + 1
            if len(top) >= int(RUNTIME_CONTENT.get("min_items", 8)):
                break
    top = top[: int(RUNTIME_CONTENT.get("max_items", 15))]
    top = enforce_apac_share(top, items)
    top = enforce_international_primary(top, items)

    # Metrics
    n24 = len([i for i in top if i.window_tag == "24小时内"])
    n7 = len([i for i in top if i.window_tag == "7天补充"])
    apac = len([i for i in top if i.region in ("中国", "亚太")])
    apac_share = (apac / len(top)) if top else 0.0
    regulatory = len([i for i in top if i.event_type == "监管审批与指南"])
    commercial = len(top) - regulatory
    yday_dup = calc_dup_rate(top, yday_titles)
    max_7d_dup = 0.0
    for s in recent_titles.values():
        max_7d_dup = max(max_7d_dup, calc_dup_rate(top, s))
    source_hits = must_source_hits(top)

    # Lane split
    lanes = {"肿瘤检测": [], "感染检测": [], "生殖与遗传检测": [], "其他": []}
    for i in top:
        lanes.setdefault(i.lane, []).append(i)

    # Platform radar
    platforms: dict[str, int] = {}
    for i in top:
        platforms[i.platform] = platforms.get(i.platform, 0) + 1

    # Region heat
    regions = {"北美": 0, "欧洲": 0, "亚太": 0, "中国": 0}
    for i in top:
        regions[i.region] = regions.get(i.region, 0) + 1

    print(f"全球IVD晨报 - {date_str}")
    print()

    print("A. 今日要点（8-15条，按重要性排序）")
    if not top:
        print("1) [7天补充] 今日未抓取到足够的可用条目（RSS源空/网络异常/关键词过滤过严）。")
        print("摘要：建议检查 GitHub Actions 日志与源站可访问性，并补充中国/亚太官方源抓取。")
        print(f"发布日期：{date_str}（北京时间）")
        print("来源：自动生成")
        print("地区：全球")
        print("赛道：其他")
        print("平台：跨平台/未标注")
        print()
    else:
        for idx, i in enumerate(top, 1):
            pub_local = i.published.astimezone(ZoneInfo(tz_name)).strftime("%Y-%m-%d %H:%M %Z")
            print(f"{idx}) [{i.window_tag}] {i.title}")
            print(f"{i.summary_cn}")
            print(f"发布日期：{pub_local}")
            print(f"来源：{i.source} | {i.link}")
            print(f"地区：{i.region}")
            print(f"赛道：{i.lane}")
            print(f"事件类型：{i.event_type}")
            print(f"技术平台：{i.platform}")
            print()

    print("B. 分赛道速览（肿瘤/感染/生殖遗传/其他）")
    for k in ["肿瘤检测", "感染检测", "生殖与遗传检测", "其他"]:
        print(f"- {k}：{len(lanes.get(k, []))} 条（以当日抓取为准）")
    print()

    print("C. 技术平台雷达（按平台汇总当日进展）")
    if platforms:
        for p, c in sorted(platforms.items(), key=lambda x: (-x[1], x[0])):
            print(f"- {p}：{c} 条")
    else:
        print("- 今日无有效平台统计。")
    print()

    print("D. 区域热力图（北美/欧洲/亚太/中国）")
    print(f"- 北美：{regions.get('北美', 0)}")
    print(f"- 欧洲：{regions.get('欧洲', 0)}")
    print(f"- 亚太：{regions.get('亚太', 0)}")
    print(f"- 中国：{regions.get('中国', 0)}")
    print()

    print("E. 三条关键趋势判断（产业与技术各至少1条）")
    print("1) 产业：并购合作与产品注册更聚焦可快速放量场景，商业化节奏正由渠道与准入共同决定。")
    print("2) 技术：PCR/NGS/免疫平台继续并行，组合菜单与自动化能力是实验室端的核心竞争变量。")
    print("3) 监管：亚太监管与中国追溯体系持续强化，跨区域上市正从“单点获批”转向“体系化合规”。")
    print()

    print("F. 信息缺口与次日跟踪清单（3-5条）")
    print("1) 继续补齐中国招采高金额公告（ccgp/省级平台/三甲医院），提升需求侧信号强度。")
    print("2) 跟踪亚太监管站点（TGA/HSA/PMDA/MFDS）新增审批与召回，避免区域偏置。")
    print("3) 对并购融资与产品发布条目做二次核验，优先采用公司公告与监管数据库。")
    print("4) 对未命中的必查信源建立备用抓取路径（网页列表页 + 日期解析）。")
    print()

    print("G. 质量指标 (Quality Audit)")
    print(
        f"24H条目数 / 7D补充数：{n24} / {n7} | "
        f"亚太占比：{apac_share:.0%} | "
        f"商业与监管事件比：{commercial}:{regulatory} | "
        f"必查信源命中清单：{source_hits} | "
        f"重复率(昨/7日峰值)：{yday_dup:.0%}/{max_7d_dup:.0%} | "
        f"rules_profile={runtime_rules.get('active_profile', 'legacy')}"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
