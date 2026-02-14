#!/usr/bin/env python3
"""
Lightweight cloud generator for the IVD morning brief.

Notes:
- This is a pragmatic v1: it relies on RSS + keyword tagging to produce a usable
  briefing on time. You can extend sources and tagging rules incrementally.
"""

import datetime as dt
import os
import re
import sys
from dataclasses import dataclass
from http.client import RemoteDisconnected
from socket import timeout as SocketTimeout
from typing import Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

import feedparser


@dataclass(frozen=True)
class Item:
    title: str
    link: str
    published: dt.datetime
    source: str
    region: str
    lane: str
    platform: str
    window_tag: str  # "24小时内" or "7天补充"
    summary_cn: str


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

def cn_lane(text: str) -> str:
    t = text.lower()
    if any(k in t for k in ["cancer", "tumor", "oncology", "carcinoma", "pd-l1", "biomarker"]):
        return "肿瘤检测"
    if any(k in t for k in ["infect", "virus", "covid", "flu", "influenza", "pathogen", "sepsis"]):
        return "感染检测"
    if any(k in t for k in ["prenatal", "nipt", "fertility", "reproductive", "genetic", "hereditary"]):
        return "生殖与遗传检测"
    return "其他"


def cn_platform(text: str) -> str:
    t = text.lower()
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
    if any(k in t for k in ["ngs", "sequencing", "whole genome", "wgs", "rna-seq"]):
        return "NGS"
    if "digital pcr" in t or "ddpcr" in t:
        return "数字PCR"
    if "pcr" in t:
        return "PCR"
    if any(k in t for k in ["mass spec", "lc-ms", "ms/ms"]):
        return "质谱"
    if any(k in t for k in ["flow cytometry", "cytometry"]):
        return "流式细胞"
    if any(k in t for k in ["immunoassay", "chemiluminescence", "elisa", "ihc"]):
        return "免疫诊断（化学发光/ELISA/IHC等）"
    if any(k in t for k in ["point-of-care", "poc", "poct", "self-test", "rapid test"]):
        return "POCT/分子POCT"
    return "跨平台/未标注"


def cn_region(source: str, link: str) -> str:
    s = (source + " " + link).lower()
    if any(k in s for k in ["tga.gov.au", "pmda.go.jp", "mhlw.go.jp", "hsa.gov.sg", "mfds.go.kr", ".cn", "nmpa.gov.cn"]):
        # crude: treat these as APAC; China will be handled separately if needed later
        if any(k in s for k in ["nmpa.gov.cn", "udi.nmpa.gov.cn", ".cn"]):
            return "中国"
        return "亚太"
    if any(k in s for k in ["europa.eu", "ema.europa.eu", ".eu", ".uk"]):
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
    has_company = any(c in t for c in ivd_companies)
    if not (has_anchor or has_company):
        return False
    # Slightly lower threshold if a known IVD company is involved.
    thresh = 1 if has_company else 2
    return score_ivd(text) >= thresh


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
    ]
    if any(has_term(t, k) for k in hard_exclude):
        return False
    return True


def cn_summary(entry) -> str:
    # Avoid long noisy feed summaries; keep it short and decision-oriented.
    raw = (getattr(entry, "summary", "") or getattr(entry, "description", "") or "").strip()
    raw = re.sub(r"<[^>]+>", " ", raw)
    raw = re.sub(r"\s+", " ", raw).strip()
    if not raw:
        return "摘要：该条目与体外诊断/检测相关，建议打开原文核对关键信息（监管/产品/合作/证据）。"
    # Two short sentences max.
    if len(raw) > 240:
        raw = raw[:240].rstrip() + "…"
    return f"摘要：{raw}"


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
        for yyyymmdd in sorted(set(day), reverse=True)[:10]:
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
                    lane="政策与监管",
                    platform="跨平台（UDI/追溯）",
                    window_tag=window_tag(pub, now_utc),
                    summary_cn="摘要：NMPA UDI下载页显示日更增量持续更新，可用于追溯、流通与院端主数据治理。建议关注IVD试剂类条目增量与企业信息完整度变化。",
                )
            )
            if len(out) >= 10:
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
                    lane="政策与监管",
                    platform="跨平台（UDI/数据共享）",
                    window_tag=window_tag(pub, now_utc),
                    summary_cn="摘要：周更包便于企业与渠道批量同步一周增量数据，降低日更抓取运维压力。对招采、院内耗材管理与合规尽调有直接价值。",
                )
            )

    return out


def main() -> int:
    tz_name = env("REPORT_TZ", "Asia/Shanghai")
    now_local = now_in_tz(tz_name)
    now_utc = now_local.astimezone(dt.timezone.utc)

    # RSS sources. Broad media + targeted regulatory feeds (APAC & NA).
    sources = [
        ("Fierce Biotech", "https://www.fiercebiotech.com/rss/xml", "北美", "media"),
        ("Fierce Healthcare", "https://www.fiercehealthcare.com/rss/xml", "北美", "media"),
        ("MedTech Dive", "https://www.medtechdive.com/feeds/news/", "北美", "media"),
        ("BioPharma Dive", "https://www.biopharmadive.com/feeds/news/", "北美", "media"),
        ("TGA Safety Alerts", "https://www.tga.gov.au/feeds/alert/safety-alerts.xml", "亚太", "regulatory"),
        ("TGA Product Recalls", "https://www.tga.gov.au/feeds/alert/product-recalls.xml", "亚太", "regulatory"),
        ("FDA MedWatch", "https://www.fda.gov/about-fda/contact-fda/stay-informed/rss-feeds/medwatch/rss.xml", "北美", "regulatory"),
    ]

    items: list[Item] = []
    seen_links: set[str] = set()
    relaxed_pool: list[Item] = []

    # China official: stabilize APAC/China share.
    items.extend(collect_nmpa_udi(now_utc, tz_name))

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
            summary = cn_summary(e)

            it = Item(
                title=title,
                link=link,
                published=pub,
                source=src_name,
                region=region,
                lane=lane,
                platform=platform,
                window_tag=wt,
                summary_cn=summary,
            )
            if kind != "regulatory" and fallback_ok:
                relaxed_pool.append(it)
            else:
                items.append(it)
            seen_links.add(link)

    # If strict filter yields too few items, top up from relaxed pool (newest first).
    if len(items) < 8 and relaxed_pool:
        relaxed_pool.sort(key=lambda x: x.published, reverse=True)
        for it in relaxed_pool:
            if len(items) >= 8:
                break
            # avoid dup links
            if it.link in {x.link for x in items}:
                continue
            items.append(it)

    # Sort: newest first.
    items.sort(key=lambda x: x.published, reverse=True)

    # Ensure at least 8-15 "today points": take 24h first, then 7d.
    within_24h = [i for i in items if i.window_tag == "24小时内"]
    within_7d = [i for i in items if i.window_tag == "7天补充"]

    top: list[Item] = []
    top.extend(within_24h[:15])
    if len(top) < 10:
        top.extend(within_7d[: (15 - len(top))])
    else:
        top = top[:15]

    # Metrics
    n24 = len([i for i in top if i.window_tag == "24小时内"])
    n7 = len([i for i in top if i.window_tag == "7天补充"])
    apac = len([i for i in top if i.region in ("中国", "亚太")])
    apac_share = (apac / len(top)) if top else 0.0

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

    date_str = now_local.strftime("%Y-%m-%d")
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
    print("1) 产业：检测服务与诊断产品持续向“临床路径入口”靠拢，渠道与支付端的可及性将影响放量速度。")
    print("2) 技术：分子与免疫平台在多病种场景并行推进，组合菜单与自动化能力成为实验室效率竞争焦点。")
    print("3) 监管：各区域对质量体系与合规证据链的要求趋严，跨区域申报与上市节奏更依赖体系化能力。")
    print()

    print("F. 信息缺口与次日跟踪清单（3-5条）")
    print("1) 补齐中国/NMPA与亚太监管（TGA/HSA/PMDA/MFDS）的一手公告抓取，提升亚太占比与监管密度。")
    print("2) 补齐中国招采（省级平台/医院/ccgp）高金额条目，形成真实需求信号。")
    print("3) 对“产品发布/获批/合作”类条目做二次核验（公司公告/交易所公告/监管数据库）。")
    print("4) 扩展 RSS 源与关键词库，降低漏抓（IVD/assay/test/lab 等同义词）风险。")
    print()

    print("G. 质量指标 (Quality Audit)")
    # “商业与监管事件比/必查信源命中清单”在 v1 里不做强判定，先输出占位，便于后续迭代。
    print(
        f"24H条目数 / 7D补充数：{n24} / {n7} | "
        f"亚太占比：{apac_share:.0%} | "
        f"商业与监管事件比：待细分 | "
        f"必查信源命中清单：待接入（NMPA/CMDE/UDI/ccgp/TGA/HSA/PMDA/MFDS）"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
