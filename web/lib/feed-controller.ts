"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { buildUrl, fetchJson } from "@/lib/api";
import { loadPins, togglePin } from "@/lib/pin";
import { FeedItem, FeedResponse, FeedSummaryResponse } from "@/lib/types";
import { isUnknown } from "@/lib/time";

export type TimeRange = "all" | "24h" | "7d" | "30d" | "custom";

export type FeedFilters = {
  group: string;
  region: string;
  event_type: string;
  trust_tier: string;
  source_id: string;
  range: TimeRange;
  start: string;
  end: string;
};

export type FeedUIState = {
  compact: boolean;
  hideUnknown: boolean;
  pinOnly: boolean;
  autoRefresh: boolean;
  includeAggregators: boolean;
};

type FeedMode = "story" | "item";
type OptionCount = { value: string; count: number };

function parseRange(v: string): TimeRange {
  if (v === "24h" || v === "7d" || v === "30d" || v === "custom") return v;
  return "all";
}

function readSavedRange(mode: FeedMode): { range: TimeRange; start: string; end: string } | null {
  if (typeof window === "undefined") return null;
  try {
    const raw = window.localStorage.getItem(`feed_range_${mode}`);
    if (!raw) return null;
    const obj = JSON.parse(raw) as { range?: string; start?: string; end?: string };
    const range = parseRange(String(obj?.range || "all"));
    return { range, start: String(obj?.start || ""), end: String(obj?.end || "") };
  } catch {
    return null;
  }
}

function parseBool(v: string | null, fallback: boolean): boolean {
  if (v === null) return fallback;
  return v === "1" || v.toLowerCase() === "true" || v.toLowerCase() === "on";
}

function dedupe(items: FeedItem[]): FeedItem[] {
  const seen = new Set<string>();
  const out: FeedItem[] = [];
  for (const it of items) {
    const key = `${it.id || ""}::${it.url || ""}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(it);
  }
  return out;
}

function isAggregatorSourceId(sourceId: string | undefined): boolean {
  const sid = String(sourceId || "").trim().toLowerCase();
  if (!sid) return false;
  return sid.startsWith("aggregator-news-google-com-google-news") || sid.includes("google-news");
}

function rangeToStartEnd(range: TimeRange, start: string, end: string): { start?: string; end?: string } {
  if (range === "custom") return { start: start || undefined, end: end || undefined };
  const now = new Date();
  if (range === "24h") return { start: new Date(now.getTime() - 24 * 3600 * 1000).toISOString() };
  if (range === "7d") return { start: new Date(now.getTime() - 7 * 24 * 3600 * 1000).toISOString() };
  if (range === "30d") return { start: new Date(now.getTime() - 30 * 24 * 3600 * 1000).toISOString() };
  return {};
}

export function useFeedController(mode: FeedMode = "story") {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const abortRef = useRef<AbortController | null>(null);
  const endpoint = mode === "story" ? "/api/feed" : "/api/feed-items";

  const [q, setQ] = useState(() => searchParams.get("q") || "");
  const [filters, setFilters] = useState<FeedFilters>(() => ({
    group: searchParams.get("group") || "",
    region: searchParams.get("region") || "",
    event_type: searchParams.get("event_type") || "",
    trust_tier: searchParams.get("trust_tier") || "",
    source_id: searchParams.get("source_id") || "",
    range: parseRange(searchParams.get("range") || "all"),
    start: searchParams.get("start") || "",
    end: searchParams.get("end") || ""
  }));
  const [ui, setUI] = useState<FeedUIState>(() => ({
    compact: parseBool(searchParams.get("compact"), false),
    hideUnknown: parseBool(searchParams.get("hide_unknown"), false),
    pinOnly: parseBool(searchParams.get("pin_only"), false),
    autoRefresh: parseBool(searchParams.get("auto"), true),
    includeAggregators: mode === "item" ? true : parseBool(searchParams.get("include_aggregators"), false),
  }));

  const [items, setItems] = useState<FeedItem[]>([]);
  const [viewMode, setViewMode] = useState<string>(() => (mode === "story" ? (searchParams.get("view") || "balanced") : "latest"));
  const [pageSize, setPageSize] = useState<number>(() => {
    const raw = Number(searchParams.get("limit") || 30);
    return [30, 50, 100].includes(raw) ? raw : 30;
  });
  const [currentCursor, setCurrentCursor] = useState<string>("");
  const [nextCursor, setNextCursor] = useState<string | null>(null);
  const [cursorHistory, setCursorHistory] = useState<string[]>([]);
  const [pageIndex, setPageIndex] = useState<number>(1);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [lastUpdated, setLastUpdated] = useState<string>("");
  const [pollNotice, setPollNotice] = useState("");
  const [pins, setPins] = useState<Set<string>>(new Set<string>());
  const [globalSummary, setGlobalSummary] = useState<FeedSummaryResponse | null>(null);

  useEffect(() => {
    setPins(loadPins());
  }, []);

  useEffect(() => {
    const hasRangeInUrl = !!(searchParams.get("range") || searchParams.get("start") || searchParams.get("end"));
    if (hasRangeInUrl) return;
    const saved = readSavedRange(mode);
    if (!saved) return;
    setFilters((prev) => ({ ...prev, range: saved.range, start: saved.start, end: saved.end }));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [mode]);

  const requestParams = useMemo(() => {
    const t = rangeToStartEnd(filters.range, filters.start, filters.end);
    return {
      q,
      group: filters.group,
      region: filters.region,
      event_type: filters.event_type,
      trust_tier: filters.trust_tier,
      source_id: filters.source_id,
      view_mode: viewMode,
      start: t.start,
      end: t.end,
      limit: pageSize
    };
  }, [filters, q, viewMode, pageSize]);

  useEffect(() => {
    const sp = new URLSearchParams();
    if (q.trim()) sp.set("q", q.trim());
    if (filters.group.trim()) sp.set("group", filters.group.trim());
    if (filters.region.trim()) sp.set("region", filters.region.trim());
    if (filters.event_type.trim()) sp.set("event_type", filters.event_type.trim());
    if (filters.trust_tier.trim()) sp.set("trust_tier", filters.trust_tier.trim());
    if (filters.source_id.trim()) sp.set("source_id", filters.source_id.trim());
    if (mode === "story" && viewMode) sp.set("view", viewMode);
    if (mode === "story" && pageSize !== 30) sp.set("limit", String(pageSize));
    if (filters.range !== "all") sp.set("range", filters.range);
    if (filters.start.trim()) sp.set("start", filters.start.trim());
    if (filters.end.trim()) sp.set("end", filters.end.trim());
    if (ui.compact) sp.set("compact", "1");
    if (ui.hideUnknown) sp.set("hide_unknown", "1");
    if (ui.pinOnly) sp.set("pin_only", "1");
    if (!ui.autoRefresh) sp.set("auto", "0");
    if (mode === "story" && ui.includeAggregators) sp.set("include_aggregators", "1");
    const qs = sp.toString();
    router.replace(qs ? `${pathname}?${qs}` : pathname, { scroll: false });
  }, [q, filters, ui, pathname, router, mode, viewMode, pageSize]);

  useEffect(() => {
    if (typeof window === "undefined") return;
    try {
      window.localStorage.setItem(
        `feed_range_${mode}`,
        JSON.stringify({ range: filters.range, start: filters.start, end: filters.end })
      );
    } catch {
      // ignore local storage failures
    }
  }, [mode, filters.range, filters.start, filters.end]);

  const fetchPage = async (cursor: string) => {
    setLoading(true);
    setError("");
    setPollNotice("");
    abortRef.current?.abort();
    const ac = new AbortController();
    abortRef.current = ac;
    try {
      const data = await fetchJson<FeedResponse>(buildUrl(endpoint, { ...requestParams, cursor }), ac.signal);
      setItems(dedupe(data.items || []));
      setNextCursor(data.next_cursor ?? null);
      setLastUpdated(new Date().toISOString());
    } catch (e) {
      const msg = String((e as Error)?.message || "");
      // Normal race during rapid filter/page changes; do not surface as user-facing error.
      if (msg.includes("aborted") || msg.includes("AbortError")) return;
      setError(msg);
    } finally {
      setLoading(false);
    }
  };

  const refresh = async () => fetchPage(currentCursor);

  const resetPaging = () => {
    setCurrentCursor("");
    setCursorHistory([]);
    setPageIndex(1);
  };

  const goNext = async () => {
    if (!nextCursor || loading) return;
    setCursorHistory((prev) => [...prev, currentCursor]);
    setCurrentCursor(nextCursor);
    setPageIndex((x) => x + 1);
    await fetchPage(nextCursor);
  };

  const goPrev = async () => {
    if (cursorHistory.length === 0 || loading) return;
    const prevCursor = cursorHistory[cursorHistory.length - 1] || "";
    setCursorHistory((prev) => prev.slice(0, -1));
    setCurrentCursor(prevCursor);
    setPageIndex((x) => Math.max(1, x - 1));
    await fetchPage(prevCursor);
  };

  const goFirst = async () => {
    resetPaging();
    await fetchPage("");
  };

  useEffect(() => {
    resetPaging();
    void fetchPage("");
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [requestParams]);

  useEffect(() => {
    if (!ui.autoRefresh || items.length === 0) return;
    const latest = items[0]?.published_at;
    if (!latest) return;
    const t = setInterval(async () => {
      try {
        const data = await fetchJson<FeedResponse>(
          buildUrl(endpoint, { ...requestParams, since: latest, limit: 20, cursor: undefined })
        );
        const incoming = data.items || [];
        if (incoming.length > 0) {
          setItems((prev) => dedupe([...incoming, ...prev]));
          setLastUpdated(new Date().toISOString());
          setPollNotice("");
        }
      } catch {
        setPollNotice("Auto pull may be unsupported by backend; use Refresh.");
      }
    }, 60_000);
    return () => clearInterval(t);
  }, [ui.autoRefresh, items, requestParams, endpoint]);

  useEffect(() => {
    let cancelled = false;
    void fetchJson<FeedSummaryResponse>(buildUrl("/api/feed-summary", { mode }))
      .then((data) => {
        if (!cancelled) setGlobalSummary(data);
      })
      .catch(() => {
        if (!cancelled) setGlobalSummary(null);
      });
    return () => {
      cancelled = true;
    };
  }, [mode]);

  const visibleItems = useMemo(() => {
    let out = items.slice();
    if (mode === "story" && !ui.includeAggregators) {
      out = out.filter((it) => !isAggregatorSourceId(it.source_id));
    }
    if (ui.hideUnknown) {
      out = out.filter((it) => !isUnknown(it.group) && !isUnknown(it.region) && !isUnknown(it.event_type));
    }
    if (ui.pinOnly) {
      out = out.filter((it) => {
        const key = it.id || it.url || "";
        return key ? pins.has(key) : false;
      });
    }
    return out;
  }, [items, mode, ui.includeAggregators, ui.hideUnknown, ui.pinOnly, pins]);

  const summary = useMemo(() => {
    const total = visibleItems.length;
    const byGroup: Record<string, number> = {};
    let unknownRegion = 0;
    let unknownEventType = 0;
    for (const it of visibleItems) {
      const g = isUnknown(it.group) ? "unknown" : String(it.group).toLowerCase();
      byGroup[g] = (byGroup[g] || 0) + 1;
      if (isUnknown(it.region)) unknownRegion += 1;
      if (isUnknown(it.event_type)) unknownEventType += 1;
    }
    return {
      total,
      byGroup,
      unknownRegionRate: total ? unknownRegion / total : 0,
      unknownEventTypeRate: total ? unknownEventType / total : 0
    };
  }, [visibleItems]);

  const scopeLabel = useMemo(() => {
    if (filters.range === "custom") {
      const s = filters.start || "N/A";
      const e = filters.end || "N/A";
      return `Time scope: custom (${s} ~ ${e})`;
    }
    return `Time scope: ${filters.range}`;
  }, [filters.range, filters.start, filters.end]);

  const regionOptions = useMemo<OptionCount[]>(() => {
    const m = new Map<string, number>();
    for (const it of items) {
      const v = (it.region && String(it.region).trim()) || "Unknown";
      m.set(v, (m.get(v) || 0) + 1);
    }
    return Array.from(m.entries())
      .map(([value, count]) => ({ value, count }))
      .sort((a, b) => b.count - a.count || a.value.localeCompare(b.value));
  }, [items]);

  const eventTypeOptions = useMemo<OptionCount[]>(() => {
    const m = new Map<string, number>();
    for (const it of items) {
      const v = (it.event_type && String(it.event_type).trim()) || "Unknown";
      m.set(v, (m.get(v) || 0) + 1);
    }
    return Array.from(m.entries())
      .map(([value, count]) => ({ value, count }))
      .sort((a, b) => b.count - a.count || a.value.localeCompare(b.value));
  }, [items]);

  return {
    q,
    setQ,
    filters,
    setFilters,
    ui,
    setUI,
    viewMode,
    setViewMode,
    pageSize,
    setPageSize,
    items: visibleItems,
    rawItems: items,
    pageIndex,
    hasPrev: cursorHistory.length > 0,
    hasNext: !!nextCursor,
    nextCursor,
    loading,
    error,
    refresh,
    goNext,
    goPrev,
    goFirst,
    summary,
    scopeLabel,
    globalSummary,
    regionOptions,
    eventTypeOptions,
    lastUpdated,
    pollNotice,
    pins,
    isPinned: (item: FeedItem) => {
      const key = item.id || item.url || "";
      return key ? pins.has(key) : false;
    },
    togglePin: (item: FeedItem) => {
      const key = item.id || item.url || "";
      if (!key) return;
      setPins((prev) => togglePin(prev, key));
    }
  };
}
