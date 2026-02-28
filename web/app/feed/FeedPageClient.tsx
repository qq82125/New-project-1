"use client";

import { useMemo, useState } from "react";
import DetailDrawer from "@/components/feed2/DetailDrawer";
import FeedList from "@/components/feed2/FeedList";
import FilterPanel from "@/components/feed2/FilterPanel";
import GroupTabs from "@/components/feed2/GroupTabs";
import SummaryBar from "@/components/feed2/SummaryBar";
import Pagination from "@/components/terminal/Pagination";
import { useFeedController } from "@/lib/feed-controller";
import { FeedItem } from "@/lib/types";
import { toDisplayTime } from "@/lib/time";

export default function FeedPageClient() {
  const c = useFeedController("story");
  const [activeItem, setActiveItem] = useState<FeedItem | null>(null);

  const lastUpdatedText = useMemo(
    () => (c.lastUpdated ? toDisplayTime(c.lastUpdated) : "not yet"),
    [c.lastUpdated]
  );

  return (
    <div className="space-y-4">
      <GroupTabs value={c.filters.group} onChange={(v) => c.setFilters({ ...c.filters, group: v })} />
      <div className="flex items-center gap-2 rounded-lg border border-line bg-panel p-2">
        {[
          { id: "balanced", label: "Balanced" },
          { id: "signal", label: "Signal" },
          { id: "latest", label: "Latest" },
        ].map((m) => (
          <button
            key={m.id}
            className={`rounded px-3 py-1 text-sm ${c.viewMode === m.id ? "bg-blue-600 text-white" : "border border-line text-muted"}`}
            onClick={() => c.setViewMode(m.id)}
          >
            {m.label}
          </button>
        ))}
        <div className="ml-auto flex items-center gap-2 text-xs text-muted">
          <span title="每页事件数量">Per page</span>
          <select
            className="rounded border border-line bg-bg px-2 py-1 text-xs"
            value={c.pageSize}
            onChange={(e) => c.setPageSize(Number(e.target.value))}
            title="切换每页展示数量（会重置到第1页）"
          >
            <option value={30}>30</option>
            <option value={50}>50</option>
            <option value={100}>100</option>
          </select>
        </div>
      </div>
      <div className="rounded-lg border border-line bg-panel px-3 py-2 text-xs text-muted">
        口径说明：
        <span className="ml-2">
          <b>Balanced</b> 分组配额混排（默认，防止 media 刷屏）；
        </span>
        <span className="ml-2">
          <b>Signal</b> 按规则信号分排序（trust/priority/sources/recency）；
        </span>
        <span className="ml-2">
          <b>Latest</b> 纯时间倒序流。
        </span>
      </div>

      <div className="flex flex-wrap items-center gap-2 rounded-lg border border-line bg-panel px-3 py-2">
        <input
          className="min-w-[260px] flex-1 rounded-md border border-line bg-bg px-3 py-2 text-sm"
          placeholder="Search..."
          value={c.q}
          onChange={(e) => c.setQ(e.target.value)}
        />
        <button className="rounded border border-line px-3 py-2 text-sm" onClick={() => void c.refresh()} disabled={c.loading}>
          Refresh
        </button>
        <label className="flex items-center gap-1 text-xs text-muted">
          Auto
          <input
            type="checkbox"
            checked={c.ui.autoRefresh}
            onChange={(e) => c.setUI({ ...c.ui, autoRefresh: e.target.checked })}
          />
        </label>
        <span className="text-xs text-muted">Last updated: {lastUpdatedText}</span>
      </div>

      <SummaryBar
        total={c.summary.total}
        byGroup={c.summary.byGroup}
        unknownRegionRate={c.summary.unknownRegionRate}
        unknownEventTypeRate={c.summary.unknownEventTypeRate}
        scopeLabel={c.scopeLabel}
        globalTotal={c.globalSummary?.total}
        globalByGroup={c.globalSummary?.by_group}
        globalUnknownRegionRate={c.globalSummary?.unknown_region_rate}
        globalUnknownEventTypeRate={c.globalSummary?.unknown_event_type_rate}
      />

      <div className="grid grid-cols-1 gap-4 xl:grid-cols-[300px_minmax(0,1fr)]">
        <FilterPanel
          filters={c.filters}
          setFilters={c.setFilters}
          ui={c.ui}
          setUI={c.setUI}
          showAggregatorToggle
          regionOptions={c.regionOptions}
          eventTypeOptions={c.eventTypeOptions}
        />

        <section className="space-y-3">
          {c.error ? <div className="rounded border border-red-500/50 bg-red-900/25 p-3 text-sm text-red-200">{c.error}</div> : null}
          {c.pollNotice ? <div className="rounded border border-yellow-500/50 bg-yellow-900/25 p-3 text-sm text-yellow-200">{c.pollNotice}</div> : null}
          {c.loading ? (
            <div className="text-sm text-muted">Loading...</div>
          ) : (
            <FeedList
              items={c.items}
              compact={c.ui.compact}
              isPinned={c.isPinned}
              onTogglePin={c.togglePin}
              onOpen={setActiveItem}
            />
          )}
          <Pagination
            pageIndex={c.pageIndex}
            hasPrev={c.hasPrev}
            hasNext={c.hasNext}
            onPrev={() => void c.goPrev()}
            onNext={() => void c.goNext()}
            onFirst={() => void c.goFirst()}
            disabled={c.loading}
          />
        </section>
      </div>

      <DetailDrawer
        mode="story"
        open={!!activeItem}
        item={activeItem}
        pinned={activeItem ? c.isPinned(activeItem) : false}
        onClose={() => setActiveItem(null)}
        onTogglePin={c.togglePin}
      />
    </div>
  );
}
