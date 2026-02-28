"use client";

import { FeedFilters, FeedUIState, TimeRange } from "@/lib/feed-controller";
import CompactToggle from "@/components/feed2/CompactToggle";

type Props = {
  filters: FeedFilters;
  setFilters: (next: FeedFilters) => void;
  ui: FeedUIState;
  setUI: (next: FeedUIState) => void;
  showAggregatorToggle?: boolean;
  regionOptions?: Array<{ value: string; count: number }>;
  eventTypeOptions?: Array<{ value: string; count: number }>;
};

function setField(filters: FeedFilters, setFilters: (next: FeedFilters) => void, key: keyof FeedFilters, value: string) {
  setFilters({ ...filters, [key]: value });
}

export default function FilterPanel({
  filters,
  setFilters,
  ui,
  setUI,
  showAggregatorToggle = false,
  regionOptions = [],
  eventTypeOptions = [],
}: Props) {
  return (
    <aside className="sticky top-4 space-y-3 rounded-lg border border-line bg-panel p-4">
      <div>
        <label className="mb-1 block text-xs text-muted">Time Range</label>
        <select
          className="w-full rounded-md border border-line bg-bg px-2 py-2 text-sm"
          value={filters.range}
          onChange={(e) => setField(filters, setFilters, "range", e.target.value as TimeRange)}
        >
          <option value="all">All-time</option>
          <option value="24h">24h</option>
          <option value="7d">7d</option>
          <option value="30d">30d</option>
          <option value="custom">Custom</option>
        </select>
      </div>

      {filters.range === "custom" ? (
        <div className="grid grid-cols-2 gap-2">
          <input
            type="date"
            className="rounded-md border border-line bg-bg px-2 py-2 text-sm"
            value={filters.start}
            onChange={(e) => setField(filters, setFilters, "start", e.target.value)}
          />
          <input
            type="date"
            className="rounded-md border border-line bg-bg px-2 py-2 text-sm"
            value={filters.end}
            onChange={(e) => setField(filters, setFilters, "end", e.target.value)}
          />
        </div>
      ) : null}

      <div>
        <label className="mb-1 block text-xs text-muted">Region</label>
        <select
          className="w-full rounded-md border border-line bg-bg px-2 py-2 text-sm"
          value={filters.region}
          onChange={(e) => setField(filters, setFilters, "region", e.target.value)}
        >
          <option value="">All</option>
          {regionOptions.map((x) => (
            <option key={x.value} value={x.value === "Unknown" ? "" : x.value}>
              {x.value} ({x.count})
            </option>
          ))}
        </select>
      </div>
      <div>
        <label className="mb-1 block text-xs text-muted">Event type</label>
        <select
          className="w-full rounded-md border border-line bg-bg px-2 py-2 text-sm"
          value={filters.event_type}
          onChange={(e) => setField(filters, setFilters, "event_type", e.target.value)}
        >
          <option value="">All</option>
          {eventTypeOptions.map((x) => (
            <option key={x.value} value={x.value === "Unknown" ? "" : x.value}>
              {x.value} ({x.count})
            </option>
          ))}
        </select>
      </div>
      <div>
        <label className="mb-1 block text-xs text-muted">Trust tier</label>
        <select
          className="w-full rounded-md border border-line bg-bg px-2 py-2 text-sm"
          value={filters.trust_tier}
          onChange={(e) => setField(filters, setFilters, "trust_tier", e.target.value)}
        >
          <option value="">All</option>
          <option value="A">A</option>
          <option value="B">B</option>
          <option value="C">C</option>
        </select>
      </div>
      <div>
        <label className="mb-1 block text-xs text-muted">Source id</label>
        <input
          className="w-full rounded-md border border-line bg-bg px-2 py-2 text-sm"
          value={filters.source_id}
          onChange={(e) => setField(filters, setFilters, "source_id", e.target.value)}
          placeholder="输入 source_id"
        />
      </div>

      <div className="space-y-2 border-t border-line pt-2">
        {showAggregatorToggle ? (
          <CompactToggle
            checked={ui.includeAggregators}
            onChange={(v) => setUI({ ...ui, includeAggregators: v })}
            label="Include aggregators"
          />
        ) : null}
        <CompactToggle checked={ui.hideUnknown} onChange={(v) => setUI({ ...ui, hideUnknown: v })} label="Hide Unknown fields" />
        <CompactToggle checked={ui.compact} onChange={(v) => setUI({ ...ui, compact: v })} label="Compact mode" />
        <CompactToggle checked={ui.pinOnly} onChange={(v) => setUI({ ...ui, pinOnly: v })} label="Pin only" />
      </div>
    </aside>
  );
}
