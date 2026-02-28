"use client";

import TimeRangePicker, { TimeRange } from "@/components/TimeRangePicker";

export type FilterState = {
  group: string;
  region: string;
  event_type: string;
  trust_tier: string;
  source_id: string;
  range: TimeRange;
  start: string;
  end: string;
};

type Props = {
  value: FilterState;
  onChange: (next: FilterState) => void;
};

function setField(value: FilterState, onChange: (next: FilterState) => void, key: keyof FilterState, v: string) {
  onChange({ ...value, [key]: v });
}

export default function Filters({ value, onChange }: Props) {
  return (
    <div className="space-y-3 rounded-lg border border-line bg-panel p-4">
      <div>
        <label className="mb-1 block text-xs text-muted">group</label>
        <input
          className="w-full rounded-md border border-line bg-bg px-2 py-2 text-sm"
          placeholder="media/regulatory/..."
          value={value.group}
          onChange={(e) => setField(value, onChange, "group", e.target.value)}
        />
      </div>
      <div>
        <label className="mb-1 block text-xs text-muted">region</label>
        <input
          className="w-full rounded-md border border-line bg-bg px-2 py-2 text-sm"
          placeholder="北美/欧洲/中国..."
          value={value.region}
          onChange={(e) => setField(value, onChange, "region", e.target.value)}
        />
      </div>
      <div>
        <label className="mb-1 block text-xs text-muted">event_type</label>
        <input
          className="w-full rounded-md border border-line bg-bg px-2 py-2 text-sm"
          placeholder="regulatory/procurement/..."
          value={value.event_type}
          onChange={(e) => setField(value, onChange, "event_type", e.target.value)}
        />
      </div>
      <div>
        <label className="mb-1 block text-xs text-muted">trust_tier</label>
        <select
          className="w-full rounded-md border border-line bg-bg px-2 py-2 text-sm"
          value={value.trust_tier}
          onChange={(e) => setField(value, onChange, "trust_tier", e.target.value)}
        >
          <option value="">All</option>
          <option value="A">A</option>
          <option value="B">B</option>
          <option value="C">C</option>
        </select>
      </div>
      <div>
        <label className="mb-1 block text-xs text-muted">source_id</label>
        <input
          className="w-full rounded-md border border-line bg-bg px-2 py-2 text-sm"
          placeholder="输入 source_id"
          value={value.source_id}
          onChange={(e) => setField(value, onChange, "source_id", e.target.value)}
        />
      </div>
      <TimeRangePicker
        range={value.range}
        start={value.start}
        end={value.end}
        onChange={(next) =>
          onChange({
            ...value,
            range: next.range,
            start: next.start ?? value.start,
            end: next.end ?? value.end
          })
        }
      />
    </div>
  );
}
