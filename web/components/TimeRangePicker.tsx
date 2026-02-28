"use client";

export type TimeRange = "all" | "24h" | "7d" | "30d" | "custom";

type Props = {
  range: TimeRange;
  start: string;
  end: string;
  onChange: (next: { range: TimeRange; start?: string; end?: string }) => void;
};

export default function TimeRangePicker({ range, start, end, onChange }: Props) {
  return (
    <div className="space-y-2">
      <label className="text-xs text-muted">时间范围</label>
      <select
        className="w-full rounded-md border border-line bg-bg px-2 py-2 text-sm"
        value={range}
        onChange={(e) => onChange({ range: e.target.value as TimeRange })}
      >
        <option value="all">All-time</option>
        <option value="24h">24h</option>
        <option value="7d">7d</option>
        <option value="30d">30d</option>
        <option value="custom">Custom</option>
      </select>
      {range === "custom" ? (
        <div className="grid grid-cols-2 gap-2">
          <input
            type="date"
            className="rounded-md border border-line bg-bg px-2 py-2 text-sm"
            value={start}
            onChange={(e) => onChange({ range, start: e.target.value })}
          />
          <input
            type="date"
            className="rounded-md border border-line bg-bg px-2 py-2 text-sm"
            value={end}
            onChange={(e) => onChange({ range, end: e.target.value })}
          />
        </div>
      ) : null}
    </div>
  );
}
