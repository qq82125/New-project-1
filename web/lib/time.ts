export function toRelativeTime(value?: string): string {
  if (!value) return "unknown";
  const t = new Date(value).getTime();
  if (Number.isNaN(t)) return "unknown";
  const diffSec = Math.floor((Date.now() - t) / 1000);
  if (diffSec < 60) return `${diffSec}s ago`;
  const diffMin = Math.floor(diffSec / 60);
  if (diffMin < 60) return `${diffMin}m ago`;
  const diffHour = Math.floor(diffMin / 60);
  if (diffHour < 24) return `${diffHour}h ago`;
  const diffDay = Math.floor(diffHour / 24);
  if (diffDay < 30) return `${diffDay}d ago`;
  const diffMon = Math.floor(diffDay / 30);
  if (diffMon < 12) return `${diffMon}mo ago`;
  const diffYear = Math.floor(diffMon / 12);
  return `${diffYear}y ago`;
}

export function toDisplayTime(value?: string): string {
  if (!value) return "unknown";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return value;
  return d.toLocaleString("zh-CN", { hour12: false });
}

export function isUnknown(value?: string): boolean {
  const v = String(value || "").trim().toLowerCase();
  return v === "" || v === "-" || v === "unknown" || v === "__unknown__";
}
