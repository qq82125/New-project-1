const PIN_KEY = "feed2:pins";

export function loadPins(): Set<string> {
  if (typeof window === "undefined") return new Set<string>();
  try {
    const raw = window.localStorage.getItem(PIN_KEY);
    if (!raw) return new Set<string>();
    const arr = JSON.parse(raw);
    if (!Array.isArray(arr)) return new Set<string>();
    return new Set(arr.map((x) => String(x)));
  } catch {
    return new Set<string>();
  }
}

export function savePins(pins: Set<string>): void {
  if (typeof window === "undefined") return;
  window.localStorage.setItem(PIN_KEY, JSON.stringify(Array.from(pins)));
}

export function togglePin(pins: Set<string>, key: string): Set<string> {
  const out = new Set(pins);
  if (out.has(key)) out.delete(key);
  else out.add(key);
  savePins(out);
  return out;
}
