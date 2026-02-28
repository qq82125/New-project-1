const API_BASE = (process.env.NEXT_PUBLIC_API_BASE_URL || "/api").replace(/\/+$/, "");

export function buildUrl(path: string, params?: Record<string, string | number | undefined | null>) {
  const rawPath = path.startsWith("/") ? path : `/${path}`;
  const fullPath = rawPath.startsWith("/api/") || rawPath === "/api" ? rawPath : `${API_BASE}${rawPath}`;
  const query = new URLSearchParams();
  if (params) {
    for (const [k, v] of Object.entries(params)) {
      if (v === undefined || v === null || `${v}`.trim() === "") continue;
      query.set(k, `${v}`);
    }
  }
  const qs = query.toString();
  return qs ? `${fullPath}?${qs}` : fullPath;
}

export async function fetchJson<T>(url: string, signal?: AbortSignal): Promise<T> {
  const resp = await fetch(url, {
    method: "GET",
    headers: { Accept: "application/json" },
    signal,
    cache: "no-store"
  });
  if (!resp.ok) {
    const txt = await resp.text();
    throw new Error(`HTTP ${resp.status}: ${txt}`);
  }
  return (await resp.json()) as T;
}
