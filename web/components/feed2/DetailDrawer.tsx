"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { buildUrl, fetchJson } from "@/lib/api";
import { FeedItem } from "@/lib/types";
import { toDisplayTime, toRelativeTime } from "@/lib/time";
import type { FeedDetailResponse, FeedItemDetailResponse } from "@/lib/types";

type Props = {
  mode: "story" | "item";
  open: boolean;
  item: FeedItem | null;
  pinned: boolean;
  onClose: () => void;
  onTogglePin: (item: FeedItem) => void;
};

export default function DetailDrawer({ mode, open, item, pinned, onClose, onTogglePin }: Props) {
  const [detail, setDetail] = useState<FeedDetailResponse | FeedItemDetailResponse | null>(null);
  const [loadingDetail, setLoadingDetail] = useState(false);

  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [open, onClose]);

  useEffect(() => {
    if (!open || !item?.id) return;
    const ac = new AbortController();
    const endpoint = mode === "story" ? `/api/feed/${item.id}` : `/api/feed-items/${item.id}`;
    setLoadingDetail(true);
    void fetchJson<FeedDetailResponse | FeedItemDetailResponse>(buildUrl(endpoint), ac.signal)
      .then((res) => setDetail(res))
      .catch(() => setDetail(null))
      .finally(() => setLoadingDetail(false));
    return () => ac.abort();
  }, [open, item?.id, mode]);

  const copyLink = async () => {
    const u = item?.url || item?.primary_url;
    if (!u) return;
    try {
      await navigator.clipboard.writeText(u);
    } catch {
      // noop
    }
  };

  return (
    <>
      <div
        className={`fixed inset-0 z-30 bg-black/45 transition-opacity ${open ? "opacity-100" : "pointer-events-none opacity-0"}`}
        onClick={onClose}
      />
      <aside
        className={`fixed right-0 top-0 z-40 h-full w-[420px] max-w-[92vw] border-l border-line bg-bg p-5 transition-transform ${
          open ? "translate-x-0" : "translate-x-full"
        }`}
      >
        {!item ? null : (
          <div className="flex h-full flex-col gap-4 overflow-auto">
            <div className="flex items-start justify-between gap-3">
              <h2 className="text-lg font-semibold">{item.title_zh || item.title || "(untitled)"}</h2>
              <button className="rounded border border-line px-2 py-1 text-xs text-muted" onClick={onClose}>
                Close
              </button>
            </div>

            <div className="text-xs text-muted">
              <span title={toDisplayTime(item.published_at)}>{toRelativeTime(item.published_at)}</span>
              <span className="ml-2">| {item.published_at || "unknown"}</span>
            </div>

            <div className="space-y-1 rounded border border-line bg-panel p-3 text-sm">
              <div>source_id: {item.source_id || "Unknown"}</div>
              <div>group: {item.group || "Unknown"}</div>
              <div>region: {item.region || "Unknown"}</div>
              <div>event_type: {item.event_type || "Unknown"}</div>
              <div>trust_tier: {item.trust_tier || "Unknown"}</div>
              {mode === "story" ? <div>sources_count: {item.sources_count ?? "Unknown"}</div> : null}
            </div>

            <div className="rounded border border-line bg-panel p-3 text-sm">{item.summary_zh || item.summary || item.snippet || "No summary"}</div>

            <div className="flex flex-wrap gap-2">
              <a className="rounded border border-line px-3 py-2 text-sm" href={item.url || item.primary_url} target="_blank" rel="noreferrer">
                Open original
              </a>
              <button className="rounded border border-line px-3 py-2 text-sm" onClick={copyLink}>
                Copy link
              </button>
              <button className="rounded border border-line px-3 py-2 text-sm" onClick={() => onTogglePin(item)}>
                {pinned ? "Unpin" : "Pin"}
              </button>
            </div>

            {loadingDetail ? <div className="text-xs text-muted">Loading detail...</div> : null}
            {!loadingDetail && mode === "story" && detail && "evidence" in detail && Array.isArray(detail.evidence) ? (
              <div className="rounded border border-line bg-panel p-3">
                <div className="mb-2 text-sm font-medium">Evidence</div>
                <div className="space-y-2 text-sm">
                  {detail.evidence.map((ev) => (
                    <div key={`${ev.raw_item_id}-${ev.rank}`} className="rounded border border-line/80 p-2">
                      <div className="font-medium">{ev.title}</div>
                      <div className="text-xs text-muted">
                        {ev.source_id} | {ev.trust_tier || "Unknown"} | {toRelativeTime(ev.published_at)}
                      </div>
                      <div className="mt-1 flex gap-3 text-xs">
                        <a className="underline" href={ev.url} target="_blank" rel="noreferrer">
                          Open
                        </a>
                        <Link className="underline" href={`/feed-items?q=${encodeURIComponent(ev.title || "")}&source_id=${encodeURIComponent(ev.source_id || "")}`}>
                          View in Items
                        </Link>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            ) : null}

            {!loadingDetail && mode === "item" && detail ? (() => {
              const mapped = (detail as FeedItemDetailResponse).story;
              if (!mapped) return null;
              return (
                <div className="rounded border border-line bg-panel p-3 text-sm">
                  <div className="mb-1 font-medium">Mapped story</div>
                  <Link className="underline" href={`/feed?q=${encodeURIComponent(mapped.story_title || "")}`}>
                    Go to Story
                  </Link>
                </div>
              );
            })() : null}

            <details className="rounded border border-line bg-panel p-3 text-xs text-muted">
              <summary className="cursor-pointer">Raw meta</summary>
              <pre className="mt-2 whitespace-pre-wrap">{JSON.stringify(detail || item, null, 2)}</pre>
            </details>
          </div>
        )}
      </aside>
    </>
  );
}
