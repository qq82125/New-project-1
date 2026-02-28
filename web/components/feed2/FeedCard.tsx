"use client";

import { useMemo, useState } from "react";
import { FeedItem } from "@/lib/types";
import { isUnknown, toDisplayTime, toRelativeTime } from "@/lib/time";

type Props = {
  item: FeedItem;
  compact: boolean;
  pinned: boolean;
  onOpen: (item: FeedItem) => void;
  onTogglePin: (item: FeedItem) => void;
};

function Badge({ label, tone = "default" }: { label: string; tone?: "default" | "unknown" | "trust" }) {
  const cls =
    tone === "unknown"
      ? "border-slate-500/60 bg-slate-700/30 text-slate-300"
      : tone === "trust"
        ? "border-amber-500/50 bg-amber-500/10 text-amber-200"
        : "border-blue-500/50 bg-blue-500/10 text-blue-200";
  return <span className={`rounded-full border px-2 py-0.5 text-[11px] ${cls}`}>{label}</span>;
}

function UnknownHintBadge({
  label,
  hint,
  active,
  onToggle,
}: {
  label: string;
  hint: string;
  active: boolean;
  onToggle: () => void;
}) {
  return (
    <button
      type="button"
      className={`rounded-full border px-2 py-0.5 text-[11px] ${
        active ? "border-slate-300 bg-slate-700/50 text-slate-100" : "border-slate-500/60 bg-slate-700/30 text-slate-300"
      }`}
      title={hint}
      onClick={onToggle}
    >
      {label}
    </button>
  );
}

function textOrUnknown(v?: string): { text: string; unknown: boolean } {
  return isUnknown(v) ? { text: "Unknown", unknown: true } : { text: String(v), unknown: false };
}

function canonicalGroup(v?: string): string {
  const g = String(v || "").toLowerCase();
  if (g.includes("procurement")) return "procurement";
  if (g.includes("regulatory")) return "regulatory";
  if (g.includes("evidence")) return "evidence";
  if (g.includes("company")) return "company";
  if (g.includes("media")) return "media";
  return g || "unknown";
}

export default function FeedCard({ item, compact, pinned, onOpen, onTogglePin }: Props) {
  const [activeHint, setActiveHint] = useState<string>("");
  const sourceIdText = String(item.source_id ?? "").trim();
  const sourceIdUnknown = isUnknown(sourceIdText);
  const titleText = String(item.title_zh ?? item.title ?? "").trim() || "(untitled)";
  const summaryText = String(item.summary_zh ?? item.summary ?? item.snippet ?? "").trim();
  const group = textOrUnknown(item.group);
  const region = textOrUnknown(item.region);
  const et = textOrUnknown(item.event_type);
  const trust = textOrUnknown(item.trust_tier);
  const rel = toRelativeTime(item.published_at);
  const abs = toDisplayTime(item.published_at);
  const g = canonicalGroup(item.group);
  const unknownHints = useMemo(() => {
    const hints: Record<string, string> = {};
    if (group.unknown) {
      hints.group = "source_group 未识别（或历史值未归一）。建议检查 source_registry 中该 source_id 的 source_group。";
    }
    if (region.unknown) {
      hints.region = item.source_id
        ? "region 映射未命中。建议补 rules/mappings/region_map.v1.yaml 或 source_registry.region。"
        : "缺少 source_id，无法套用来源映射计算 region。";
    }
    if (et.unknown) {
      hints.event_type =
        g === "media" || g === "unknown"
          ? "媒体类条目未命中 event_type 关键词规则。可补关键词或 source/domain fallback。"
          : "group fallback 未生效。建议检查 source_group 归一化与回填。";
    }
    if (sourceIdUnknown) {
      hints.source_id = "该 story 未带 primary source_id（或原始条目缺 source_id）。建议检查聚合回填。";
    }
    if (!summaryText) {
      hints.summary = "源未提供摘要，且正文抓取未产出 snippet。可为该源开启稳定摘要/正文提取。";
    }
    return hints;
  }, [et.unknown, g, group.unknown, region.unknown, sourceIdUnknown, summaryText]);

  return (
    <article className="rounded-lg border border-line bg-panel px-4 py-3">
      <div className="mb-2 flex flex-wrap items-center gap-2">
        {group.unknown ? (
          <UnknownHintBadge
            label={group.text}
            hint={unknownHints.group || "Unknown group"}
            active={activeHint === "group"}
            onToggle={() => setActiveHint((v) => (v === "group" ? "" : "group"))}
          />
        ) : (
          <Badge label={group.text} tone="default" />
        )}
        {region.unknown ? (
          <UnknownHintBadge
            label={region.text}
            hint={unknownHints.region || "Unknown region"}
            active={activeHint === "region"}
            onToggle={() => setActiveHint((v) => (v === "region" ? "" : "region"))}
          />
        ) : (
          <Badge label={region.text} tone="default" />
        )}
        {et.unknown ? (
          <UnknownHintBadge
            label={et.text}
            hint={unknownHints.event_type || "Unknown event_type"}
            active={activeHint === "event_type"}
            onToggle={() => setActiveHint((v) => (v === "event_type" ? "" : "event_type"))}
          />
        ) : (
          <Badge label={et.text} tone="default" />
        )}
        <Badge label={trust.text} tone={trust.unknown ? "unknown" : "trust"} />
        {typeof item.sources_count === "number" ? <Badge label={`sources:${item.sources_count}`} tone="default" /> : null}
        {pinned ? <Badge label="Pinned" tone="trust" /> : null}
        <span className="ml-auto text-xs text-muted" title={abs}>
          {rel}
        </span>
      </div>
      {activeHint && unknownHints[activeHint] ? (
        <div className="mb-2 rounded border border-slate-500/40 bg-slate-800/50 px-2 py-1 text-xs text-slate-200">
          {unknownHints[activeHint]}
        </div>
      ) : null}
      <button className="mb-1 text-left text-base font-semibold leading-6 hover:underline" onClick={() => onOpen(item)}>
        {titleText}
      </button>
      {!compact ? (
        <>
          <p className="mb-2 text-sm text-muted" title={unknownHints.summary || ""}>
            {summaryText || "暂无摘要"}
          </p>
          <div className="flex items-center justify-between text-xs text-muted">
            <span>
              source_id:{" "}
              {sourceIdUnknown ? (
                <button
                  type="button"
                  className="underline decoration-dotted underline-offset-2"
                  title={unknownHints.source_id || "Unknown source_id"}
                  onClick={() => setActiveHint((v) => (v === "source_id" ? "" : "source_id"))}
                >
                  Unknown
                </button>
              ) : (
                sourceIdText
              )}
            </span>
            <div className="flex gap-3">
              <button className="underline" onClick={() => onOpen(item)}>
                Details
              </button>
              <button className="underline" onClick={() => onTogglePin(item)}>
                {pinned ? "Unpin" : "Pin"}
              </button>
            </div>
          </div>
        </>
      ) : (
        <div className="flex items-center justify-between text-xs text-muted">
          <span>{sourceIdText || "Unknown"}</span>
          <button className="underline" onClick={() => onTogglePin(item)}>
            {pinned ? "Unpin" : "Pin"}
          </button>
        </div>
      )}
    </article>
  );
}
