"use client";

type Props = {
  total: number;
  byGroup: Record<string, number>;
  unknownRegionRate: number;
  unknownEventTypeRate: number;
  scopeLabel?: string;
  globalTotal?: number;
  globalByGroup?: Record<string, number>;
  globalUnknownRegionRate?: number;
  globalUnknownEventTypeRate?: number;
};

function pct(v: number): string {
  return `${Math.round(v * 100)}%`;
}

export default function SummaryBar({
  total,
  byGroup,
  unknownRegionRate,
  unknownEventTypeRate,
  scopeLabel,
  globalTotal,
  globalByGroup,
  globalUnknownRegionRate,
  globalUnknownEventTypeRate,
}: Props) {
  return (
    <div className="rounded-lg border border-line bg-panel px-4 py-3 text-xs text-muted">
      <div className="flex flex-wrap gap-4">
        <span>View Items: {total}</span>
        <span>Regulatory: {byGroup.regulatory || 0}</span>
        <span>Media: {byGroup.media || 0}</span>
        <span>Evidence: {byGroup.evidence || 0}</span>
        <span>Company: {byGroup.company || 0}</span>
        <span>Procurement: {byGroup.procurement || 0}</span>
      </div>
      <div className="mt-1 flex flex-wrap gap-4">
        <span>View unknown region: {pct(unknownRegionRate)}</span>
        <span>View unknown event_type: {pct(unknownEventTypeRate)}</span>
      </div>
      {scopeLabel ? <div className="mt-1 text-[11px] text-zinc-400">{scopeLabel}</div> : null}
      {typeof globalTotal === "number" ? (
        <>
          <div className="mt-2 border-t border-line pt-2 text-[11px] text-zinc-400">Postgres 全库分布</div>
          <div className="mt-1 flex flex-wrap gap-4">
            <span>All Items: {globalTotal}</span>
            <span>Regulatory: {globalByGroup?.regulatory || 0}</span>
            <span>Media: {globalByGroup?.media || 0}</span>
            <span>Evidence: {globalByGroup?.evidence || 0}</span>
            <span>Company: {globalByGroup?.company || 0}</span>
            <span>Procurement: {globalByGroup?.procurement || 0}</span>
          </div>
          <div className="mt-1 flex flex-wrap gap-4">
            <span>All unknown region: {pct(globalUnknownRegionRate || 0)}</span>
            <span>All unknown event_type: {pct(globalUnknownEventTypeRate || 0)}</span>
          </div>
        </>
      ) : null}
      {globalTotal === undefined ? (
        <div className="mt-2 text-[11px] text-zinc-500">全库统计加载中...</div>
      ) : null}
      <div className="mt-1 text-[11px] text-zinc-500">
        口径说明：View=当前列表（受筛选/分页影响）；All=Postgres 全库聚合。
      </div>
    </div>
  );
}
