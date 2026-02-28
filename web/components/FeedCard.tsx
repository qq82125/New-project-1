import Link from "next/link";
import { FeedItem } from "@/lib/types";

type Props = {
  item: FeedItem;
};

export default function FeedCard({ item }: Props) {
  return (
    <article className="rounded-lg border border-line bg-panel p-4">
      <h3 className="mb-2 text-base font-semibold leading-6">
        <Link href={`/feed/${item.id}`} className="hover:underline">
          {item.title || "(untitled)"}
        </Link>
      </h3>
      <p className="mb-3 line-clamp-3 text-sm text-muted">{item.summary || "暂无摘要"}</p>
      <div className="flex flex-wrap gap-x-3 gap-y-1 text-xs text-muted">
        <span>source_id: {item.source_id || "-"}</span>
        <span>group: {item.group || "-"}</span>
        <span>region: {item.region || "-"}</span>
        <span>event_type: {item.event_type || "-"}</span>
        <span>trust: {item.trust_tier || "-"}</span>
        <span>published: {item.published_at || "-"}</span>
      </div>
    </article>
  );
}
