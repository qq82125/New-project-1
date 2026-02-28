import FeedCard from "@/components/FeedCard";
import { FeedItem } from "@/lib/types";

type Props = {
  items: FeedItem[];
};

export default function FeedList({ items }: Props) {
  if (!items.length) {
    return <div className="rounded-lg border border-line bg-panel p-6 text-sm text-muted">暂无数据</div>;
  }
  return (
    <div className="space-y-3">
      {items.map((it) => (
        <FeedCard key={it.id} item={it} />
      ))}
    </div>
  );
}
