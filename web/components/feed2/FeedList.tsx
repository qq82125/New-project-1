"use client";

import FeedCard from "@/components/feed2/FeedCard";
import { FeedItem } from "@/lib/types";

type Props = {
  items: FeedItem[];
  compact: boolean;
  isPinned: (item: FeedItem) => boolean;
  onOpen: (item: FeedItem) => void;
  onTogglePin: (item: FeedItem) => void;
};

export default function FeedList({ items, compact, isPinned, onOpen, onTogglePin }: Props) {
  if (!items.length) {
    return <div className="rounded-lg border border-line bg-panel p-6 text-sm text-muted">No items</div>;
  }
  return (
    <div className="space-y-3">
      {items.map((item) => (
        <FeedCard
          key={`${item.id}-${item.url}`}
          item={item}
          compact={compact}
          pinned={isPinned(item)}
          onOpen={onOpen}
          onTogglePin={onTogglePin}
        />
      ))}
    </div>
  );
}
