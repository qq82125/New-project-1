export type FeedItem = {
  id: string;
  title: string;
  title_zh?: string;
  url?: string;
  primary_url?: string;
  source_id?: string;
  source?: string;
  group?: string;
  region?: string;
  lane?: string;
  event_type?: string;
  trust_tier?: "A" | "B" | "C" | string;
  published_at?: string;
  summary?: string;
  summary_zh?: string;
  snippet?: string;
  score?: number;
  track?: string;
  sources_count?: number;
  priority?: number;
};

export type FeedResponse = {
  ok?: boolean;
  items: FeedItem[];
  next_cursor?: string | null;
};

export type FeedSummaryResponse = {
  ok?: boolean;
  mode?: "story" | "item" | string;
  total: number;
  by_group: Record<string, number>;
  unknown_region_rate: number;
  unknown_event_type_rate: number;
};

export type FeedDetailResponse = {
  ok?: boolean;
  story?: FeedItem;
  evidence?: Array<{
    raw_item_id: string;
    title: string;
    url: string;
    source_id: string;
    published_at?: string;
    trust_tier?: string;
    priority?: number;
    is_primary?: boolean;
    rank?: number;
    group?: string;
    region?: string;
  }>;
  item?: FeedItem;
};

export type FeedItemDetailResponse = {
  ok?: boolean;
  item: FeedItem & {
    raw_payload?: Record<string, unknown>;
  };
  story?: {
    story_id?: string;
    story_title?: string;
  } | null;
};
