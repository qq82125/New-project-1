"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { useParams } from "next/navigation";
import { buildUrl, fetchJson } from "@/lib/api";
import { FeedDetailResponse } from "@/lib/types";

export default function FeedDetailPage() {
  const params = useParams<{ id: string }>();
  const id = params?.id || "";
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [data, setData] = useState<FeedDetailResponse | null>(null);

  useEffect(() => {
    if (!id) return;
    setLoading(true);
    setError("");
    fetchJson<FeedDetailResponse>(buildUrl(`/api/feed/${id}`))
      .then((r) => setData(r))
      .catch((e) => setError((e as Error).message))
      .finally(() => setLoading(false));
  }, [id]);

  if (loading) return <div className="text-sm text-muted">加载中...</div>;
  if (error) return <div className="text-sm text-red-300">{error}</div>;
  if (!data) return <div className="text-sm text-muted">未找到条目</div>;

  return (
    <div className="space-y-4">
      <div className="text-sm text-muted">
        <Link href="/feed" className="hover:underline">
          ← 返回 Feed
        </Link>
      </div>
      <article className="space-y-3 rounded-lg border border-line bg-panel p-5">
        <h1 className="text-xl font-semibold">{data.story?.title || "-"}</h1>
        <div className="flex flex-wrap gap-3 text-xs text-muted">
          <span>group: {data.story?.group || "-"}</span>
          <span>region: {data.story?.region || "-"}</span>
          <span>trust_tier: {data.story?.trust_tier || "-"}</span>
          <span>published_at: {data.story?.published_at || "-"}</span>
        </div>
        <a href={data.story?.primary_url || ""} target="_blank" rel="noreferrer" className="text-sm underline">
          打开原文
        </a>
      </article>

      <section className="space-y-2 rounded-lg border border-line bg-panel p-5">
        <h2 className="text-sm font-semibold">证据来源</h2>
        {(data.evidence || []).length === 0 ? (
          <div className="text-sm text-muted">无</div>
        ) : (
          <ul className="space-y-2">
            {(data.evidence || []).map((x) => (
              <li key={x.raw_item_id} className="text-sm">
                <a href={x.url} target="_blank" rel="noreferrer" className="underline">
                  {x.title}
                </a>{" "}
                <span className="text-muted">({x.source_id} / {x.published_at || "-"})</span>
              </li>
            ))}
          </ul>
        )}
      </section>
    </div>
  );
}
