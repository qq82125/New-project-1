import { Suspense } from "react";
import FeedPageClient from "./FeedPageClient";

export const dynamic = "force-dynamic";

export default function FeedPage() {
  return (
    <Suspense fallback={<div className="text-sm text-muted">Loading feed...</div>}>
      <FeedPageClient />
    </Suspense>
  );
}
