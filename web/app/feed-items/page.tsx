import { Suspense } from "react";
import FeedItemsPageClient from "./FeedItemsPageClient";

export const dynamic = "force-dynamic";

export default function FeedItemsPage() {
  return (
    <Suspense fallback={<div className="text-sm text-muted">Loading items...</div>}>
      <FeedItemsPageClient />
    </Suspense>
  );
}

