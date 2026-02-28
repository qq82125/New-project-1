"use client";

import Link from "next/link";
import { useEffect, useState } from "react";

type ProbeState = "checking" | "ok" | "unauthorized" | "error";

export default function AdminShellPage() {
  const [state, setState] = useState<ProbeState>("checking");

  useEffect(() => {
    let cancelled = false;
    void fetch("/admin", { method: "GET", cache: "no-store" })
      .then((resp) => {
        if (cancelled) return;
        if (resp.status === 401) {
          setState("unauthorized");
          return;
        }
        if (resp.ok) {
          setState("ok");
          return;
        }
        setState("error");
      })
      .catch(() => {
        if (!cancelled) setState("error");
      });
    return () => {
      cancelled = true;
    };
  }, []);

  return (
    <section className="rounded-lg border border-line bg-panel p-0">
      <div className="border-b border-line px-4 py-3 text-sm text-muted">Admin Shell（统一入口）</div>
      {state === "checking" ? (
        <div className="p-4 text-sm text-muted">正在检查 Admin 可访问性...</div>
      ) : null}
      {state === "unauthorized" ? (
        <div className="space-y-3 p-4 text-sm">
          <div className="rounded border border-yellow-500/40 bg-yellow-950/20 p-3 text-yellow-200">
            当前未登录 Admin（401）。请先完成登录，再返回此页面。
          </div>
          <div className="flex items-center gap-2">
            <Link href="/admin" className="rounded border border-line px-3 py-2 text-sm hover:bg-bg">
              打开 /admin 登录
            </Link>
            <button
              className="rounded border border-line px-3 py-2 text-sm hover:bg-bg"
              onClick={() => window.location.reload()}
            >
              我已登录，刷新
            </button>
          </div>
        </div>
      ) : null}
      {state === "error" ? (
        <div className="space-y-3 p-4 text-sm">
          <div className="rounded border border-red-500/40 bg-red-950/20 p-3 text-red-200">
            Admin 暂不可达，请检查 `admin-api` 容器状态和端口映射。
          </div>
          <div className="flex items-center gap-2">
            <Link href="/admin" className="rounded border border-line px-3 py-2 text-sm hover:bg-bg">
              打开 /admin
            </Link>
            <button
              className="rounded border border-line px-3 py-2 text-sm hover:bg-bg"
              onClick={() => window.location.reload()}
            >
              重试
            </button>
          </div>
        </div>
      ) : null}
      {state === "ok" ? (
        <iframe
          src="/admin"
          title="Admin"
          className="h-[calc(100vh-170px)] w-full rounded-b-lg bg-white"
        />
      ) : null}
    </section>
  );
}
