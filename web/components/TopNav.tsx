"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

function isActive(pathname: string, key: "feed" | "items" | "dashboard" | "admin"): boolean {
  if (key === "feed") return pathname === "/feed" || pathname.startsWith("/feed/");
  if (key === "items") return pathname === "/feed-items" || pathname === "/items";
  if (key === "dashboard") return pathname === "/dashboard";
  if (key === "admin") return pathname === "/admin-shell" || pathname === "/admin" || pathname.startsWith("/admin/");
  return false;
}

function navClass(active: boolean): string {
  return active ? "font-semibold text-text" : "text-muted hover:text-text";
}

export default function TopNav() {
  const pathname = usePathname() || "/";
  return (
    <nav className="mx-auto flex max-w-7xl items-center gap-8 px-6 py-4 text-sm">
      <Link href="/feed" className="text-base font-semibold tracking-tight text-text">
        OmniGlean
      </Link>
      <Link href="/feed" className={navClass(isActive(pathname, "feed"))}>
        Feed
      </Link>
      <Link href="/feed-items" className={navClass(isActive(pathname, "items"))}>
        Items
      </Link>
      <Link href="/dashboard" className={navClass(isActive(pathname, "dashboard"))}>
        Dashboard
      </Link>
      <Link href="/admin-shell" className={navClass(isActive(pathname, "admin"))}>
        Admin
      </Link>
    </nav>
  );
}

