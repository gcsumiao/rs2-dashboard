"use client";

import Link from "next/link";
import { usePathname, useSearchParams } from "next/navigation";
import { navigations } from "@/config/site";
import { cn } from "@/lib/utils";

export default function Navigation() {
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const currentTab = searchParams.get("tab") ?? "overview";
  return (
    <nav className="flex flex-grow flex-col gap-y-1 p-2">
      {navigations.map((navigation) => {
        const Icon = navigation.icon;
        const href = navigation.href ?? "/";
        const parsed = new URL(href, "http://localhost");
        const navTab = parsed.searchParams.get("tab");
        let linkHref = href;
        if (parsed.pathname === "/" && navTab) {
          const params = new URLSearchParams(searchParams.toString());
          params.set("tab", navTab);
          linkHref = `${parsed.pathname}?${params.toString()}`;
        }
        const isActive =
          pathname === parsed.pathname &&
          (navTab ? navTab === currentTab : true);
        return (
          <Link
            key={navigation.name}
            href={linkHref}
            className={cn(
              "flex items-center rounded-md px-2 py-1.5 hover:bg-slate-200 dark:hover:bg-slate-800",
              isActive
                ? "bg-slate-200 dark:bg-slate-800"
                : "bg-transparent",
            )}
          >
            <Icon
              size={16}
              className="mr-2 text-slate-800 dark:text-slate-200"
            />
            <span className="text-sm text-slate-700 dark:text-slate-300">
              {navigation.name}
            </span>
          </Link>
        );
      })}
    </nav>
  );
}
