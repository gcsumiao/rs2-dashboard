import { Gauge, type LucideIcon } from "lucide-react";

export type SiteConfig = typeof siteConfig;
export type Navigation = {
  icon: LucideIcon;
  name: string;
  href: string;
};

export const siteConfig = {
  title: "RS2 Inspection Dashboard",
  description: "Interactive RS2 inspection analytics dashboard in UTC",
};

export const navigations: Navigation[] = [
  {
    icon: Gauge,
    name: "Dashboard",
    href: "/",
  },
];
