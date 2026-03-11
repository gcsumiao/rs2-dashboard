import {
  CarFront,
  Database,
  Gauge,
  type LucideIcon,
  MapPinned,
  ShoppingCart,
  UserRound,
  Wrench,
} from "lucide-react";

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
    name: "Overview",
    href: "/?tab=overview",
  },
  {
    icon: UserRound,
    name: "Users",
    href: "/?tab=users",
  },
  {
    icon: Wrench,
    name: "Tools",
    href: "/?tab=tools",
  },
  {
    icon: CarFront,
    name: "VIN",
    href: "/?tab=vin",
  },
  {
    icon: MapPinned,
    name: "Geo",
    href: "/?tab=geo",
  },
  {
    icon: ShoppingCart,
    name: "LTL",
    href: "/?tab=ltl",
  },
  {
    icon: Database,
    name: "Data Gaps",
    href: "/?tab=gaps",
  },
];
