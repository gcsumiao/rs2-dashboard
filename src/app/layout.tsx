import type { Metadata } from "next";
import { Gabarito } from "next/font/google";
import { Suspense } from "react";
import { SideNav } from "@/components/nav";
import { siteConfig } from "@/config/site";
import { cn } from "@/lib/utils";
import "@/style/globals.css";
import { Providers } from "./providers";

const gabarito = Gabarito({ subsets: ["latin"], variable: "--font-gabarito" });

export const metadata: Metadata = {
  title: siteConfig.title,
  description: siteConfig.description,
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" suppressHydrationWarning>
      <body className={cn("bg-background font-sans", gabarito.variable)}>
        <Providers>
          <div className="flex min-h-[100dvh]">
            <Suspense fallback={<div className="w-44 shrink-0 border-r border-border bg-slate-100 dark:bg-slate-900" />}>
              <SideNav />
            </Suspense>
            <div className="flex-grow overflow-auto">{children}</div>
          </div>
        </Providers>
      </body>
    </html>
  );
}
