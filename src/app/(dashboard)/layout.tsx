import { TopNav } from "@/components/nav";

export default function DashboardLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <>
      <TopNav title="RS2 Inspection Dashboard" />
      <main>{children}</main>
    </>
  );
}
