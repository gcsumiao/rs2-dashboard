import { Suspense } from "react";
import Container from "@/components/container";
import { DashboardClient } from "@/components/dashboard/dashboard-client";

function DashboardLoadingFallback() {
  return <div className="py-8 text-sm text-muted-foreground">Loading dashboard...</div>;
}

export default function Home() {
  return (
    <Container className="py-4">
      <Suspense fallback={<DashboardLoadingFallback />}>
        <DashboardClient />
      </Suspense>
    </Container>
  );
}
