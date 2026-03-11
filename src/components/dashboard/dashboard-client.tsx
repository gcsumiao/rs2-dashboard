"use client"

import { startTransition, useDeferredValue, useEffect, useRef, useState } from "react"
import dynamic from "next/dynamic"
import { usePathname, useRouter, useSearchParams } from "next/navigation"
import { Activity, CalendarDays, Car, MapPinned, TrendingUp, Users } from "lucide-react"
import {
  Area,
  AreaChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts"
import { MetricCard } from "@/components/dashboard/metric-card"
import { PageHeader } from "@/components/dashboard/page-header"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { Badge } from "@/components/ui/badge"

const UsZipHeatmap = dynamic(
  () => import("@/components/dashboard/us-zip-heatmap").then((module) => module.UsZipHeatmap),
  {
    ssr: false,
    loading: () => <div className="h-[420px] rounded-lg border border-border bg-muted/40" />,
  }
)

type DashboardPayload = {
  meta: {
    start: string
    end: string
    topN: number
    available_start: string
    available_end: string
    prior_equal_range: { start: string; end: string; days: number }
    has_prior_data: boolean
  }
  overview: {
    scans: number
    unique_users: number
    unique_vins: number
    prior_scans: number | null
    scan_lift_pct_vs_prior: number | null
  }
  dailyTrend: Array<{ date_pt: string; scans: number; unique_users: number; unique_vins: number }>
  topUsers: Array<{ user_id: string; user_email: string; same_day_reports: number; inspections: number }>
  topTools: Array<{ tool_name: string; inspections: number; abs_parts: number; srs_parts: number; cel_parts: number }>
  topVins: Array<{
    vin: string
    vehicle_year: string
    vehicle_make: string
    vehicle_model: string
    vehicle_engine: string
    inspections: number
  }>
  topAccountVins: Array<{
    account_id: string
    user_email: string
    vin: string
    vehicle_year: string
    vehicle_make: string
    vehicle_model: string
    vehicle_engine: string
    inspections: number
  }>
  topZipPostal: Array<{ zip_postal: string; city: string; state: string; zip5: string; inspections: number }>
  ltlRetailers: Array<{ buy_from: string; inspections: number; total_buy_clicks: number }>
  ltlClickedAccounts: Array<{ account_id: string; user_email: string; inspections: number; total_buy_clicks: number }>
  zipHeatPoints: Array<{
    zip_postal: string
    zip5: string
    city: string
    state: string
    lat: number
    lng: number
    scans: number
    percentile: number
  }>
  zipHeatMeta: {
    points_with_centroid: number
    points_total_zip5: number
    truncated: boolean
    missing_centroid_lookup: boolean
  }
  fourWeekLift: {
    status: string
    windows?: {
      recent: { start: string; end: string }
      prior: { start: string; end: string }
    }
    summary?: {
      recent_scans: number
      prior_scans: number
      scan_delta: number
      scan_lift_pct: number | null
      recent_unique_users: number
      prior_unique_users: number
      recent_unique_vins: number
      prior_unique_vins: number
    }
    drivers?: {
      tools: Array<{ key: string; delta: number; recent: number; prior: number }>
      buy_from: Array<{ key: string; delta: number; recent: number; prior: number }>
      zip5: Array<{ key: string; delta: number; recent: number; prior: number }>
    }
  }
  dataGaps: Array<{ question: string; status: "blocked" | "skipped"; reason: string; required_source: string }>
  quality: {
    generated_at_utc: string
    mapping_fallback_usb937: string | null
    ambiguous_mapping_keys: number
    top_unmapped_usb: Array<{ usb_product_id: string; rows: number }>
  }
}

const DEFAULT_START = "2026-02-01"
const DEFAULT_END = "2026-02-28"
const DEFAULT_TAB = "overview"
const ALLOWED_TABS = new Set(["overview", "users", "tools", "vin", "geo", "ltl", "gaps"])

function numberLabel(value: number): string {
  return value.toLocaleString()
}

function percentLabel(value: number | null): string {
  if (value === null || Number.isNaN(value)) return "n/a"
  const rounded = value.toFixed(1)
  return `${value >= 0 ? "+" : ""}${rounded}%`
}

function shortDate(datePt: string): string {
  return datePt.slice(5)
}

function TopTable({
  title,
  subtitle,
  headers,
  rows,
}: {
  title: string
  subtitle: string
  headers: string[]
  rows: Array<Array<string | number>>
}) {
  return (
    <Card className="bg-card border border-border">
      <CardHeader className="pb-2">
        <CardTitle className="text-base font-medium">{title}</CardTitle>
        <p className="text-xs text-muted-foreground">{subtitle}</p>
      </CardHeader>
      <CardContent>
        <div className="max-h-[460px] overflow-auto rounded-md border border-border">
          <table className="w-full text-sm">
            <thead className="sticky top-0 bg-card">
              <tr className="border-b border-border">
                {headers.map((header) => (
                  <th key={header} className="px-3 py-2 text-left text-xs font-medium text-muted-foreground">
                    {header}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.length === 0 ? (
                <tr>
                  <td className="px-3 py-4 text-xs text-muted-foreground" colSpan={headers.length}>
                    No rows in selected range.
                  </td>
                </tr>
              ) : (
                rows.map((row, index) => (
                  <tr key={`${title}-${index}`} className="border-b border-border last:border-0">
                    {row.map((cell, cellIndex) => (
                      <td key={`${title}-${index}-${cellIndex}`} className="px-3 py-2 text-xs">
                        {String(cell)}
                      </td>
                    ))}
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </CardContent>
    </Card>
  )
}

export function DashboardClient() {
  const router = useRouter()
  const pathname = usePathname()
  const searchParams = useSearchParams()

  const appliedStart = searchParams.get("start") ?? DEFAULT_START
  const appliedEnd = searchParams.get("end") ?? DEFAULT_END
  const appliedTopN = Math.max(1, Math.min(500, Number.parseInt(searchParams.get("topN") ?? "100", 10) || 100))
  const requestedTab = searchParams.get("tab") ?? DEFAULT_TAB
  const appliedTab = ALLOWED_TABS.has(requestedTab) ? requestedTab : DEFAULT_TAB

  const [start, setStart] = useState(appliedStart)
  const [end, setEnd] = useState(appliedEnd)
  const [topN, setTopN] = useState(appliedTopN)
  const [data, setData] = useState<DashboardPayload | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const deferredAppliedStart = useDeferredValue(appliedStart)
  const deferredAppliedEnd = useDeferredValue(appliedEnd)
  const deferredAppliedTopN = useDeferredValue(appliedTopN)
  const lastRequestKeyRef = useRef("")
  const inFlightRef = useRef<AbortController | null>(null)

  useEffect(() => {
    setStart((prev) => (prev === appliedStart ? prev : appliedStart))
    setEnd((prev) => (prev === appliedEnd ? prev : appliedEnd))
    setTopN((prev) => (prev === appliedTopN ? prev : appliedTopN))
  }, [appliedStart, appliedEnd, appliedTopN])

  const replaceParams = (updates: Partial<Record<"start" | "end" | "topN", string>>) => {
    const params = new URLSearchParams(searchParams)
    for (const [key, value] of Object.entries(updates)) {
      if (!value) {
        params.delete(key)
      } else {
        params.set(key, value)
      }
    }
    const nextQuery = params.toString()
    if (nextQuery === searchParams.toString()) {
      return
    }
    startTransition(() => {
      router.replace(`${pathname}?${nextQuery}`, { scroll: false })
    })
  }

  useEffect(() => {
    const requestKey = `${deferredAppliedStart}|${deferredAppliedEnd}|${deferredAppliedTopN}`
    if (lastRequestKeyRef.current === requestKey) {
      return
    }
    lastRequestKeyRef.current = requestKey

    inFlightRef.current?.abort()
    const controller = new AbortController()
    inFlightRef.current = controller

    const qs = new URLSearchParams({
      start: deferredAppliedStart,
      end: deferredAppliedEnd,
      topN: String(Math.max(1, Math.min(500, Math.floor(deferredAppliedTopN || 100)))),
      tz: "UTC",
    })

    const run = async () => {
      setLoading(true)
      setError(null)
      try {
        const response = await fetch(`/api/rs2/dashboard?${qs.toString()}`, {
          cache: "no-store",
          signal: controller.signal,
        })
        if (!response.ok) {
          const body = (await response.json().catch(() => ({}))) as { error?: string }
          throw new Error(body.error ?? `HTTP ${response.status}`)
        }
        const payload = (await response.json()) as DashboardPayload
        if (!controller.signal.aborted) {
          setData(payload)
        }
      } catch (err) {
        if (controller.signal.aborted) {
          return
        }
        setError(err instanceof Error ? err.message : "Failed to load RS2 dashboard payload")
        setData(null)
      } finally {
        if (!controller.signal.aborted) {
          setLoading(false)
        }
      }
    }
    void run()

    return () => {
      controller.abort()
    }
  }, [deferredAppliedStart, deferredAppliedEnd, deferredAppliedTopN])

  const onApplyCustomRange = () => {
    replaceParams({ start, end })
  }

  const onTopNApply = () => {
    replaceParams({ topN: String(Math.max(1, Math.min(500, Math.floor(topN || 100)))) })
  }

  const headerDescription = data
    ? `Range ${data.meta.start} to ${data.meta.end} (UTC) | top ${data.meta.topN} rows`
    : "Loading RS2 inspection metrics"

  return (
    <>
      <PageHeader title="RS2 Inspection Intelligence Dashboard" description={headerDescription}>
        <Badge variant="outline" className="text-xs bg-transparent">
          UTC
        </Badge>
      </PageHeader>

      <Card className="sticky top-3 z-10 mb-6 border border-border bg-card/95 backdrop-blur">
        <CardContent className="pt-4">
          <div className="grid grid-cols-2 gap-2 sm:grid-cols-4">
              <Input type="date" value={start} onChange={(event) => setStart(event.target.value)} />
              <Input type="date" value={end} onChange={(event) => setEnd(event.target.value)} />
              <Input
                type="number"
                min={1}
                max={500}
                value={topN}
                onChange={(event) => setTopN(Number.parseInt(event.target.value || "100", 10))}
              />
              <div className="flex gap-2">
                <Button variant="outline" className="w-full bg-transparent" onClick={onApplyCustomRange}>
                  Apply Range
                </Button>
                <Button variant="outline" className="w-full bg-transparent" onClick={onTopNApply}>
                  Apply TopN
                </Button>
              </div>
          </div>
        </CardContent>
      </Card>

      {loading && (
        <Card className="mb-6 border border-border bg-card">
          <CardContent className="py-10 text-sm text-muted-foreground">Loading dashboard data…</CardContent>
        </Card>
      )}

      {error && (
        <Card className="mb-6 border border-destructive/40 bg-card">
          <CardContent className="py-10 text-sm text-destructive">Failed to load API data: {error}</CardContent>
        </Card>
      )}

      {!loading && !error && data && appliedTab === "overview" && (
        <>
          <div className="mb-6 grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
            <MetricCard
              title="Inspections"
              value={numberLabel(data.overview.scans)}
              change={percentLabel(data.overview.scan_lift_pct_vs_prior)}
              changeSuffix={data.meta.has_prior_data ? "vs prior range" : ""}
              isPositiveOutcome={(data.overview.scan_lift_pct_vs_prior ?? 0) >= 0}
              icon={Activity}
            />
            <MetricCard
              title="Unique Users"
              value={numberLabel(data.overview.unique_users)}
              change={data.meta.has_prior_data ? `${numberLabel(data.overview.prior_scans ?? 0)} prior scans` : "prior range unavailable"}
              isPositiveOutcome
              icon={Users}
            />
            <MetricCard
              title="Unique VINs"
              value={numberLabel(data.overview.unique_vins)}
              change="VIN_final (ReportVIN fallback)"
              isPositiveOutcome
              icon={Car}
            />
            <MetricCard
              title="Date Range"
              value={`${data.meta.start} -> ${data.meta.end}`}
              change={`${data.meta.prior_equal_range.days} day window`}
              isPositiveOutcome
              icon={CalendarDays}
            />
          </div>

          <div className="mb-6 grid grid-cols-1 gap-4 lg:grid-cols-3">
            <Card className="border border-border bg-card lg:col-span-2">
              <CardHeader className="pb-2">
                <CardTitle className="text-base font-medium">Daily Scan Trend</CardTitle>
                <p className="text-xs text-muted-foreground">Daily inspections in UTC</p>
              </CardHeader>
              <CardContent>
                <div className="h-[340px]">
                  <ResponsiveContainer width="100%" height="100%">
                    <AreaChart data={data.dailyTrend}>
                      <defs>
                        <linearGradient id="scanGradient" x1="0" y1="0" x2="0" y2="1">
                          <stop offset="5%" stopColor="#2563eb" stopOpacity={0.55} />
                          <stop offset="95%" stopColor="#2563eb" stopOpacity={0.05} />
                        </linearGradient>
                      </defs>
                      <CartesianGrid strokeDasharray="3 3" strokeOpacity={0.25} />
                      <XAxis dataKey="date_pt" tickFormatter={shortDate} tick={{ fontSize: 11 }} />
                      <YAxis tick={{ fontSize: 11 }} />
                      <Tooltip
                        formatter={(value: number, name: string) => [numberLabel(value), name]}
                        labelFormatter={(label) => `Date: ${label}`}
                      />
                      <Area
                        type="monotone"
                        dataKey="scans"
                        stroke="#2563eb"
                        strokeWidth={2}
                        fill="url(#scanGradient)"
                        name="Inspections"
                      />
                    </AreaChart>
                  </ResponsiveContainer>
                </div>
              </CardContent>
            </Card>

            <Card className="border border-border bg-card">
              <CardHeader className="pb-2">
                <CardTitle className="text-base font-medium">4-Week Lift (Fixed Window)</CardTitle>
                <p className="text-xs text-muted-foreground">From 4-week extract: recent 28d vs prior 28d</p>
              </CardHeader>
              <CardContent className="space-y-3 text-sm">
                <div className="rounded-md border border-border p-3">
                  <p className="text-xs text-muted-foreground">Scan Lift</p>
                  <p className="text-xl font-semibold">
                    {percentLabel(data.fourWeekLift.summary?.scan_lift_pct ?? null)}
                  </p>
                  <p className="text-xs text-muted-foreground">
                    {data.fourWeekLift.windows
                      ? `${data.fourWeekLift.windows.recent.start} -> ${data.fourWeekLift.windows.recent.end}`
                      : "window unavailable"}
                  </p>
                </div>
                <div className="space-y-2">
                  <p className="text-xs font-medium text-muted-foreground">Top Tool Drivers</p>
                  {(data.fourWeekLift.drivers?.tools ?? []).slice(0, 5).map((item) => (
                    <div key={item.key} className="flex items-center justify-between text-xs">
                      <span className="truncate pr-3">{item.key}</span>
                      <span className={item.delta >= 0 ? "text-[var(--color-positive)]" : "text-[var(--color-negative)]"}>
                        {item.delta >= 0 ? "+" : ""}
                        {numberLabel(item.delta)}
                      </span>
                    </div>
                  ))}
                </div>
              </CardContent>
            </Card>
          </div>
        </>
      )}

      {!loading && !error && data && appliedTab === "users" && (
        <TopTable
          title="Top 100 USER IDs by Inspections"
          subtitle="Ranked by distinct DiagnosticReportId count in selected range"
          headers={["Rank", "User ID", "User Email", "Same-Day Reports", "Inspections"]}
          rows={data.topUsers.map((row, idx) => [
            idx + 1,
            row.user_id,
            row.user_email,
            numberLabel(row.same_day_reports),
            numberLabel(row.inspections),
          ])}
        />
      )}

      {!loading && !error && data && appliedTab === "tools" && (
        <TopTable
          title="Top INNOVA Tool Part # Usage with Diagnostic Part Signals"
          subtitle="Usage ranked by inspections, with ABS/SRS/CEL part row counts from fix-part extract"
          headers={["Rank", "Tool Part #", "Inspections", "CEL Parts", "ABS Parts", "SRS Parts"]}
          rows={data.topTools.map((row, idx) => [
            idx + 1,
            row.tool_name,
            numberLabel(row.inspections),
            numberLabel(row.cel_parts),
            numberLabel(row.abs_parts),
            numberLabel(row.srs_parts),
          ])}
        />
      )}

      {!loading && !error && data && appliedTab === "vin" && (
        <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
          <TopTable
            title="Most Frequently Inspected VINs"
            subtitle="VIN_final = ReportVIN fallback VehicleVIN"
            headers={["Rank", "VIN", "Year", "Make", "Model", "Inspections"]}
            rows={data.topVins.map((row, idx) => [
              idx + 1,
              row.vin,
              row.vehicle_year,
              row.vehicle_make,
              row.vehicle_model,
              numberLabel(row.inspections),
            ])}
          />
          <TopTable
            title="VIN by Same Account"
            subtitle="Top account + VIN combinations by inspections"
            headers={["Rank", "Account ID", "User Email", "VIN", "Year", "Make", "Model", "Inspections"]}
            rows={data.topAccountVins.map((row, idx) => [
              idx + 1,
              row.account_id,
              row.user_email,
              row.vin,
              row.vehicle_year,
              row.vehicle_make,
              row.vehicle_model,
              numberLabel(row.inspections),
            ])}
          />
        </div>
      )}

      {!loading && !error && data && appliedTab === "geo" && (
        <div className="grid grid-cols-1 gap-4 xl:grid-cols-3">
          <div className="xl:col-span-2">
            <UsZipHeatmap
              points={data.zipHeatPoints}
              missingLookup={data.zipHeatMeta.missing_centroid_lookup}
            />
          </div>
          <div className="space-y-4">
            <TopTable
              title="Top ZIP / Postal Codes"
              subtitle="Usage ranking by inspections"
              headers={["Rank", "ZIP/Postal", "ZIP5", "City", "State", "Inspections"]}
              rows={data.topZipPostal.map((row, idx) => [
                idx + 1,
                row.zip_postal,
                row.zip5,
                row.city,
                row.state,
                numberLabel(row.inspections),
              ])}
            />
            <Card className="border border-border bg-card">
              <CardHeader className="pb-2">
                <CardTitle className="text-base font-medium">Heatmap Coverage</CardTitle>
              </CardHeader>
              <CardContent className="space-y-1 text-xs text-muted-foreground">
                <p>ZIP5 with centroid: {numberLabel(data.zipHeatMeta.points_with_centroid)}</p>
                <p>Total ZIP5 in range: {numberLabel(data.zipHeatMeta.points_total_zip5)}</p>
                <p>Truncated for rendering: {data.zipHeatMeta.truncated ? "Yes" : "No"}</p>
              </CardContent>
            </Card>
          </div>
        </div>
      )}

      {!loading && !error && data && appliedTab === "ltl" && (
        <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
          <TopTable
            title="Top LTL Retailers"
            subtitle="BuyNow click rows grouped by BuyFrom (distinct report IDs)"
            headers={["Rank", "BuyFrom", "Inspections", "Total Buy Clicks"]}
            rows={data.ltlRetailers.map((row, idx) => [
              idx + 1,
              row.buy_from,
              numberLabel(row.inspections),
              numberLabel(row.total_buy_clicks),
            ])}
          />
          <TopTable
            title="Top LTL Clicked Accounts"
            subtitle="BuyNow click rows grouped by clicked AccountId (distinct report IDs)"
            headers={["Rank", "Clicked Account ID", "User Email", "Inspections", "Total Buy Clicks"]}
            rows={data.ltlClickedAccounts.map((row, idx) => [
              idx + 1,
              row.account_id,
              row.user_email,
              numberLabel(row.inspections),
              numberLabel(row.total_buy_clicks),
            ])}
          />
        </div>
      )}

      {!loading && !error && data && appliedTab === "gaps" && (
        <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
          <Card className="border border-border bg-card">
            <CardHeader className="pb-2">
              <CardTitle className="text-base font-medium">Data Gaps / Blockers</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              {data.dataGaps.map((item) => (
                <div key={item.question} className="rounded-md border border-border p-3">
                  <div className="mb-1 flex items-center justify-between gap-2">
                    <p className="text-sm font-medium">{item.question}</p>
                    <Badge variant={item.status === "blocked" ? "destructive" : "outline"}>{item.status}</Badge>
                  </div>
                  <p className="text-xs text-muted-foreground">{item.reason}</p>
                  <p className="mt-1 text-xs text-muted-foreground">Required source: {item.required_source}</p>
                </div>
              ))}
            </CardContent>
          </Card>

          <Card className="border border-border bg-card">
            <CardHeader className="pb-2">
              <CardTitle className="text-base font-medium">Data Quality Audit</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3 text-sm">
              <div className="rounded-md border border-border p-3">
                <p className="text-xs text-muted-foreground">Generated (UTC)</p>
                <p className="font-medium">{data.quality.generated_at_utc}</p>
              </div>
              <div className="rounded-md border border-border p-3">
                <p className="text-xs text-muted-foreground">Ambiguous Mapping Keys</p>
                <p className="font-medium">{data.quality.ambiguous_mapping_keys}</p>
              </div>
              <div className="rounded-md border border-border p-3">
                <p className="mb-2 text-xs text-muted-foreground">Top Unmapped USB IDs</p>
                <div className="space-y-1">
                  {data.quality.top_unmapped_usb.slice(0, 8).map((item) => (
                    <div key={item.usb_product_id} className="flex items-center justify-between text-xs">
                      <span>{item.usb_product_id || "(blank)"}</span>
                      <span>{numberLabel(item.rows)}</span>
                    </div>
                  ))}
                </div>
              </div>
            </CardContent>
          </Card>
        </div>
      )}

      {!loading && !error && data && (
        <Card className="mt-6 border border-border bg-card">
          <CardContent className="flex items-center gap-2 py-3 text-xs text-muted-foreground">
            <TrendingUp className="h-3.5 w-3.5" />
            Last refresh source range: {data.meta.available_start} {"->"} {data.meta.available_end} (UTC daily buckets)
            <MapPinned className="ml-3 h-3.5 w-3.5" />
            ZIP heatmap uses ZIP5 lat/lng lookup (geo CSV + deterministic fallback centroids).
          </CardContent>
        </Card>
      )}
    </>
  )
}
