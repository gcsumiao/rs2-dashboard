import { readFile } from "fs/promises"
import path from "path"

type DailyMetric = {
  date_pt: string
  scans: number
  unique_users: number
  unique_vins: number
}

type FourWeekLiftSummary = {
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

type QualityAudit = {
  generated_at_utc: string
  row_counts?: {
    scan_level?: {
      top_unmapped_usb?: Array<[string, number]>
    }
  }
  mapping?: {
    fallback_example_usb_937_blank_customer?: string
    ambiguous_mapping_keys?: Array<unknown>
  }
}

type ScanDayAgg = {
  scans: number
  users: Map<string, number>
  user_emails: Map<string, number>
  tool_stats: Map<string, { inspections: number; abs_parts: number; srs_parts: number; cel_parts: number }>
  vins: Map<string, number>
  account_vins: Map<string, number>
  zip_postal: Map<string, number>
  zip5: Map<string, number>
}

type BuyNowDayAgg = {
  buy_from: Map<string, { reports: string[]; clicks: number }>
  clicked_account: Map<string, { reports: string[]; clicks: number; emails: Array<[string, number]> }>
}

type LookupData = {
  user_lookup: Record<string, string>
  vin_lookup: Record<string, { year: string; make: string; model: string; engine: string }>
  account_vin_lookup: Record<string, { email: string; year: string; make: string; model: string; engine: string }>
  zip_lookup: Record<string, { city: string; state: string; zip5: string; lat?: string; lng?: string }>
  clicked_account_email_lookup: Record<string, string>
  mapping_fallback_usb937: string | null
  ambiguous_mapping_keys: number
}

type LoadedData = {
  snapshotDefault: DashboardResponse | null
  snapshotDefaultTopN: number
  scanDayAgg: Map<string, ScanDayAgg>
  buynowDayAgg: Map<string, BuyNowDayAgg>
  dailyMetrics: DailyMetric[]
  fourWeekLift: FourWeekLiftSummary
  quality: QualityAudit
  lookup: LookupData
  availableDates: string[]
}

type SnapshotOnlyData = {
  snapshotDefault: DashboardResponse | null
  snapshotDefaultTopN: number
}

type DashboardParams = {
  start: string
  end: string
  topN: number
}

type RankedItem = {
  key: string
  inspections: number
}

type HeatPoint = {
  zip_postal: string
  zip5: string
  city: string
  state: string
  lat: number
  lng: number
  scans: number
  percentile: number
}

type DateRange = { start: string; end: string; days: number }

export type DashboardResponse = {
  meta: {
    start: string
    end: string
    topN: number
    available_start: string
    available_end: string
    prior_equal_range: DateRange
    has_prior_data: boolean
    snapshot_ready?: boolean
  }
  overview: {
    scans: number
    unique_users: number
    unique_vins: number
    prior_scans: number | null
    scan_lift_pct_vs_prior: number | null
  }
  dailyTrend: DailyMetric[]
  topUsers: Array<{ user_id: string; user_email: string; same_day_reports: number; inspections: number }>
  topTools: Array<{
    tool_name: string
    inspections: number
    abs_parts: number
    srs_parts: number
    cel_parts: number
  }>
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
  topZipPostal: Array<{
    zip_postal: string
    city: string
    state: string
    zip5: string
    inspections: number
  }>
  ltlRetailers: Array<{ buy_from: string; inspections: number; total_buy_clicks: number }>
  ltlClickedAccounts: Array<{ account_id: string; user_email: string; inspections: number; total_buy_clicks: number }>
  zipHeatPoints: HeatPoint[]
  zipHeatMeta: {
    points_with_centroid: number
    points_total_zip5: number
    truncated: boolean
    missing_centroid_lookup: boolean
  }
  fourWeekLift: FourWeekLiftSummary
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
const MAX_TOP_N = 500
const MAX_HEAT_POINTS = 8000

const STATE_CENTROIDS: Record<string, { lat: number; lng: number }> = {
  AL: { lat: 32.806671, lng: -86.79113 },
  AK: { lat: 61.370716, lng: -152.404419 },
  AZ: { lat: 33.729759, lng: -111.431221 },
  AR: { lat: 34.969704, lng: -92.373123 },
  CA: { lat: 36.116203, lng: -119.681564 },
  CO: { lat: 39.059811, lng: -105.311104 },
  CT: { lat: 41.597782, lng: -72.755371 },
  DE: { lat: 39.318523, lng: -75.507141 },
  FL: { lat: 27.766279, lng: -81.686783 },
  GA: { lat: 33.040619, lng: -83.643074 },
  HI: { lat: 21.094318, lng: -157.498337 },
  ID: { lat: 44.240459, lng: -114.478828 },
  IL: { lat: 40.349457, lng: -88.986137 },
  IN: { lat: 39.849426, lng: -86.258278 },
  IA: { lat: 42.011539, lng: -93.210526 },
  KS: { lat: 38.5266, lng: -96.726486 },
  KY: { lat: 37.66814, lng: -84.670067 },
  LA: { lat: 31.169546, lng: -91.867805 },
  ME: { lat: 44.693947, lng: -69.381927 },
  MD: { lat: 39.063946, lng: -76.802101 },
  MA: { lat: 42.230171, lng: -71.530106 },
  MI: { lat: 43.326618, lng: -84.536095 },
  MN: { lat: 45.694454, lng: -93.900192 },
  MS: { lat: 32.741646, lng: -89.678696 },
  MO: { lat: 38.456085, lng: -92.288368 },
  MT: { lat: 46.921925, lng: -110.454353 },
  NE: { lat: 41.12537, lng: -98.268082 },
  NV: { lat: 38.313515, lng: -117.055374 },
  NH: { lat: 43.452492, lng: -71.563896 },
  NJ: { lat: 40.298904, lng: -74.521011 },
  NM: { lat: 34.840515, lng: -106.248482 },
  NY: { lat: 42.165726, lng: -74.948051 },
  NC: { lat: 35.630066, lng: -79.806419 },
  ND: { lat: 47.528912, lng: -99.784012 },
  OH: { lat: 40.388783, lng: -82.764915 },
  OK: { lat: 35.565342, lng: -96.928917 },
  OR: { lat: 44.572021, lng: -122.070938 },
  PA: { lat: 40.590752, lng: -77.209755 },
  RI: { lat: 41.680893, lng: -71.51178 },
  SC: { lat: 33.856892, lng: -80.945007 },
  SD: { lat: 44.299782, lng: -99.438828 },
  TN: { lat: 35.747845, lng: -86.692345 },
  TX: { lat: 31.054487, lng: -97.563461 },
  UT: { lat: 40.150032, lng: -111.862434 },
  VT: { lat: 44.045876, lng: -72.710686 },
  VA: { lat: 37.769337, lng: -78.169968 },
  WA: { lat: 47.400902, lng: -121.490494 },
  WV: { lat: 38.491226, lng: -80.954453 },
  WI: { lat: 44.268543, lng: -89.616508 },
  WY: { lat: 42.755966, lng: -107.30249 },
  DC: { lat: 38.897438, lng: -77.026817 },
}

let snapshotCachePromise: Promise<SnapshotOnlyData> | null = null
let fullCachePromise: Promise<LoadedData> | null = null
let fullWarmStarted = false
let commonRangesWarmStarted = false
const RANGE_RESPONSE_CACHE_LIMIT = 8
const rangeResponseCache = new Map<string, DashboardResponse>()

function parseIsoDate(dateText: string): Date {
  return new Date(`${dateText}T00:00:00Z`)
}

function shiftDate(dateText: string, days: number): string {
  const date = parseIsoDate(dateText)
  date.setUTCDate(date.getUTCDate() + days)
  return date.toISOString().slice(0, 10)
}

function rangeDaysInclusive(start: string, end: string): number {
  const startDate = parseIsoDate(start).getTime()
  const endDate = parseIsoDate(end).getTime()
  return Math.floor((endDate - startDate) / 86400000) + 1
}

function makePriorRange(start: string, end: string): DateRange {
  const days = rangeDaysInclusive(start, end)
  const priorEnd = shiftDate(start, -1)
  const priorStart = shiftDate(priorEnd, -(days - 1))
  return { start: priorStart, end: priorEnd, days }
}

function clampTopN(value: number): number {
  if (!Number.isFinite(value)) return 100
  if (value < 1) return 1
  if (value > MAX_TOP_N) return MAX_TOP_N
  return Math.floor(value)
}

function normalizeDateParam(value: string | null, fallback: string): string {
  if (!value) return fallback
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return fallback
  return value
}

function mapFromPairs(pairs: Array<[string, number]>): Map<string, number> {
  const map = new Map<string, number>()
  for (const [key, value] of pairs) {
    map.set(String(key), Number(value))
  }
  return map
}

function parseScanDayAgg(raw: string): Map<string, ScanDayAgg> {
  const parsed = JSON.parse(raw) as {
    dates: Record<
      string,
      {
        scans: number
        users: Array<[string, number]>
        user_emails: Array<[string, number]>
        tool_stats: Array<[string, number, number, number, number]>
        vins: Array<[string, number]>
        account_vins: Array<[string, number]>
        zip_postal: Array<[string, number]>
        zip5: Array<[string, number]>
      }
    >
  }
  const map = new Map<string, ScanDayAgg>()
  for (const [datePt, slot] of Object.entries(parsed.dates ?? {})) {
    const toolStats = new Map<string, { inspections: number; abs_parts: number; srs_parts: number; cel_parts: number }>()
    for (const row of slot.tool_stats ?? []) {
      toolStats.set(String(row[0]), {
        inspections: Number(row[1]),
        abs_parts: Number(row[2]),
        srs_parts: Number(row[3]),
        cel_parts: Number(row[4]),
      })
    }
    map.set(datePt, {
      scans: Number(slot.scans ?? 0),
      users: mapFromPairs(slot.users ?? []),
      user_emails: mapFromPairs(slot.user_emails ?? []),
      tool_stats: toolStats,
      vins: mapFromPairs(slot.vins ?? []),
      account_vins: mapFromPairs(slot.account_vins ?? []),
      zip_postal: mapFromPairs(slot.zip_postal ?? []),
      zip5: mapFromPairs(slot.zip5 ?? []),
    })
  }
  return map
}

function parseBuyNowAgg(raw: string): Map<string, BuyNowDayAgg> {
  const parsed = JSON.parse(raw) as {
    dates: Record<
      string,
      {
        buy_from: Array<[string, string[], number]>
        clicked_account: Array<[string, string[], number, Array<[string, number]>]>
      }
    >
  }
  const map = new Map<string, BuyNowDayAgg>()
  for (const [datePt, slot] of Object.entries(parsed.dates ?? {})) {
    const buyFrom = new Map<string, { reports: string[]; clicks: number }>()
    for (const row of slot.buy_from ?? []) {
      buyFrom.set(String(row[0]), { reports: row[1] ?? [], clicks: Number(row[2] ?? 0) })
    }
    const clicked = new Map<string, { reports: string[]; clicks: number; emails: Array<[string, number]> }>()
    for (const row of slot.clicked_account ?? []) {
      clicked.set(String(row[0]), {
        reports: row[1] ?? [],
        clicks: Number(row[2] ?? 0),
        emails: row[3] ?? [],
      })
    }
    map.set(datePt, { buy_from: buyFrom, clicked_account: clicked })
  }
  return map
}

async function loadSnapshotOnly(): Promise<SnapshotOnlyData> {
  const dataDir = path.resolve(process.cwd(), "data", "rs2")
  const snapshotRaw = await readFile(path.join(dataDir, "snapshot_default.json"), "utf-8").catch(() => "")
  const snapshotDefault = snapshotRaw ? (JSON.parse(snapshotRaw) as DashboardResponse) : null
  return {
    snapshotDefault,
    snapshotDefaultTopN: snapshotDefault?.meta.topN ?? MAX_TOP_N,
  }
}

async function getSnapshotOnlyData(): Promise<SnapshotOnlyData> {
  if (!snapshotCachePromise) {
    snapshotCachePromise = loadSnapshotOnly()
  }
  return snapshotCachePromise
}

async function loadFullData(): Promise<LoadedData> {
  const dataDir = path.resolve(process.cwd(), "data", "rs2")
  const snapshotOnly = await getSnapshotOnlyData()

  const [scanAggRaw, buynowAggRaw, dailyMetricsRaw, fourWeekLiftRaw, qualityRaw, lookupRaw] = await Promise.all(
    [
      readFile(path.join(dataDir, "scan_day_agg.json"), "utf-8"),
      readFile(path.join(dataDir, "buynow_click_day_reports.json"), "utf-8"),
      readFile(path.join(dataDir, "daily_scan_metrics.json"), "utf-8"),
      readFile(path.join(dataDir, "four_week_lift_summary.json"), "utf-8"),
      readFile(path.join(dataDir, "quality_audit.json"), "utf-8"),
      readFile(path.join(dataDir, "lookups.json"), "utf-8"),
    ]
  )

  const scanDayAgg = parseScanDayAgg(scanAggRaw)
  const buynowDayAgg = parseBuyNowAgg(buynowAggRaw)
  const dailyMetrics = JSON.parse(dailyMetricsRaw) as DailyMetric[]
  const lookup = JSON.parse(lookupRaw) as LookupData
  const availableDates = Array.from(scanDayAgg.keys()).sort()

  return {
    snapshotDefault: snapshotOnly.snapshotDefault,
    snapshotDefaultTopN: snapshotOnly.snapshotDefaultTopN,
    scanDayAgg,
    buynowDayAgg,
    dailyMetrics,
    fourWeekLift: JSON.parse(fourWeekLiftRaw) as FourWeekLiftSummary,
    quality: JSON.parse(qualityRaw) as QualityAudit,
    lookup,
    availableDates,
  }
}

async function getFullData(): Promise<LoadedData> {
  if (!fullCachePromise) {
    fullCachePromise = loadFullData()
  }
  return fullCachePromise
}

function getRangeCacheKey(start: string, end: string, topN: number): string {
  return `${start}|${end}|${topN}`
}

function getCachedRangeResponse(key: string): DashboardResponse | null {
  const hit = rangeResponseCache.get(key)
  if (!hit) return null
  // refresh recency for simple LRU behavior
  rangeResponseCache.delete(key)
  rangeResponseCache.set(key, hit)
  return hit
}

function setCachedRangeResponse(key: string, payload: DashboardResponse): void {
  if (rangeResponseCache.has(key)) {
    rangeResponseCache.delete(key)
  }
  rangeResponseCache.set(key, payload)
  while (rangeResponseCache.size > RANGE_RESPONSE_CACHE_LIMIT) {
    const oldestKey = rangeResponseCache.keys().next().value
    if (!oldestKey) break
    rangeResponseCache.delete(oldestKey)
  }
}

function warmFullDataInBackground(): void {
  if (fullWarmStarted) return
  fullWarmStarted = true
  void getFullData()
    .then(() => {
      if (commonRangesWarmStarted) return
      commonRangesWarmStarted = true
      const commonRanges = [
        { start: shiftDate(DEFAULT_END, -6), end: DEFAULT_END }, // 7D
        { start: shiftDate(DEFAULT_END, -13), end: DEFAULT_END }, // 14D
        { start: shiftDate(DEFAULT_END, -27), end: DEFAULT_END }, // 28D
        { start: "2026-01-01", end: "2026-01-31" }, // Jan month
        { start: "2026-02-01", end: "2026-02-28" }, // Feb month
      ]
      for (const range of commonRanges) {
        const key = getRangeCacheKey(range.start, range.end, 100)
        if (rangeResponseCache.has(key)) continue
        void buildRs2DashboardResponse({ start: range.start, end: range.end, topN: 100 }).catch(() => {
          // Keep warming best-effort; ignore individual failures.
        })
      }
    })
    .catch(() => {
      // Allow retry on next request if warmup fails.
      fullWarmStarted = false
    })
}

function takeRanked(counter: Map<string, number>, topN: number): RankedItem[] {
  const values: RankedItem[] = []
  for (const [key, inspections] of counter.entries()) {
    values.push({ key, inspections })
  }
  values.sort((a, b) => (b.inspections === a.inspections ? a.key.localeCompare(b.key) : b.inspections - a.inspections))
  return values.slice(0, topN)
}

function buildPercentileMap(counter: Map<string, number>): Map<string, number> {
  const entries = Array.from(counter.entries()).sort((a, b) => a[1] - b[1])
  const out = new Map<string, number>()
  if (entries.length === 0) return out
  if (entries.length === 1) {
    out.set(entries[0][0], 100)
    return out
  }
  for (let i = 0; i < entries.length; i += 1) {
    out.set(entries[i][0], Math.round((i / (entries.length - 1)) * 100))
  }
  return out
}

function filterDaily(metrics: DailyMetric[], start: string, end: string): DailyMetric[] {
  return metrics.filter((item) => item.date_pt >= start && item.date_pt <= end)
}

function sumDailyScans(metrics: DailyMetric[], start: string, end: string): number {
  let scans = 0
  for (const item of metrics) {
    if (item.date_pt < start || item.date_pt > end) continue
    scans += item.scans
  }
  return scans
}

function stableHash(text: string): number {
  let hash = 2166136261
  for (let i = 0; i < text.length; i += 1) {
    hash ^= text.charCodeAt(i)
    hash += (hash << 1) + (hash << 4) + (hash << 7) + (hash << 8) + (hash << 24)
  }
  return Math.abs(hash >>> 0)
}

function addCount(target: Map<string, number>, key: string, value: number): void {
  target.set(key, (target.get(key) ?? 0) + value)
}

function sliceSnapshot(snapshot: DashboardResponse, topN: number): DashboardResponse {
  return {
    ...snapshot,
    meta: {
      ...snapshot.meta,
      topN,
    },
    topUsers: snapshot.topUsers.slice(0, topN),
    topTools: snapshot.topTools.slice(0, topN),
    topVins: snapshot.topVins.slice(0, topN),
    topAccountVins: snapshot.topAccountVins.slice(0, topN),
    topZipPostal: snapshot.topZipPostal.slice(0, topN),
    ltlRetailers: snapshot.ltlRetailers.slice(0, topN),
    ltlClickedAccounts: snapshot.ltlClickedAccounts.slice(0, topN),
  }
}

export async function buildRs2DashboardResponse(params: DashboardParams): Promise<DashboardResponse> {
  const topN = clampTopN(params.topN)
  const start = normalizeDateParam(params.start, DEFAULT_START)
  const end = normalizeDateParam(params.end, DEFAULT_END)
  const rangeKey = getRangeCacheKey(start, end, topN)
  const cached = getCachedRangeResponse(rangeKey)
  if (cached) {
    return cached
  }
  const snapshotOnly = await getSnapshotOnlyData()

  if (
    snapshotOnly.snapshotDefault &&
    start === DEFAULT_START &&
    end === DEFAULT_END &&
    topN <= snapshotOnly.snapshotDefaultTopN &&
    (snapshotOnly.snapshotDefault.zipHeatPoints?.length ?? 0) > 0
  ) {
    const sliced = sliceSnapshot(snapshotOnly.snapshotDefault, topN)
    setCachedRangeResponse(rangeKey, sliced)
    warmFullDataInBackground()
    return sliced
  }
  const data = await getFullData()

  const usersCounter = new Map<string, number>()
  const userEmailCounter = new Map<string, number>()
  const userMaxDay = new Map<string, number>()
  const toolInspectionCounter = new Map<string, number>()
  const toolAbsCounter = new Map<string, number>()
  const toolSrsCounter = new Map<string, number>()
  const toolCelCounter = new Map<string, number>()
  const vinsCounter = new Map<string, number>()
  const accountVinsCounter = new Map<string, number>()
  const zipCounter = new Map<string, number>()
  const zip5Counter = new Map<string, number>()
  let scans = 0

  for (const [datePt, slot] of data.scanDayAgg.entries()) {
    if (datePt < start || datePt > end) continue
    scans += slot.scans

    for (const [user, count] of slot.users.entries()) {
      addCount(usersCounter, user, count)
      const prev = userMaxDay.get(user) ?? 0
      if (count > prev) {
        userMaxDay.set(user, count)
      }
    }
    for (const [pair, count] of slot.user_emails.entries()) {
      addCount(userEmailCounter, pair, count)
    }
    for (const [tool, stat] of slot.tool_stats.entries()) {
      addCount(toolInspectionCounter, tool, stat.inspections)
      addCount(toolAbsCounter, tool, stat.abs_parts)
      addCount(toolSrsCounter, tool, stat.srs_parts)
      addCount(toolCelCounter, tool, stat.cel_parts)
    }
    for (const [vin, count] of slot.vins.entries()) addCount(vinsCounter, vin, count)
    for (const [key, count] of slot.account_vins.entries()) addCount(accountVinsCounter, key, count)
    for (const [zipPostal, count] of slot.zip_postal.entries()) addCount(zipCounter, zipPostal, count)
    for (const [zip5, count] of slot.zip5.entries()) addCount(zip5Counter, zip5, count)
  }

  const ltlRetailerReports = new Map<string, Set<string>>()
  const ltlRetailerClicks = new Map<string, number>()
  const ltlAccountReports = new Map<string, Set<string>>()
  const ltlAccountClicks = new Map<string, number>()
  const ltlAccountEmailCounter = new Map<string, number>()

  for (const [datePt, slot] of data.buynowDayAgg.entries()) {
    if (datePt < start || datePt > end) continue
    for (const [group, value] of slot.buy_from.entries()) {
      const existing = ltlRetailerReports.get(group) ?? new Set<string>()
      for (const reportId of value.reports) existing.add(reportId)
      ltlRetailerReports.set(group, existing)
      addCount(ltlRetailerClicks, group, value.clicks)
    }
    for (const [account, value] of slot.clicked_account.entries()) {
      const existing = ltlAccountReports.get(account) ?? new Set<string>()
      for (const reportId of value.reports) existing.add(reportId)
      ltlAccountReports.set(account, existing)
      addCount(ltlAccountClicks, account, value.clicks)
      for (const [email, count] of value.emails) {
        addCount(ltlAccountEmailCounter, `${account}|||${email}`, Number(count))
      }
    }
  }

  const primaryUserEmail = new Map<string, string>()
  for (const [user, inspections] of usersCounter.entries()) {
    let bestEmail = data.lookup.user_lookup[user] ?? ""
    let bestCount = -1
    for (const [pair, count] of userEmailCounter.entries()) {
      const [pairUser, email] = pair.split("|||")
      if (pairUser !== user) continue
      if (count > bestCount) {
        bestCount = count
        bestEmail = email ?? ""
      }
    }
    if (!bestEmail && inspections > 0) {
      bestEmail = data.lookup.user_lookup[user] ?? ""
    }
    primaryUserEmail.set(user, bestEmail)
  }

  const primaryClickedEmail = new Map<string, string>()
  for (const [account] of ltlAccountReports.entries()) {
    let email = data.lookup.clicked_account_email_lookup[account] ?? ""
    let bestCount = -1
    for (const [pair, count] of ltlAccountEmailCounter.entries()) {
      const [pairAccount, pairEmail] = pair.split("|||")
      if (pairAccount !== account) continue
      if (count > bestCount) {
        bestCount = count
        email = pairEmail ?? ""
      }
    }
    primaryClickedEmail.set(account, email)
  }

  const priorRange = makePriorRange(start, end)
  const priorScans = sumDailyScans(data.dailyMetrics, priorRange.start, priorRange.end)
  const hasPriorData = priorScans > 0
  const scanLiftPctVsPrior = hasPriorData ? ((scans - priorScans) / priorScans) * 100 : null

  const topUsers = takeRanked(usersCounter, topN).map((item) => ({
    user_id: item.key,
    user_email: primaryUserEmail.get(item.key) ?? "",
    same_day_reports: userMaxDay.get(item.key) ?? 0,
    inspections: item.inspections,
  }))

  const mappedToolCounter = new Map(
    Array.from(toolInspectionCounter.entries()).filter(([toolName]) => Boolean(toolName) && toolName !== "UNMAPPED")
  )
  const topTools = takeRanked(mappedToolCounter, topN).map((item) => ({
    tool_name: item.key,
    inspections: item.inspections,
    abs_parts: toolAbsCounter.get(item.key) ?? 0,
    srs_parts: toolSrsCounter.get(item.key) ?? 0,
    cel_parts: toolCelCounter.get(item.key) ?? 0,
  }))

  const topVins = takeRanked(vinsCounter, topN).map((item) => {
    const meta = data.lookup.vin_lookup[item.key] ?? { year: "", make: "", model: "", engine: "" }
    return {
      vin: item.key,
      vehicle_year: meta.year ?? "",
      vehicle_make: meta.make ?? "",
      vehicle_model: meta.model ?? "",
      vehicle_engine: meta.engine ?? "",
      inspections: item.inspections,
    }
  })

  const topAccountVins = takeRanked(accountVinsCounter, topN).map((item) => {
    const [account_id, vin] = item.key.split("|||")
    const meta =
      data.lookup.account_vin_lookup[item.key] ?? {
        email: data.lookup.user_lookup[account_id] ?? "",
        year: "",
        make: "",
        model: "",
        engine: "",
      }
    return {
      account_id: account_id ?? "",
      user_email: meta.email ?? "",
      vin: vin ?? "",
      vehicle_year: meta.year ?? "",
      vehicle_make: meta.make ?? "",
      vehicle_model: meta.model ?? "",
      vehicle_engine: meta.engine ?? "",
      inspections: item.inspections,
    }
  })

  const topZipPostal = takeRanked(zipCounter, topN).map((item) => {
    const meta = data.lookup.zip_lookup[item.key] ?? { city: "", state: "", zip5: "" }
    return {
      zip_postal: item.key,
      city: meta.city ?? "",
      state: meta.state ?? "",
      zip5: meta.zip5 ?? "",
      inspections: item.inspections,
    }
  })

  const ltlRetailers = takeRanked(
    new Map(Array.from(ltlRetailerReports.entries()).map(([key, set]) => [key, set.size])),
    topN
  ).map((item) => ({
    buy_from: item.key,
    inspections: item.inspections,
    total_buy_clicks: ltlRetailerClicks.get(item.key) ?? 0,
  }))

  const ltlClickedAccounts = takeRanked(
    new Map(Array.from(ltlAccountReports.entries()).map(([key, set]) => [key, set.size])),
    topN
  ).map((item) => ({
    account_id: item.key,
    user_email: primaryClickedEmail.get(item.key) ?? "",
    inspections: item.inspections,
    total_buy_clicks: ltlAccountClicks.get(item.key) ?? 0,
  }))

  const percentileMap = buildPercentileMap(zipCounter)
  const heatPoints: HeatPoint[] = []
  for (const [zipPostal, count] of zipCounter.entries()) {
    const info = data.lookup.zip_lookup[zipPostal]
    if (!info) continue
    const state = info.state?.toUpperCase() ?? ""
    let lat = Number.parseFloat(info.lat ?? "")
    let lng = Number.parseFloat(info.lng ?? "")
    if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
      const centroid = STATE_CENTROIDS[state]
      if (!centroid) continue
      const hash = stableHash(zipPostal)
      const offsetLat = ((hash % 2001) - 1000) / 2500
      const offsetLng = ((Math.floor(hash / 2001) % 2001) - 1000) / 2500
      lat = centroid.lat + offsetLat
      lng = centroid.lng + offsetLng
    }
    const zip5 = info.zip5 || zipPostal
    heatPoints.push({
      zip_postal: zipPostal,
      zip5,
      city: info.city ?? "",
      state,
      lat,
      lng,
      scans: count,
      percentile: percentileMap.get(zipPostal) ?? 0,
    })
  }
  heatPoints.sort((a, b) => b.scans - a.scans || a.zip_postal.localeCompare(b.zip_postal))
  const truncated = heatPoints.length > MAX_HEAT_POINTS
  const zipHeatPoints = truncated ? heatPoints.slice(0, MAX_HEAT_POINTS) : heatPoints

  const topUnmappedUsb = data.quality.row_counts?.scan_level?.top_unmapped_usb ?? []

  const response: DashboardResponse = {
    meta: {
      start,
      end,
      topN,
      available_start: data.availableDates[0] ?? start,
      available_end: data.availableDates[data.availableDates.length - 1] ?? end,
      prior_equal_range: priorRange,
      has_prior_data: hasPriorData,
    },
    overview: {
      scans,
      unique_users: usersCounter.size,
      unique_vins: vinsCounter.size,
      prior_scans: hasPriorData ? priorScans : null,
      scan_lift_pct_vs_prior: scanLiftPctVsPrior,
    },
    dailyTrend: filterDaily(data.dailyMetrics, start, end),
    topUsers,
    topTools,
    topVins,
    topAccountVins,
    topZipPostal,
    ltlRetailers,
    ltlClickedAccounts,
    zipHeatPoints,
    zipHeatMeta: {
      points_with_centroid: heatPoints.length,
      points_total_zip5: zipCounter.size,
      truncated,
      missing_centroid_lookup: heatPoints.length === 0,
    },
    fourWeekLift: data.fourWeekLift,
    dataGaps: [
      {
        question: "Vehicle Care section usage instances last month",
        status: "blocked",
        reason: "GA4/event telemetry extract not loaded in this dashboard workspace.",
        required_source: "GA4 Vehicle Care event export",
      },
      {
        question: "Scan limit policy by user/time frame",
        status: "skipped",
        reason: "Policy/config source intentionally not provided for this dashboard scope.",
        required_source: "Policy/config documentation",
      },
      {
        question: "Report retention duration / forever availability",
        status: "skipped",
        reason: "Policy/config source intentionally not provided for this dashboard scope.",
        required_source: "Policy/config documentation",
      },
    ],
    quality: {
      generated_at_utc: data.quality.generated_at_utc,
      mapping_fallback_usb937: data.lookup.mapping_fallback_usb937,
      ambiguous_mapping_keys: data.lookup.ambiguous_mapping_keys,
      top_unmapped_usb: topUnmappedUsb.map(([usbProductId, rows]) => ({
        usb_product_id: String(usbProductId),
        rows: Number(rows),
      })),
    },
  }
  setCachedRangeResponse(rangeKey, response)
  return response
}

export function getNormalizedParams(searchParams: URLSearchParams): DashboardParams {
  const start = normalizeDateParam(searchParams.get("start"), DEFAULT_START)
  const end = normalizeDateParam(searchParams.get("end"), DEFAULT_END)
  const topN = clampTopN(Number.parseInt(searchParams.get("topN") ?? "100", 10))
  if (start <= end) return { start, end, topN }
  return { start: end, end: start, topN }
}
