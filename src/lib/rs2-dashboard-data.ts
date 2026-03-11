import { dbQuery } from "@/lib/rs2-db"
import { US_STATE_CODES, US_STATE_NAME_BY_CODE } from "@/lib/us-state-names"
import { extractZip5, getZipGeoLookup } from "@/lib/zip-geo-lookup"

type DateRange = { start: string; end: string; days: number }

type DashboardParams = {
  start: string
  end: string
  topN: number
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

type StateInspection = {
  state: string
  state_name: string
  inspections: number
  top_cities: Array<{ city: string; inspections: number }>
}

export type DashboardResponse = {
  meta: {
    start: string
    end: string
    topN: number
    available_start: string
    available_end: string
    prior_equal_range: DateRange
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
  stateInspections: StateInspection[]
  geoStateMeta: {
    states_with_inspections: number
    zip5_with_lookup: number
    points_total_zip5: number
    missing_geo_lookup: boolean
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

function toInt(value: unknown): number {
  if (value === null || value === undefined) return 0
  if (typeof value === "number") return Number.isFinite(value) ? Math.trunc(value) : 0
  const parsed = Number.parseInt(String(value), 10)
  return Number.isFinite(parsed) ? parsed : 0
}

function topDelta(
  recent: Map<string, number>,
  prior: Map<string, number>,
  limit = 12
): Array<{ key: string; delta: number; recent: number; prior: number }> {
  const rows: Array<{ key: string; delta: number; recent: number; prior: number }> = []
  const keys = new Set<string>([...recent.keys(), ...prior.keys()])
  for (const key of keys) {
    const recentValue = recent.get(key) ?? 0
    const priorValue = prior.get(key) ?? 0
    rows.push({
      key,
      delta: recentValue - priorValue,
      recent: recentValue,
      prior: priorValue,
    })
  }
  rows.sort((a, b) => (b.delta === a.delta ? a.key.localeCompare(b.key) : b.delta - a.delta))
  return rows.slice(0, limit)
}

async function loadWindowSummary(start: string, end: string): Promise<{ scans: number; users: number; vins: number }> {
  const rows = await dbQuery<{ scans: string; users: string; vins: string }>(
    `
      SELECT
        COUNT(*)::bigint AS scans,
        COUNT(DISTINCT NULLIF(user_id, ''))::bigint AS users,
        COUNT(DISTINCT NULLIF(vin_final, ''))::bigint AS vins
      FROM rs2_scan
      WHERE date_utc BETWEEN $1::date AND $2::date
    `,
    [start, end]
  )
  const row = rows[0]
  return {
    scans: toInt(row?.scans),
    users: toInt(row?.users),
    vins: toInt(row?.vins),
  }
}

async function loadCounterMap(start: string, end: string, kind: "tools" | "zip5" | "buy_from"): Promise<Map<string, number>> {
  if (kind === "tools") {
    const rows = await dbQuery<{ key: string; value: string }>(
      `
        SELECT tool_name AS key, COUNT(*)::bigint AS value
        FROM rs2_scan
        WHERE date_utc BETWEEN $1::date AND $2::date
          AND tool_name <> ''
          AND tool_name <> 'UNMAPPED'
        GROUP BY tool_name
      `,
      [start, end]
    )
    return new Map(rows.map((row) => [row.key, toInt(row.value)]))
  }
  if (kind === "zip5") {
    const rows = await dbQuery<{ key: string; value: string }>(
      `
        SELECT zip5 AS key, COUNT(*)::bigint AS value
        FROM rs2_scan
        WHERE date_utc BETWEEN $1::date AND $2::date
          AND zip5 <> ''
        GROUP BY zip5
      `,
      [start, end]
    )
    return new Map(rows.map((row) => [row.key, toInt(row.value)]))
  }
  const rows = await dbQuery<{ key: string; value: string }>(
    `
      SELECT buy_from AS key, COUNT(*)::bigint AS value
      FROM rs2_buynow_click
      WHERE date_utc BETWEEN $1::date AND $2::date
        AND buy_from <> ''
      GROUP BY buy_from
    `,
    [start, end]
  )
  return new Map(rows.map((row) => [row.key, toInt(row.value)]))
}

async function loadFourWeekLift(): Promise<FourWeekLiftSummary> {
  const maxDateRows = await dbQuery<{ max_date: string | null }>(
    "SELECT MAX(date_utc)::text AS max_date FROM rs2_scan"
  )
  const maxDate = maxDateRows[0]?.max_date
  if (!maxDate) {
    return { status: "empty" }
  }

  const recentEnd = maxDate
  const recentStart = shiftDate(recentEnd, -27)
  const priorEnd = shiftDate(recentStart, -1)
  const priorStart = shiftDate(priorEnd, -27)

  const [recent, prior, recentTools, priorTools, recentZip5, priorZip5, recentBuyFrom, priorBuyFrom] = await Promise.all([
    loadWindowSummary(recentStart, recentEnd),
    loadWindowSummary(priorStart, priorEnd),
    loadCounterMap(recentStart, recentEnd, "tools"),
    loadCounterMap(priorStart, priorEnd, "tools"),
    loadCounterMap(recentStart, recentEnd, "zip5"),
    loadCounterMap(priorStart, priorEnd, "zip5"),
    loadCounterMap(recentStart, recentEnd, "buy_from"),
    loadCounterMap(priorStart, priorEnd, "buy_from"),
  ])

  const lift = prior.scans > 0 ? ((recent.scans - prior.scans) / prior.scans) * 100 : null
  return {
    status: "ok",
    windows: {
      recent: { start: recentStart, end: recentEnd },
      prior: { start: priorStart, end: priorEnd },
    },
    summary: {
      recent_scans: recent.scans,
      prior_scans: prior.scans,
      scan_delta: recent.scans - prior.scans,
      scan_lift_pct: lift,
      recent_unique_users: recent.users,
      prior_unique_users: prior.users,
      recent_unique_vins: recent.vins,
      prior_unique_vins: prior.vins,
    },
    drivers: {
      tools: topDelta(recentTools, priorTools),
      zip5: topDelta(recentZip5, priorZip5),
      buy_from: topDelta(recentBuyFrom, priorBuyFrom),
    },
  }
}

export async function buildRs2DashboardResponse(params: DashboardParams): Promise<DashboardResponse> {
  const topN = clampTopN(params.topN)
  const normalizedStart = normalizeDateParam(params.start, DEFAULT_START)
  const normalizedEnd = normalizeDateParam(params.end, DEFAULT_END)
  const start = normalizedStart <= normalizedEnd ? normalizedStart : normalizedEnd
  const end = normalizedStart <= normalizedEnd ? normalizedEnd : normalizedStart
  const priorRange = makePriorRange(start, end)

  const [
    availableRow,
    overviewRow,
    priorRow,
    dailyTrendRows,
    topUsersRows,
    topToolsRows,
    topVinsRows,
    topAccountVinsRows,
    topZipPostalRows,
    stateZipRows,
    zip5TotalRow,
    ltlRetailersRows,
    ltlClickedRows,
    qualityRow,
    unmappedRows,
    zipGeoLookup,
    fourWeekLift,
  ] = await Promise.all([
    dbQuery<{ available_start: string | null; available_end: string | null }>(
      `
        SELECT
          MIN(date_utc)::text AS available_start,
          MAX(date_utc)::text AS available_end
        FROM rs2_scan
      `
    ),
    dbQuery<{ scans: string; unique_users: string; unique_vins: string }>(
      `
        SELECT
          COUNT(*)::bigint AS scans,
          COUNT(DISTINCT NULLIF(user_id, ''))::bigint AS unique_users,
          COUNT(DISTINCT NULLIF(vin_final, ''))::bigint AS unique_vins
        FROM rs2_scan
        WHERE date_utc BETWEEN $1::date AND $2::date
      `,
      [start, end]
    ),
    dbQuery<{ scans: string }>(
      `
        SELECT COUNT(*)::bigint AS scans
        FROM rs2_scan
        WHERE date_utc BETWEEN $1::date AND $2::date
      `,
      [priorRange.start, priorRange.end]
    ),
    dbQuery<{ date_pt: string; scans: string; unique_users: string; unique_vins: string }>(
      `
        SELECT
          date_utc::text AS date_pt,
          COUNT(*)::bigint AS scans,
          COUNT(DISTINCT NULLIF(user_id, ''))::bigint AS unique_users,
          COUNT(DISTINCT NULLIF(vin_final, ''))::bigint AS unique_vins
        FROM rs2_scan
        WHERE date_utc BETWEEN $1::date AND $2::date
        GROUP BY date_utc
        ORDER BY date_utc
      `,
      [start, end]
    ),
    dbQuery<{ user_id: string; user_email: string; same_day_reports: string; inspections: string }>(
      `
        WITH scoped AS (
          SELECT user_id, email, date_utc
          FROM rs2_scan
          WHERE date_utc BETWEEN $1::date AND $2::date
        ),
        user_counts AS (
          SELECT user_id, COUNT(*)::bigint AS inspections
          FROM scoped
          WHERE user_id <> ''
          GROUP BY user_id
        ),
        user_day AS (
          SELECT user_id, date_utc, COUNT(*)::bigint AS day_count
          FROM scoped
          WHERE user_id <> ''
          GROUP BY user_id, date_utc
        ),
        user_day_max AS (
          SELECT user_id, MAX(day_count)::bigint AS same_day_reports
          FROM user_day
          GROUP BY user_id
        ),
        user_email_rank AS (
          SELECT
            user_id,
            email,
            COUNT(*)::bigint AS email_count,
            ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY COUNT(*) DESC, email) AS rn
          FROM scoped
          WHERE user_id <> '' AND email <> ''
          GROUP BY user_id, email
        )
        SELECT
          uc.user_id,
          COALESCE(uer.email, '') AS user_email,
          COALESCE(udm.same_day_reports, 0)::bigint AS same_day_reports,
          uc.inspections
        FROM user_counts uc
        LEFT JOIN user_day_max udm ON udm.user_id = uc.user_id
        LEFT JOIN user_email_rank uer ON uer.user_id = uc.user_id AND uer.rn = 1
        ORDER BY uc.inspections DESC, uc.user_id
        LIMIT $3
      `,
      [start, end, topN]
    ),
    dbQuery<{ tool_name: string; inspections: string; abs_parts: string; srs_parts: string; cel_parts: string }>(
      `
        SELECT
          s.tool_name,
          COUNT(*)::bigint AS inspections,
          SUM(COALESCE(p.abs_parts, 0))::bigint AS abs_parts,
          SUM(COALESCE(p.srs_parts, 0))::bigint AS srs_parts,
          SUM(COALESCE(p.cel_parts, 0))::bigint AS cel_parts
        FROM rs2_scan s
        LEFT JOIN rs2_report_part_counts p ON p.report_id = s.report_id
        WHERE s.date_utc BETWEEN $1::date AND $2::date
          AND s.tool_name <> ''
          AND s.tool_name <> 'UNMAPPED'
        GROUP BY s.tool_name
        ORDER BY inspections DESC, s.tool_name
        LIMIT $3
      `,
      [start, end, topN]
    ),
    dbQuery<{ vin: string; vehicle_year: string; vehicle_make: string; vehicle_model: string; inspections: string }>(
      `
        WITH scoped AS (
          SELECT vin_final, vehicle_year, vehicle_make, vehicle_model
          FROM rs2_scan
          WHERE date_utc BETWEEN $1::date AND $2::date
            AND vin_final <> ''
        ),
        vin_counts AS (
          SELECT vin_final AS vin, COUNT(*)::bigint AS inspections
          FROM scoped
          GROUP BY vin_final
        ),
        vin_meta AS (
          SELECT
            vin_final AS vin,
            vehicle_year,
            vehicle_make,
            vehicle_model,
            ROW_NUMBER() OVER (
              PARTITION BY vin_final
              ORDER BY COUNT(*) DESC, vehicle_year, vehicle_make, vehicle_model
            ) AS rn
          FROM scoped
          GROUP BY vin_final, vehicle_year, vehicle_make, vehicle_model
        )
        SELECT
          vc.vin,
          COALESCE(vm.vehicle_year, '') AS vehicle_year,
          COALESCE(vm.vehicle_make, '') AS vehicle_make,
          COALESCE(vm.vehicle_model, '') AS vehicle_model,
          vc.inspections
        FROM vin_counts vc
        LEFT JOIN vin_meta vm ON vm.vin = vc.vin AND vm.rn = 1
        ORDER BY vc.inspections DESC, vc.vin
        LIMIT $3
      `,
      [start, end, topN]
    ),
    dbQuery<{
      account_id: string
      user_email: string
      vin: string
      vehicle_year: string
      vehicle_make: string
      vehicle_model: string
      inspections: string
    }>(
      `
        WITH scoped AS (
          SELECT account_id, vin_final, email, vehicle_year, vehicle_make, vehicle_model
          FROM rs2_scan
          WHERE date_utc BETWEEN $1::date AND $2::date
            AND account_id <> ''
            AND vin_final <> ''
        ),
        pair_counts AS (
          SELECT account_id, vin_final, COUNT(*)::bigint AS inspections
          FROM scoped
          GROUP BY account_id, vin_final
        ),
        email_rank AS (
          SELECT
            account_id,
            vin_final,
            email,
            ROW_NUMBER() OVER (
              PARTITION BY account_id, vin_final
              ORDER BY COUNT(*) DESC, email
            ) AS rn
          FROM scoped
          WHERE email <> ''
          GROUP BY account_id, vin_final, email
        ),
        meta_rank AS (
          SELECT
            account_id,
            vin_final,
            vehicle_year,
            vehicle_make,
            vehicle_model,
            ROW_NUMBER() OVER (
              PARTITION BY account_id, vin_final
              ORDER BY COUNT(*) DESC, vehicle_year, vehicle_make, vehicle_model
            ) AS rn
          FROM scoped
          GROUP BY account_id, vin_final, vehicle_year, vehicle_make, vehicle_model
        )
        SELECT
          pc.account_id,
          COALESCE(er.email, '') AS user_email,
          pc.vin_final AS vin,
          COALESCE(mr.vehicle_year, '') AS vehicle_year,
          COALESCE(mr.vehicle_make, '') AS vehicle_make,
          COALESCE(mr.vehicle_model, '') AS vehicle_model,
          pc.inspections
        FROM pair_counts pc
        LEFT JOIN email_rank er ON er.account_id = pc.account_id AND er.vin_final = pc.vin_final AND er.rn = 1
        LEFT JOIN meta_rank mr ON mr.account_id = pc.account_id AND mr.vin_final = pc.vin_final AND mr.rn = 1
        ORDER BY pc.inspections DESC, pc.account_id, pc.vin_final
        LIMIT $3
      `,
      [start, end, topN]
    ),
    dbQuery<{ zip_postal: string; city: string; state: string; zip5: string; inspections: string }>(
      `
        WITH scoped AS (
          SELECT zip_postal, zip5, city, state
          FROM rs2_scan
          WHERE date_utc BETWEEN $1::date AND $2::date
            AND zip_postal <> ''
        ),
        zip_counts AS (
          SELECT
            zip_postal,
            COALESCE(MAX(NULLIF(zip5, '')), '') AS zip5,
            COUNT(*)::bigint AS inspections
          FROM scoped
          GROUP BY zip_postal
        ),
        zip_meta AS (
          SELECT
            zip_postal,
            zip5,
            city,
            state,
            ROW_NUMBER() OVER (
              PARTITION BY zip_postal
              ORDER BY
                COUNT(*) DESC,
                CASE WHEN city <> '' THEN 0 ELSE 1 END,
                CASE WHEN state <> '' THEN 0 ELSE 1 END,
                CASE WHEN zip5 <> '' THEN 0 ELSE 1 END,
                zip5,
                city,
                state
            ) AS rn
          FROM scoped
          GROUP BY zip_postal, zip5, city, state
        )
        SELECT
          zc.zip_postal,
          COALESCE(NULLIF(zm.city, ''), zl.city, '') AS city,
          COALESCE(NULLIF(zm.state, ''), zl.state, '') AS state,
          zc.zip5,
          zc.inspections
        FROM zip_counts zc
        LEFT JOIN zip_meta zm ON zm.zip_postal = zc.zip_postal AND zm.rn = 1
        LEFT JOIN rs2_zip_lookup zl ON zl.zip5 = zc.zip5
        ORDER BY zc.inspections DESC, zc.zip_postal
        LIMIT $3
      `,
      [start, end, topN]
    ),
    dbQuery<{ state: string; zip5: string; inspections: string }>(
      `
        SELECT
          COALESCE(NULLIF(state, ''), '') AS state,
          zip5,
          COUNT(*)::bigint AS inspections
        FROM rs2_scan
        WHERE date_utc BETWEEN $1::date AND $2::date
          AND zip5 <> ''
        GROUP BY state, zip5
        ORDER BY inspections DESC, state, zip5
      `,
      [start, end]
    ),
    dbQuery<{ points_total_zip5: string }>(
      `
        SELECT COUNT(DISTINCT zip5)::bigint AS points_total_zip5
        FROM rs2_scan
        WHERE date_utc BETWEEN $1::date AND $2::date
          AND zip5 <> ''
      `,
      [start, end]
    ),
    dbQuery<{ buy_from: string; inspections: string; total_buy_clicks: string }>(
      `
        SELECT
          buy_from,
          COUNT(DISTINCT report_id)::bigint AS inspections,
          COUNT(*)::bigint AS total_buy_clicks
        FROM rs2_buynow_click
        WHERE date_utc BETWEEN $1::date AND $2::date
          AND buy_from <> ''
        GROUP BY buy_from
        ORDER BY inspections DESC, buy_from
        LIMIT $3
      `,
      [start, end, topN]
    ),
    dbQuery<{ account_id: string; user_email: string; inspections: string; total_buy_clicks: string }>(
      `
        WITH scoped AS (
          SELECT report_id, clicked_account_id, email
          FROM rs2_buynow_click
          WHERE date_utc BETWEEN $1::date AND $2::date
            AND clicked_account_id <> ''
        ),
        account_counts AS (
          SELECT
            clicked_account_id AS account_id,
            COUNT(DISTINCT report_id)::bigint AS inspections,
            COUNT(*)::bigint AS total_buy_clicks
          FROM scoped
          GROUP BY clicked_account_id
        ),
        email_rank AS (
          SELECT
            clicked_account_id AS account_id,
            email,
            ROW_NUMBER() OVER (
              PARTITION BY clicked_account_id
              ORDER BY COUNT(*) DESC, email
            ) AS rn
          FROM scoped
          WHERE email <> ''
          GROUP BY clicked_account_id, email
        )
        SELECT
          ac.account_id,
          COALESCE(er.email, '') AS user_email,
          ac.inspections,
          ac.total_buy_clicks
        FROM account_counts ac
        LEFT JOIN email_rank er ON er.account_id = ac.account_id AND er.rn = 1
        ORDER BY ac.inspections DESC, ac.account_id
        LIMIT $3
      `,
      [start, end, topN]
    ),
    dbQuery<{ generated_at_utc: string; mapping_fallback_usb937: string | null; ambiguous_mapping_keys: string }>(
      `
        SELECT
          generated_at_utc::text AS generated_at_utc,
          mapping_fallback_usb937,
          ambiguous_mapping_keys::text AS ambiguous_mapping_keys
        FROM rs2_quality
        ORDER BY id DESC
        LIMIT 1
      `
    ),
    dbQuery<{ usb_product_id: string; rows: string }>(
      `
        SELECT
          COALESCE(usb_product_id, '') AS usb_product_id,
          COUNT(*)::bigint AS rows
        FROM rs2_scan
        WHERE tool_name = 'UNMAPPED'
        GROUP BY usb_product_id
        ORDER BY rows DESC, usb_product_id
        LIMIT 20
      `
    ),
    getZipGeoLookup(),
    loadFourWeekLift(),
  ])

  const available = availableRow[0]
  const overview = overviewRow[0]
  const priorScans = toInt(priorRow[0]?.scans)
  const hasPriorData = priorScans > 0
  const scans = toInt(overview?.scans)
  const scanLift = hasPriorData ? ((scans - priorScans) / priorScans) * 100 : null

  const topZipPostal = topZipPostalRows.map((row) => {
    const zip5 = row.zip5 || extractZip5(row.zip_postal)
    const lookup = zip5 ? zipGeoLookup.get(zip5) : undefined
    return {
      zip_postal: row.zip_postal,
      city: row.city || lookup?.city || "",
      state: (row.state || lookup?.state || "").toUpperCase(),
      zip5,
      inspections: toInt(row.inspections),
    }
  })

  const stateTotals = new Map<string, number>()
  const stateCityTotals = new Map<string, Map<string, number>>()
  const zip5WithLookup = new Set<string>()

  for (const row of stateZipRows) {
    const zip5 = row.zip5 || ""
    const lookup = zip5 ? zipGeoLookup.get(zip5) : undefined
    const stateCode = (row.state || lookup?.state || "").toUpperCase()
    if (!stateCode || !US_STATE_CODES.has(stateCode)) continue

    const inspections = toInt(row.inspections)
    stateTotals.set(stateCode, (stateTotals.get(stateCode) ?? 0) + inspections)

    if (!lookup) continue
    zip5WithLookup.add(zip5)
    if (!lookup.city) continue

    const cityTotals = stateCityTotals.get(stateCode) ?? new Map<string, number>()
    cityTotals.set(lookup.city, (cityTotals.get(lookup.city) ?? 0) + inspections)
    stateCityTotals.set(stateCode, cityTotals)
  }

  const stateInspections = Array.from(stateTotals.entries())
    .map(([state, inspections]) => ({
      state,
      state_name: US_STATE_NAME_BY_CODE[state] ?? state,
      inspections,
      top_cities: Array.from(stateCityTotals.get(state)?.entries() ?? [])
        .sort((a, b) => (b[1] === a[1] ? a[0].localeCompare(b[0]) : b[1] - a[1]))
        .slice(0, 3)
        .map(([city, cityInspections]) => ({ city, inspections: cityInspections })),
    }))
    .sort((a, b) => (b.inspections === a.inspections ? a.state.localeCompare(b.state) : b.inspections - a.inspections))

  const quality = qualityRow[0]
  const generatedAt = quality?.generated_at_utc ?? new Date().toISOString()

  return {
    meta: {
      start,
      end,
      topN,
      available_start: available?.available_start ?? start,
      available_end: available?.available_end ?? end,
      prior_equal_range: priorRange,
      has_prior_data: hasPriorData,
    },
    overview: {
      scans,
      unique_users: toInt(overview?.unique_users),
      unique_vins: toInt(overview?.unique_vins),
      prior_scans: hasPriorData ? priorScans : null,
      scan_lift_pct_vs_prior: scanLift,
    },
    dailyTrend: dailyTrendRows.map((row) => ({
      date_pt: row.date_pt,
      scans: toInt(row.scans),
      unique_users: toInt(row.unique_users),
      unique_vins: toInt(row.unique_vins),
    })),
    topUsers: topUsersRows.map((row) => ({
      user_id: row.user_id,
      user_email: row.user_email,
      same_day_reports: toInt(row.same_day_reports),
      inspections: toInt(row.inspections),
    })),
    topTools: topToolsRows.map((row) => ({
      tool_name: row.tool_name,
      inspections: toInt(row.inspections),
      abs_parts: toInt(row.abs_parts),
      srs_parts: toInt(row.srs_parts),
      cel_parts: toInt(row.cel_parts),
    })),
    topVins: topVinsRows.map((row) => ({
      vin: row.vin,
      vehicle_year: row.vehicle_year,
      vehicle_make: row.vehicle_make,
      vehicle_model: row.vehicle_model,
      vehicle_engine: "",
      inspections: toInt(row.inspections),
    })),
    topAccountVins: topAccountVinsRows.map((row) => ({
      account_id: row.account_id,
      user_email: row.user_email,
      vin: row.vin,
      vehicle_year: row.vehicle_year,
      vehicle_make: row.vehicle_make,
      vehicle_model: row.vehicle_model,
      vehicle_engine: "",
      inspections: toInt(row.inspections),
    })),
    topZipPostal,
    ltlRetailers: ltlRetailersRows.map((row) => ({
      buy_from: row.buy_from,
      inspections: toInt(row.inspections),
      total_buy_clicks: toInt(row.total_buy_clicks),
    })),
    ltlClickedAccounts: ltlClickedRows.map((row) => ({
      account_id: row.account_id,
      user_email: row.user_email,
      inspections: toInt(row.inspections),
      total_buy_clicks: toInt(row.total_buy_clicks),
    })),
    stateInspections,
    geoStateMeta: {
      states_with_inspections: stateInspections.length,
      zip5_with_lookup: zip5WithLookup.size,
      points_total_zip5: toInt(zip5TotalRow[0]?.points_total_zip5),
      missing_geo_lookup: zip5WithLookup.size === 0,
    },
    fourWeekLift,
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
      generated_at_utc: generatedAt,
      mapping_fallback_usb937: quality?.mapping_fallback_usb937 ?? null,
      ambiguous_mapping_keys: toInt(quality?.ambiguous_mapping_keys),
      top_unmapped_usb: unmappedRows.map((row) => ({
        usb_product_id: row.usb_product_id,
        rows: toInt(row.rows),
      })),
    },
  }
}

export function getNormalizedParams(searchParams: URLSearchParams): DashboardParams {
  const start = normalizeDateParam(searchParams.get("start"), DEFAULT_START)
  const end = normalizeDateParam(searchParams.get("end"), DEFAULT_END)
  const topN = clampTopN(Number.parseInt(searchParams.get("topN") ?? "100", 10))
  if (start <= end) return { start, end, topN }
  return { start: end, end: start, topN }
}
