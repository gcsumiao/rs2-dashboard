import { NextResponse } from "next/server"
import { buildRs2DashboardResponse, getNormalizedParams } from "@/lib/rs2-dashboard-data"

export const dynamic = "force-dynamic"

export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url)
    const params = getNormalizedParams(searchParams)
    const payload = await buildRs2DashboardResponse(params)
    return NextResponse.json(payload)
  } catch (error) {
    const raw = error instanceof Error ? error.message : "Unexpected error while loading RS2 dashboard data"
    let message = raw
    if (
      raw.includes("connection to server") ||
      raw.includes("ECONNREFUSED") ||
      raw.includes("database") && raw.includes("does not exist")
    ) {
      message = "Database is not ready. Run `npm run db:up` then `npm run db:load:rs2`."
    } else if (raw.includes("role") && raw.includes("does not exist")) {
      message = "Database user is not initialized. Run `npm run db:load:rs2`."
    } else if (raw.includes("relation") && raw.includes("does not exist")) {
      message = "RS2 tables are not initialized. Run `npm run db:load:rs2`."
    }
    return NextResponse.json({ error: message }, { status: 500 })
  }
}
