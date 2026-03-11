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
    const message = error instanceof Error ? error.message : "Unexpected error while loading RS2 dashboard data"
    return NextResponse.json({ error: message }, { status: 500 })
  }
}
