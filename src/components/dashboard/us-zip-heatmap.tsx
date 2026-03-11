"use client"

import { useEffect, useMemo, useState } from "react"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"

type StateInspection = {
  state: string
  state_name: string
  inspections: number
  top_cities: Array<{ city: string; inspections: number }>
}

type StateShape = {
  state: string
  state_name: string
  path: string
  label: [number, number] | null
}

type StateMapAsset = {
  viewBox: [number, number, number, number]
  states: StateShape[]
}

type HoveredState = {
  details: StateInspection
  x: number
  y: number
}

type Props = {
  states: StateInspection[]
  title?: string
  subtitle?: string
  missingLookup?: boolean
}

const COLOR_SCALE = ["#dfe7f3", "#bdd0ef", "#91b2e6", "#5f89d9", "#325fc1", "#17336d"]
const SMALL_LABEL_STATES = new Set(["CT", "DC", "DE", "HI", "MA", "MD", "NH", "NJ", "RI", "VT"])

function clamp(value: number, min: number, max: number): number {
  return Math.min(Math.max(value, min), max)
}

function buildLegendBreaks(values: number[]): number[] {
  if (values.length === 0) return [0, 0, 0, 0, 0]
  const sorted = [...values].sort((a, b) => a - b)
  const pick = (ratio: number) => sorted[Math.min(sorted.length - 1, Math.floor((sorted.length - 1) * ratio))]
  return [pick(0), pick(0.25), pick(0.5), pick(0.75), sorted[sorted.length - 1]]
}

function colorForCount(count: number, breaks: number[]): string {
  if (count <= 0) return "#edf2f7"
  if (count <= breaks[0]) return COLOR_SCALE[1]
  if (count <= breaks[1]) return COLOR_SCALE[2]
  if (count <= breaks[2]) return COLOR_SCALE[3]
  if (count <= breaks[3]) return COLOR_SCALE[4]
  return COLOR_SCALE[5]
}

function formatRangeLabel(min: number, max: number): string {
  if (min === max) return min.toLocaleString()
  return `${min.toLocaleString()}-${max.toLocaleString()}`
}

export function UsZipHeatmap({
  states,
  title = "U.S. State Inspection Map",
  subtitle = "State totals by inspection volume",
  missingLookup = false,
}: Props) {
  const [asset, setAsset] = useState<StateMapAsset | null>(null)
  const [mapError, setMapError] = useState<string | null>(null)
  const [hoveredState, setHoveredState] = useState<HoveredState | null>(null)

  useEffect(() => {
    let cancelled = false

    void fetch("/data/us-state-map.json", { cache: "force-cache" })
      .then((response) => {
        if (!response.ok) {
          throw new Error(`HTTP ${response.status}`)
        }
        return response.json() as Promise<StateMapAsset>
      })
      .then((payload) => {
        if (!cancelled) {
          setAsset(payload)
        }
      })
      .catch(() => {
        if (!cancelled) {
          setMapError("Failed to load U.S. state geometry")
        }
      })

    return () => {
      cancelled = true
    }
  }, [])

  const statesByCode = useMemo(() => new Map(states.map((entry) => [entry.state, entry])), [states])

  const summary = useMemo(() => {
    const values = states.map((entry) => entry.inspections).filter((value) => value > 0)
    const breaks = buildLegendBreaks(values)
    const totalInspections = states.reduce((sum, entry) => sum + entry.inspections, 0)
    const legend = [
      { color: COLOR_SCALE[1], label: formatRangeLabel(1, Math.max(1, breaks[0])) },
      { color: COLOR_SCALE[2], label: formatRangeLabel(Math.max(1, breaks[0] + 1), Math.max(1, breaks[1])) },
      { color: COLOR_SCALE[3], label: formatRangeLabel(Math.max(1, breaks[1] + 1), Math.max(1, breaks[2])) },
      { color: COLOR_SCALE[4], label: formatRangeLabel(Math.max(1, breaks[2] + 1), Math.max(1, breaks[3])) },
      { color: COLOR_SCALE[5], label: `${Math.max(1, breaks[3] + 1).toLocaleString()}+` },
    ]

    return {
      activeStates: states.length,
      breaks,
      legend,
      totalInspections,
    }
  }, [states])

  const showFallback = !asset || Boolean(mapError) || states.length === 0

  return (
    <Card className="border border-border bg-card">
      <CardHeader className="pb-2">
        <CardTitle className="text-base font-medium">{title}</CardTitle>
        <p className="text-xs text-muted-foreground">{subtitle}</p>
      </CardHeader>
      <CardContent>
        {showFallback ? (
          <div className="flex min-h-[480px] items-center justify-center rounded-2xl border border-dashed border-border bg-muted/40 p-6 text-center text-sm text-muted-foreground">
            {mapError
              ? "State map failed to initialize. Check the local geometry asset and retry."
              : missingLookup
                ? "No state totals could be resolved from the available ZIP lookup for the selected range."
                : "No U.S. state inspection totals exist in the selected date range."}
          </div>
        ) : (
          <div className="relative overflow-hidden rounded-2xl border border-border bg-[radial-gradient(circle_at_top,_rgba(96,165,250,0.14),_transparent_34%),linear-gradient(180deg,_#fbfdff_0%,_#eef4fb_100%)]">
            <div className="pointer-events-none absolute left-4 top-4 z-10 rounded-2xl border border-slate-200/80 bg-white/88 px-4 py-3 shadow-sm backdrop-blur">
              <p className="text-[11px] uppercase tracking-[0.2em] text-slate-500">National View</p>
              <p className="text-2xl font-semibold text-slate-950">{summary.totalInspections.toLocaleString()}</p>
              <p className="text-xs text-slate-600">{summary.activeStates} states with inspections</p>
            </div>

            <div className="pointer-events-none absolute bottom-4 left-4 z-10 rounded-2xl border border-slate-200/80 bg-white/88 px-4 py-3 shadow-sm backdrop-blur">
              <p className="mb-2 text-[11px] uppercase tracking-[0.2em] text-slate-500">Inspection Scale</p>
              <div className="space-y-2">
                {summary.legend.map((item) => (
                  <div key={`${item.color}-${item.label}`} className="flex items-center gap-2 text-[11px] text-slate-700">
                    <span className="h-3 w-3 rounded-sm border border-slate-200" style={{ backgroundColor: item.color }} />
                    <span>{item.label}</span>
                  </div>
                ))}
                <div className="flex items-center gap-2 text-[11px] text-slate-500">
                  <span className="h-3 w-3 rounded-sm border border-slate-200 bg-[#edf2f7]" />
                  <span>No inspections</span>
                </div>
              </div>
            </div>

            <div className="relative min-h-[500px] p-4 md:min-h-[560px]">
              <svg
                viewBox={asset.viewBox.join(" ")}
                className="h-full w-full"
                role="img"
                aria-label="United States state inspection choropleth"
              >
                <g>
                  {asset.states.map((shape) => {
                    const details = statesByCode.get(shape.state) ?? {
                      state: shape.state,
                      state_name: shape.state_name,
                      inspections: 0,
                      top_cities: [],
                    }

                    return (
                      <path
                        key={shape.state}
                        d={shape.path}
                        data-state-path={shape.state}
                        fill={colorForCount(details.inspections, summary.breaks)}
                        stroke={hoveredState?.details.state === shape.state ? "#0f172a" : "rgba(15, 23, 42, 0.18)"}
                        strokeWidth={hoveredState?.details.state === shape.state ? 2.2 : 1.2}
                        className="cursor-pointer transition-all duration-150 ease-out"
                        onMouseMove={(event) => {
                          const bounds = event.currentTarget.ownerSVGElement?.getBoundingClientRect()
                          if (!bounds) return

                          setHoveredState({
                            details,
                            x: clamp(event.clientX - bounds.left + 16, 12, bounds.width - 220),
                            y: clamp(event.clientY - bounds.top + 16, 12, bounds.height - 132),
                          })
                        }}
                        onMouseLeave={() => setHoveredState((current) => (current?.details.state === shape.state ? null : current))}
                      />
                    )
                  })}
                </g>

                <g className="pointer-events-none">
                  {asset.states.map((shape) => {
                    if (!shape.label || SMALL_LABEL_STATES.has(shape.state)) {
                      return null
                    }

                    const details = statesByCode.get(shape.state)
                    const [x, y] = shape.label
                    return (
                      <text
                        key={`label-${shape.state}`}
                        x={x}
                        y={y}
                        textAnchor="middle"
                        className="fill-slate-700 text-[10px] font-semibold tracking-[0.18em]"
                        opacity={details ? 0.9 : 0.55}
                      >
                        {shape.state}
                      </text>
                    )
                  })}
                </g>
              </svg>

              {hoveredState && (
                <div
                  data-state-tooltip
                  className="pointer-events-none absolute z-20 max-w-[220px] rounded-2xl border border-slate-200/90 bg-white/96 px-4 py-3 shadow-[0_18px_45px_rgba(15,23,42,0.16)] backdrop-blur"
                  style={{ left: hoveredState.x, top: hoveredState.y }}
                >
                  <p className="text-[11px] uppercase tracking-[0.18em] text-slate-500">{hoveredState.details.state}</p>
                  <p className="text-base font-semibold text-slate-950">{hoveredState.details.state_name}</p>
                  <p className="mt-1 text-sm text-slate-700">
                    {hoveredState.details.inspections.toLocaleString()} inspections
                  </p>
                  <p className="mt-3 text-[11px] uppercase tracking-[0.18em] text-slate-500">Top Cities</p>
                  {hoveredState.details.top_cities.length > 0 ? (
                    <div className="mt-1 space-y-1 text-xs text-slate-700">
                      {hoveredState.details.top_cities.map((city) => (
                        <p key={`${hoveredState.details.state}-${city.city}`}>
                          {city.city}: {city.inspections.toLocaleString()}
                        </p>
                      ))}
                    </div>
                  ) : (
                    <p className="mt-1 text-xs text-slate-500">No city annotations available</p>
                  )}
                </div>
              )}
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  )
}
