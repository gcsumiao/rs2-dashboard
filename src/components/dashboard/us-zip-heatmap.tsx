"use client"

import { useEffect, useRef, useState } from "react"
import "maplibre-gl/dist/maplibre-gl.css"
import type { Map as MapLibreMap, Popup as MapLibrePopup, StyleSpecification } from "maplibre-gl"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"

type HeatPoint = {
  zip_postal: string
  zip5: string
  city: string
  lat: number
  lng: number
  state: string
  scans: number
  percentile: number
}

type Props = {
  points: HeatPoint[]
  title?: string
  subtitle?: string
  missingLookup?: boolean
}

const MAP_STYLE: StyleSpecification = {
  version: 8,
  name: "RS2 Minimal",
  sources: {},
  layers: [
    {
      id: "background",
      type: "background",
      paint: {
        "background-color": "#f8fafc",
      },
    },
  ],
}

function centerForUs(): [number, number] {
  return [-96, 38.5]
}

export function UsZipHeatmap({
  points,
  title = "U.S. ZIP Scan Heatmap",
  subtitle = "Heat intensity by inspection volume",
  missingLookup = false,
}: Props) {
  const mapContainer = useRef<HTMLDivElement | null>(null)
  const [mapError, setMapError] = useState<string | null>(null)

  useEffect(() => {
    if (!mapContainer.current) return
    if (points.length === 0) return

    let cancelled = false
    let map: MapLibreMap | null = null
    let popup: MapLibrePopup | null = null

    void import("maplibre-gl")
      .then((module) => {
        if (cancelled || !mapContainer.current) return
        const maplibregl = module.default
        map = new maplibregl.Map({
          container: mapContainer.current,
          style: MAP_STYLE,
          center: centerForUs(),
          zoom: 3.2,
          maxZoom: 12,
          minZoom: 2,
        })

        map.addControl(new maplibregl.NavigationControl({ visualizePitch: false }), "top-right")

        map.on("load", () => {
          map?.fitBounds(
            [
              [-125, 24],
              [-66, 50],
            ],
            { padding: 12, duration: 0 }
          )

          const features = points.map((point) => ({
            type: "Feature" as const,
            geometry: {
              type: "Point" as const,
              coordinates: [point.lng, point.lat] as [number, number],
            },
            properties: {
              zip5: point.zip5,
              zip_postal: point.zip_postal,
              city: point.city,
              state: point.state,
              scans: point.scans,
              percentile: point.percentile,
            },
          }))

          map?.addSource("zip-points", {
            type: "geojson",
            data: {
              type: "FeatureCollection",
              features,
            },
          })

          map?.addLayer({
            id: "zip-heat-layer",
            type: "heatmap",
            source: "zip-points",
            minzoom: 2,
            maxzoom: 11,
            paint: {
              "heatmap-weight": ["interpolate", ["linear"], ["get", "scans"], 1, 0.2, 1000, 1],
              "heatmap-intensity": ["interpolate", ["linear"], ["zoom"], 2, 1, 7, 2.1],
              "heatmap-color": [
                "interpolate",
                ["linear"],
                ["heatmap-density"],
                0,
                "rgba(11, 25, 47, 0.05)",
                0.18,
                "rgba(37, 99, 235, 0.55)",
                0.45,
                "rgba(16, 185, 129, 0.7)",
                0.7,
                "rgba(245, 158, 11, 0.82)",
                1,
                "rgba(220, 38, 38, 0.95)",
              ],
              "heatmap-radius": ["interpolate", ["linear"], ["zoom"], 2, 14, 7, 38, 10, 52],
              "heatmap-opacity": ["interpolate", ["linear"], ["zoom"], 2, 0.95, 10, 0.55],
            },
          })

          map?.addLayer({
            id: "zip-visible-points",
            type: "circle",
            source: "zip-points",
            minzoom: 2,
            paint: {
              "circle-radius": ["interpolate", ["linear"], ["zoom"], 2, 1.6, 6, 3.2, 10, 4.8],
              "circle-color": [
                "interpolate",
                ["linear"],
                ["get", "percentile"],
                0,
                "#60a5fa",
                50,
                "#22c55e",
                80,
                "#f59e0b",
                100,
                "#ef4444",
              ],
              "circle-stroke-color": "#f8fafc",
              "circle-stroke-width": 0.4,
              "circle-opacity": 0.65,
            },
          })

          map?.on("mousemove", "zip-visible-points", (event) => {
            const feature = event.features?.[0]
            if (!feature?.properties) return
            const p = feature.properties as {
              zip5?: string
              zip_postal?: string
              city?: string
              state?: string
              scans?: number | string
              percentile?: number | string
            }
            const scans = Number(p.scans ?? 0)
            const percentile = Number(p.percentile ?? 0)
            const html = `
              <div style="font-size:12px;line-height:1.4">
                <div style="font-weight:600">ZIP ${p.zip_postal ?? p.zip5 ?? "n/a"} ${p.state ? `(${p.state})` : ""}</div>
                <div>City: ${p.city ?? "n/a"}</div>
                <div>Inspections: ${scans.toLocaleString()}</div>
                <div>Percentile: ${percentile}%</div>
              </div>
            `
            if (!popup) {
              popup = new maplibregl.Popup({
                closeButton: false,
                closeOnClick: false,
                offset: 12,
              })
            }
            popup.setLngLat(event.lngLat).setHTML(html).addTo(map as MapLibreMap)
          })

          map?.on("mouseleave", "zip-visible-points", () => {
            if (popup) {
              popup.remove()
              popup = null
            }
          })
        })
      })
      .catch(() => {
        if (!cancelled) {
          setMapError("Failed to load map library")
        }
      })

    return () => {
      cancelled = true
      if (popup) {
        popup.remove()
      }
      if (map) {
        map.remove()
      }
    }
  }, [points])

  const noData = points.length === 0
  const showFallback = noData || Boolean(mapError)

  return (
    <Card className="bg-card border border-border">
      <CardHeader className="pb-2">
        <CardTitle className="text-base font-medium">{title}</CardTitle>
        <p className="text-xs text-muted-foreground">{subtitle}</p>
      </CardHeader>
      <CardContent>
        {showFallback ? (
          <div className="h-[420px] rounded-lg border border-dashed border-border bg-muted/40 p-4 text-sm text-muted-foreground">
            {mapError
              ? "Map failed to initialize. Check map style/network access and retry."
              : missingLookup
                ? "No mappable U.S. state/city points were produced for the selected range."
                : "No U.S. ZIP points in the selected date range."}
          </div>
        ) : (
          <div ref={mapContainer} className="h-[420px] w-full overflow-hidden rounded-lg border border-border" />
        )}
      </CardContent>
    </Card>
  )
}
