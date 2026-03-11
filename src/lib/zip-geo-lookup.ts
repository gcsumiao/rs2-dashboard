import { readFile } from "node:fs/promises"
import path from "node:path"

export type ZipGeoEntry = {
  zip5: string
  city: string
  state: string
  stateName: string
  lat: number
  lng: number
}

const ZIP5_RE = /^\d{5}$/
const ZIPMAP_PATH = path.join(process.cwd(), "..", "raw_data", "zipmap.csv")

let zipGeoLookupPromise: Promise<Map<string, ZipGeoEntry>> | null = null

function parseCsvRow(row: string): string[] {
  const cells: string[] = []
  let current = ""
  let inQuotes = false

  for (let index = 0; index < row.length; index += 1) {
    const char = row[index]

    if (char === '"') {
      if (inQuotes && row[index + 1] === '"') {
        current += '"'
        index += 1
        continue
      }
      inQuotes = !inQuotes
      continue
    }

    if (char === "," && !inQuotes) {
      cells.push(current)
      current = ""
      continue
    }

    current += char
  }

  cells.push(current)
  return cells
}

function toFloat(value: string): number {
  const parsed = Number.parseFloat(value)
  return Number.isFinite(parsed) ? parsed : 0
}

function normalizeZip5(value: string): string {
  const zip5 = value.trim().padStart(5, "0")
  return ZIP5_RE.test(zip5) ? zip5 : ""
}

async function loadZipGeoLookup(): Promise<Map<string, ZipGeoEntry>> {
  try {
    const csv = await readFile(ZIPMAP_PATH, "utf8")
    const rows = csv.split(/\r?\n/)
    if (rows.length === 0) {
      return new Map()
    }

    const header = parseCsvRow(rows[0])
    const zipIndex = header.indexOf("zip")
    const latIndex = header.indexOf("lat")
    const lngIndex = header.indexOf("lng")
    const cityIndex = header.indexOf("city")
    const stateIndex = header.indexOf("state_id")
    const stateNameIndex = header.indexOf("state_name")

    if ([zipIndex, latIndex, lngIndex, cityIndex, stateIndex, stateNameIndex].some((index) => index < 0)) {
      return new Map()
    }

    const lookup = new Map<string, ZipGeoEntry>()

    for (const row of rows.slice(1)) {
      if (!row.trim()) continue
      const cells = parseCsvRow(row)
      const zip5 = normalizeZip5(cells[zipIndex] ?? "")
      if (!zip5) continue

      const nextEntry: ZipGeoEntry = {
        zip5,
        city: (cells[cityIndex] ?? "").trim(),
        state: (cells[stateIndex] ?? "").trim().toUpperCase(),
        stateName: (cells[stateNameIndex] ?? "").trim(),
        lat: toFloat(cells[latIndex] ?? ""),
        lng: toFloat(cells[lngIndex] ?? ""),
      }

      const previous = lookup.get(zip5)
      if (!previous || (!previous.city && nextEntry.city) || (!previous.stateName && nextEntry.stateName)) {
        lookup.set(zip5, nextEntry)
      }
    }

    return lookup
  } catch {
    return new Map()
  }
}

export async function getZipGeoLookup(): Promise<Map<string, ZipGeoEntry>> {
  if (!zipGeoLookupPromise) {
    zipGeoLookupPromise = loadZipGeoLookup()
  }
  return zipGeoLookupPromise
}

export function extractZip5(zipPostal: string): string {
  const digitsOnly = zipPostal.replace(/\D/g, "")
  const zip5 = digitsOnly.slice(0, 5)
  return ZIP5_RE.test(zip5) ? zip5 : ""
}
