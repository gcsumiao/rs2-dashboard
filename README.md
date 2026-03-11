# RS2 Dashboard

Interactive RS2 inspection dashboard built on Next.js + VisActor template shell.

## What This Repo Includes

- Single-page tabbed RS2 dashboard:
  - Overview / Users / Tools / VIN / Geo / LTL / Data Gaps
- UTC-only daily bucketing (ETL, API, UI)
- API endpoint:
  - `GET /api/rs2/dashboard?start=YYYY-MM-DD&end=YYYY-MM-DD&topN=number&tz=UTC`
- Precomputed RS2 artifacts under `data/rs2/` (generated locally, not committed)

## Required Local Inputs

Place these files in the parent workspace (default expected paths):

- `/Users/sumiaoc/Desktop/RS2_dashboard/raw_data/202602/4-week lift scan.csv`
- `/Users/sumiaoc/Desktop/RS2_dashboard/raw_data/202602/fix-part raw extract.csv`
- `/Users/sumiaoc/Desktop/RS2_dashboard/raw_data/202602/BuyNow raw extract.csv`
- `/Users/sumiaoc/Desktop/RS2_dashboard/RS2tool_mapping/RS2_Tool_05122025_01.xlsx`
- `/Users/sumiaoc/Desktop/RS2_dashboard/zipmap.csv`

## Local Run

1. Install dependencies:

```bash
npm install
```

2. Build RS2 artifacts:

```bash
npm run build:rs2data
```

`build:rs2data` now skips rebuild when inputs are unchanged. To force a full rebuild:

```bash
npm run build:rs2data:force
```

3. Run app:

```bash
npm run dev
```
`npm run dev` already includes incremental `build:rs2data`.

Open [http://localhost:3000](http://localhost:3000).

## Key Data Rules Implemented

- Tool mapping key: `UsbProductId + CustomerId`
- Fallback mapping: blank customer + matching USB (ex: `937 -> 5110 / 3020RS`)
- Fix type mapping for part counts:
  - `2 = ABS`
  - `0 = CEL`
  - `3 = SRS`
- Default range: `2026-02-01` to `2026-02-28`

## Generated Artifacts

`npm run build:rs2data` writes:

- `data/rs2/snapshot_default.json`
- `data/rs2/scan_day_agg.json`
- `data/rs2/buynow_click_day_reports.json`
- `data/rs2/daily_scan_metrics.json`
- `data/rs2/lookups.json`
- `data/rs2/quality_audit.json`
- `data/rs2/four_week_lift_summary.json`

## Vercel Deployment Notes

- Framework preset: Next.js
- Install command: `npm install`
- Build command: `npm run build:rs2data && npm run build`
- Output: default Next.js output
- Ensure raw/mapping inputs are available in build environment or replace ETL input paths with mounted storage.
