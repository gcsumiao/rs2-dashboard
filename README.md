# RS2 Dashboard (PostgreSQL Mode)

Interactive RS2 inspection dashboard backed by local PostgreSQL 18 in Docker.

## Architecture

- App: Next.js
- DB: PostgreSQL 18 (`docker-compose.yml`)
- Data source: `/Users/sumiaoc/Desktop/RS2_dashboard/raw_data`
- API: `GET /api/rs2/dashboard?start=YYYY-MM-DD&end=YYYY-MM-DD&topN=number&tz=UTC`
- Time basis: UTC end-to-end

Snapshot JSON fast-path is removed from dashboard API logic. The API now queries PostgreSQL directly.

Current loader logic:
- Scan-level base comes from `YYYYMM/fix-part raw extract.csv` (deduped by `DiagnosticReportId`).
- Part counts come from `FixType` rows in the same fix-part extracts.
- BuyNow clicks come from each `YYYYMM/BuyNow raw extract.csv`.
- Validation checks are read from `raw_data/data_validation/*ScanResults.csv`.

## Prerequisites

- Docker Desktop
- Python 3
- Node.js / npm

## One-Time Setup

1. Install node dependencies:

```bash
npm install --legacy-peer-deps
```

2. Install Python loader dependency:

```bash
python3 -m pip install -r scripts/requirements-rs2-db.txt
```

3. Start PostgreSQL container:

```bash
npm run db:up
```

4. Load RS2 raw files into PostgreSQL:

```bash
npm run db:load:rs2
```
`db:load:rs2` bootstraps a project-local virtualenv (`.venv_rs2db`) and installs `psycopg` there, so it does not depend on system Python package permissions.

5. Run dashboard:

```bash
npm run dev
```

Open [http://localhost:3000](http://localhost:3000).

## Daily Workflow

- If raw data did not change: just run `npm run dev`.
- If raw data changed (e.g. new `202601`/`202602` files): run `npm run db:load:rs2` then `npm run dev`.

## Commands

- `npm run db:up`: start PostgreSQL container
- `npm run db:down`: stop PostgreSQL container
- `npm run db:logs`: follow PostgreSQL logs
- `npm run db:load:rs2`: truncate and reload RS2 tables from raw extracts

## Default Inputs (Auto-Resolved)

Loader defaults:

- raw root: `/Users/sumiaoc/Desktop/RS2_dashboard/raw_data` (recursive search)
- mapping: `/Users/sumiaoc/Desktop/RS2_dashboard/raw_data/RS2tool_mapping/RS2_Tool_05122025_01.xlsx`
- geo source priority:
  1. `data/geo/us_zip_centroids.csv` (if already populated)
  2. `/Users/sumiaoc/Desktop/RS2_dashboard/raw_data/zipmap.csv`

## Data Rules Implemented

- Tool mapping key: `UsbProductId + CustomerId`
- Fallback mapping: blank customer + matching USB (`937 -> 5110 / 3020RS`)
- FixType mapping:
  - `2 = ABS`
  - `0 = CEL`
  - `3 = SRS`
