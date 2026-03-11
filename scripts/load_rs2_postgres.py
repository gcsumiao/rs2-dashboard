#!/usr/bin/env python3
"""Load RS2 raw extracts into local PostgreSQL for dashboard queries."""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import psycopg

from build_rs2_data import (
    ZIP5_RE,
    build_report_part_counts,
    extract_zip5,
    geo_lookup_csv_has_payload,
    infer_state_from_zip5,
    load_geo_lookup,
    load_mapping,
    normalize_customer_id,
    normalize_nullable,
    normalize_vin,
    normalize_zip_postal,
    parse_datetime,
    resolve_tool_name,
)


def find_files(root: Path, filename: str) -> List[Path]:
    return sorted(path for path in root.rglob(filename) if path.is_file())


def pick_scan_files(raw_root: Path) -> List[Path]:
    preferred = find_files(raw_root, "4-week lift scan.csv")
    if preferred:
        return preferred
    fallback = find_files(raw_root, "scan-level raw extract.csv")
    if fallback:
        return fallback
    return []


def pick_latest(files: List[Path], label: str) -> Path:
    if not files:
        raise FileNotFoundError(f"Missing required input for {label}")
    files = sorted(files, key=lambda path: path.stat().st_mtime, reverse=True)
    return files[0]


def load_schema_sql(project_root: Path) -> str:
    schema_path = project_root / "scripts" / "sql" / "rs2_schema.sql"
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")
    return schema_path.read_text(encoding="utf-8")


def copy_report_part_counts(conn: psycopg.Connection, report_parts: Dict[str, Tuple[int, int, int]]) -> int:
    inserted = 0
    with conn.cursor() as cur:
        with cur.copy(
            "COPY rs2_report_part_counts (report_id, abs_parts, srs_parts, cel_parts) FROM STDIN"
        ) as copy:
            for report_id, (abs_parts, srs_parts, cel_parts) in report_parts.items():
                copy.write_row((report_id, int(abs_parts), int(srs_parts), int(cel_parts)))
                inserted += 1
    return inserted


def copy_zip_lookup(conn: psycopg.Connection, geo_lookup: Dict[str, Dict[str, object]]) -> int:
    inserted = 0
    with conn.cursor() as cur:
        with cur.copy("COPY rs2_zip_lookup (zip5, city, state, lat, lng) FROM STDIN") as copy:
            for zip5, payload in geo_lookup.items():
                if not ZIP5_RE.match(zip5):
                    continue
                city = normalize_nullable(str(payload.get("city", "")))
                state = normalize_nullable(str(payload.get("state", ""))).upper()
                lat = payload.get("lat")
                lng = payload.get("lng")
                lat_val = float(lat) if lat not in (None, "") else None
                lng_val = float(lng) if lng not in (None, "") else None
                copy.write_row((zip5, city, state, lat_val, lng_val))
                inserted += 1
    return inserted


def copy_scan_rows(
    conn: psycopg.Connection,
    scan_files: Iterable[Path],
    mapping,
    geo_lookup: Dict[str, Dict[str, object]],
) -> int:
    inserted = 0
    for scan_csv in scan_files:
        with scan_csv.open(newline="", encoding="utf-8-sig") as fh, conn.cursor() as cur:
            reader = csv.DictReader(fh)
            with cur.copy(
                """
                COPY rs2_scan (
                  report_id, created_utc, date_utc, user_id, account_id, email,
                  report_vin, vehicle_vin, vin_final, zip_postal, zip5, city, state,
                  usb_product_id, customer_id, vehicle_year, vehicle_make, vehicle_model, tool_name
                ) FROM STDIN
                """
            ) as copy:
                for row in reader:
                    report_id = normalize_nullable(row.get("DiagnosticReportId", ""))
                    if not report_id:
                        continue
                    created = parse_datetime(row.get("CreatedDateTimeUTC", ""))
                    if created is None:
                        continue

                    user_id = normalize_nullable(row.get("UserId", ""))
                    account_id = normalize_nullable(row.get("AccountId", ""))
                    email = normalize_nullable(row.get("Email", "")).lower()
                    report_vin = normalize_nullable(row.get("ReportVIN", "")).upper()
                    vehicle_vin = normalize_nullable(row.get("VehicleVIN", "")).upper()
                    vin_final = normalize_vin(report_vin, vehicle_vin)

                    zip_postal = normalize_zip_postal(row.get("Zip", ""))
                    zip5 = extract_zip5(zip_postal)
                    city = normalize_nullable(row.get("City", ""))
                    state = normalize_nullable(row.get("UserState", "")).upper()

                    geo = geo_lookup.get(zip5, {}) if zip5 else {}
                    if not city:
                        city = normalize_nullable(str(geo.get("city", "")))
                    if not state:
                        state = normalize_nullable(str(geo.get("state", ""))).upper()
                    if not state:
                        state = infer_state_from_zip5(zip5)

                    usb_product_id = normalize_nullable(row.get("UsbProductId", ""))
                    customer_id = normalize_customer_id(row.get("CustomerId", ""))
                    tool_name = resolve_tool_name(mapping, usb_product_id, customer_id)

                    copy.write_row(
                        (
                            report_id,
                            created,
                            created.date(),
                            user_id,
                            account_id,
                            email,
                            report_vin,
                            vehicle_vin,
                            vin_final,
                            zip_postal,
                            zip5,
                            city,
                            state,
                            usb_product_id,
                            customer_id,
                            normalize_nullable(row.get("VehicleYear", "")),
                            normalize_nullable(row.get("VehicleMake", "")),
                            normalize_nullable(row.get("VehicleModel", "")),
                            tool_name,
                        )
                    )
                    inserted += 1
    return inserted


def copy_buynow_rows(conn: psycopg.Connection, buynow_csv: Path) -> int:
    inserted = 0
    with buynow_csv.open(newline="", encoding="utf-8-sig") as fh, conn.cursor() as cur:
        reader = csv.reader(fh)
        _header = next(reader, None)
        if _header is None:
            return 0

        idx_report = 0
        idx_created = 1
        idx_email = 11
        idx_buy_from = 22
        idx_click_id = 23
        idx_clicked_account = 29
        idx_clicked_dt = 30

        with cur.copy(
            """
            COPY rs2_buynow_click (
              report_id, click_id, created_utc, date_utc, email, buy_from, clicked_account_id
            ) FROM STDIN
            """
        ) as copy:
            for row in reader:
                report_id = normalize_nullable(row[idx_report] if idx_report < len(row) else "")
                click_id = normalize_nullable(row[idx_click_id] if idx_click_id < len(row) else "")
                clicked_dt_raw = normalize_nullable(row[idx_clicked_dt] if idx_clicked_dt < len(row) else "")
                if not report_id or not click_id or not clicked_dt_raw:
                    continue

                created = parse_datetime(row[idx_created] if idx_created < len(row) else "")
                if created is None:
                    created = parse_datetime(clicked_dt_raw)
                if created is None:
                    continue

                email = normalize_nullable(row[idx_email] if idx_email < len(row) else "").lower()
                buy_from = normalize_nullable(row[idx_buy_from] if idx_buy_from < len(row) else "")
                clicked_account = normalize_nullable(row[idx_clicked_account] if idx_clicked_account < len(row) else "")

                copy.write_row((report_id, click_id, created, created.date(), email, buy_from, clicked_account))
                inserted += 1
    return inserted


def main() -> None:
    parser = argparse.ArgumentParser(description="Load RS2 extracts into PostgreSQL")
    parser.add_argument("--db-url", default="postgresql://rs2:rs2@localhost:5432/rs2_dashboard", help="PostgreSQL URL")
    parser.add_argument("--raw-root", type=Path, default=None, help="Root folder that contains raw RS2 extracts")
    parser.add_argument("--mapping-xlsx", type=Path, default=None, help="Path to RS2 tool mapping workbook")
    parser.add_argument("--geo-csv", type=Path, default=None, help="ZIP lookup CSV (zip/zip5 + city/state + lat/lng)")
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parents[1]
    workspace_root = project_root.parent

    raw_root = args.raw_root or (workspace_root / "raw_data")
    mapping_xlsx = args.mapping_xlsx or (workspace_root / "RS2tool_mapping" / "RS2_Tool_05122025_01.xlsx")
    geo_compact = project_root / "data" / "geo" / "us_zip_centroids.csv"
    default_geo = workspace_root / "zipmap.csv"
    if args.geo_csv is not None:
        geo_csv = args.geo_csv
    elif geo_lookup_csv_has_payload(geo_compact):
        geo_csv = geo_compact
    else:
        geo_csv = default_geo

    scan_files = pick_scan_files(raw_root)
    fix_csv = pick_latest(find_files(raw_root, "fix-part raw extract.csv"), "fix-part raw extract.csv")
    buynow_csv = pick_latest(find_files(raw_root, "BuyNow raw extract.csv"), "BuyNow raw extract.csv")
    if not scan_files:
        raise FileNotFoundError("Missing scan input: expected 4-week lift scan.csv or scan-level raw extract.csv")
    if not mapping_xlsx.exists():
        raise FileNotFoundError(f"Missing mapping workbook: {mapping_xlsx}")
    if not geo_csv.exists():
        raise FileNotFoundError(f"Missing geo lookup csv: {geo_csv}")

    print(f"Using raw root: {raw_root}", flush=True)
    print(f"Using scan files ({len(scan_files)}): {[str(path) for path in scan_files]}", flush=True)
    print(f"Using fix file: {fix_csv}", flush=True)
    print(f"Using buynow file: {buynow_csv}", flush=True)
    print(f"Using mapping: {mapping_xlsx}", flush=True)
    print(f"Using geo source: {geo_csv}", flush=True)

    mapping = load_mapping(mapping_xlsx)
    report_parts = build_report_part_counts(fix_csv)
    geo_lookup = load_geo_lookup(geo_csv)
    generated_at = datetime.now(timezone.utc)

    schema_sql = load_schema_sql(project_root)
    with psycopg.connect(args.db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(schema_sql)
            cur.execute("TRUNCATE rs2_scan, rs2_report_part_counts, rs2_buynow_click, rs2_zip_lookup, rs2_quality")

        part_rows = copy_report_part_counts(conn, report_parts)
        print(f"Loaded rs2_report_part_counts rows: {part_rows}", flush=True)

        zip_rows = copy_zip_lookup(conn, geo_lookup)
        print(f"Loaded rs2_zip_lookup rows: {zip_rows}", flush=True)

        scan_rows = copy_scan_rows(conn, scan_files, mapping, geo_lookup)
        print(f"Loaded rs2_scan rows: {scan_rows}", flush=True)

        buynow_rows = copy_buynow_rows(conn, buynow_csv)
        print(f"Loaded rs2_buynow_click rows: {buynow_rows}", flush=True)

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO rs2_quality (generated_at_utc, mapping_fallback_usb937, ambiguous_mapping_keys)
                VALUES (%s, %s, %s)
                """,
                (generated_at, mapping.blank_usb.get("937"), len(mapping.conflicts)),
            )
            cur.execute("ANALYZE rs2_scan")
            cur.execute("ANALYZE rs2_buynow_click")
            cur.execute("ANALYZE rs2_report_part_counts")
            cur.execute("ANALYZE rs2_zip_lookup")

        conn.commit()

    print("RS2 PostgreSQL load complete.", flush=True)


if __name__ == "__main__":
    main()
