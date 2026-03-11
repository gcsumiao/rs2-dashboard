#!/usr/bin/env python3
"""Load RS2 raw extracts into local PostgreSQL for dashboard queries."""

from __future__ import annotations

import argparse
import csv
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

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

try:
    import psycopg  # type: ignore
except ModuleNotFoundError as exc:
    raise SystemExit(
        "Missing Python package 'psycopg'. Run loader via `npm run db:load:rs2` "
        "which uses the project virtualenv bootstrap."
    ) from exc


@dataclass
class MonthBundle:
    month: str
    fix_csv: Path
    buynow_csv: Path
    scan_csv: Path | None


def load_schema_sql(project_root: Path) -> str:
    schema_path = project_root / "scripts" / "sql" / "rs2_schema.sql"
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")
    return schema_path.read_text(encoding="utf-8")


def discover_month_bundles(raw_root: Path) -> List[MonthBundle]:
    bundles: List[MonthBundle] = []
    for entry in sorted(raw_root.iterdir()):
        if not entry.is_dir():
            continue
        month = entry.name
        if not re.fullmatch(r"\d{6}", month):
            continue

        fix_csv = entry / "fix-part raw extract.csv"
        buynow_csv = entry / "BuyNow raw extract.csv"
        scan_csv = entry / "scan-level raw extract.csv"

        if not fix_csv.exists():
            raise FileNotFoundError(f"Missing {fix_csv}")
        if not buynow_csv.exists():
            raise FileNotFoundError(f"Missing {buynow_csv}")

        bundles.append(MonthBundle(month=month, fix_csv=fix_csv, buynow_csv=buynow_csv, scan_csv=scan_csv if scan_csv.exists() else None))

    if not bundles:
        raise FileNotFoundError(f"No YYYYMM month folders found under {raw_root}")
    return bundles


def normalize_retailer_key(value: str) -> str:
    lowered = normalize_nullable(value).lower()
    lowered = lowered.replace("&", "and")
    lowered = re.sub(r"[^a-z0-9]+", "", lowered)
    return lowered


def load_validation_expected(validation_dir: Path) -> Dict[str, Dict[str, Tuple[str, int]]]:
    expected: Dict[str, Dict[str, Tuple[str, int]]] = {}
    if not validation_dir.exists():
        return expected

    for path in sorted(validation_dir.glob("*ScanResults.csv")):
        match = re.match(r"(\d{6})ScanResults\.csv$", path.name)
        if not match:
            continue
        month = match.group(1)
        month_expected: Dict[str, Tuple[str, int]] = {}
        with path.open(newline="", encoding="utf-8-sig") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                retailer = normalize_nullable(row.get("Retailer", ""))
                scans_raw = normalize_nullable(row.get("TotalUniqueReportScans", "0"))
                if not retailer:
                    continue
                try:
                    scans = int(float(scans_raw))
                except ValueError:
                    scans = 0
                month_expected[normalize_retailer_key(retailer)] = (retailer, scans)
        expected[month] = month_expected
    return expected


def copy_report_part_counts(conn: psycopg.Connection, bundles: Iterable[MonthBundle]) -> int:
    combined: Dict[str, List[int]] = {}
    for bundle in bundles:
        counts = build_report_part_counts(bundle.fix_csv)
        for report_id, (abs_parts, srs_parts, cel_parts) in counts.items():
            bucket = combined.setdefault(report_id, [0, 0, 0])
            bucket[0] += int(abs_parts)
            bucket[1] += int(srs_parts)
            bucket[2] += int(cel_parts)

    inserted = 0
    with conn.cursor() as cur:
        with cur.copy(
            "COPY rs2_report_part_counts (report_id, abs_parts, srs_parts, cel_parts) FROM STDIN"
        ) as copy:
            for report_id, (abs_parts, srs_parts, cel_parts) in combined.items():
                copy.write_row((report_id, abs_parts, srs_parts, cel_parts))
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


def copy_scan_rows_from_fix_part(
    conn: psycopg.Connection,
    bundles: Iterable[MonthBundle],
    mapping,
    geo_lookup: Dict[str, Dict[str, object]],
) -> int:
    inserted = 0
    seen_report_ids: set[str] = set()

    with conn.cursor() as cur:
        with cur.copy(
            """
            COPY rs2_scan (
              report_id, created_utc, date_utc, source_month, user_id, account_id, email,
              report_vin, vehicle_vin, vin_final, zip_postal, zip5, city, state,
              usb_product_id, customer_id, vehicle_year, vehicle_make, vehicle_model, buy_from, tool_name
            ) FROM STDIN
            """
        ) as copy:
            for bundle in bundles:
                with bundle.fix_csv.open(newline="", encoding="utf-8-sig") as fh:
                    reader = csv.DictReader(fh)
                    for row in reader:
                        report_id = normalize_nullable(row.get("DiagnosticReportId", ""))
                        if not report_id or report_id in seen_report_ids:
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
                        buy_from = normalize_nullable(row.get("BuyFrom", ""))

                        copy.write_row(
                            (
                                report_id,
                                created,
                                created.date(),
                                bundle.month,
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
                                buy_from,
                                tool_name,
                            )
                        )
                        inserted += 1
                        seen_report_ids.add(report_id)
    return inserted


def copy_buynow_rows(conn: psycopg.Connection, bundles: Iterable[MonthBundle]) -> int:
    inserted = 0
    with conn.cursor() as cur:
        with cur.copy(
            """
            COPY rs2_buynow_click (
              report_id, click_id, created_utc, date_utc, email, buy_from, clicked_account_id
            ) FROM STDIN
            """
        ) as copy:
            for bundle in bundles:
                with bundle.buynow_csv.open(newline="", encoding="utf-8-sig") as fh:
                    reader = csv.reader(fh)
                    _header = next(reader, None)
                    if _header is None:
                        continue

                    idx_report = 0
                    idx_created = 1
                    idx_email = 11
                    idx_buy_from = 22
                    idx_click_id = 23
                    idx_clicked_account = 29
                    idx_clicked_dt = 30

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


def run_validation_checks(
    conn: psycopg.Connection,
    expected_by_month: Dict[str, Dict[str, Tuple[str, int]]],
) -> Tuple[int, List[str]]:
    actual_rows = conn.execute(
        """
        SELECT
          source_month,
          buy_from,
          COUNT(*)::int AS scans
        FROM rs2_scan
        GROUP BY source_month, buy_from
        """
    ).fetchall()

    actual_by_month: Dict[str, Dict[str, Tuple[str, int]]] = {}
    for source_month, buy_from, scans in actual_rows:
        month = str(source_month or "")
        retailer = normalize_nullable(str(buy_from or ""))
        key = normalize_retailer_key(retailer if retailer else "other")
        month_map = actual_by_month.setdefault(month, {})
        prev = month_map.get(key)
        prev_count = prev[1] if prev else 0
        month_map[key] = (retailer if retailer else "Other", prev_count + int(scans))

    inserted = 0
    mismatches: List[str] = []
    with conn.cursor() as cur:
        for month in sorted(set(expected_by_month.keys()) | set(actual_by_month.keys())):
            expected = expected_by_month.get(month, {})
            actual = actual_by_month.get(month, {})
            keys = sorted(set(expected.keys()) | set(actual.keys()))

            for key in keys:
                expected_label, expected_count = expected.get(key, (actual.get(key, ("", 0))[0], 0))
                actual_label, actual_count = actual.get(key, (expected_label, 0))

                if key == "total":
                    retailer_name = "Total"
                    actual_total = sum(value[1] for k, value in actual.items() if k != "total")
                    actual_count = actual_total
                else:
                    retailer_name = expected_label or actual_label or key

                delta = actual_count - expected_count
                cur.execute(
                    """
                    INSERT INTO rs2_validation_retailer_month (
                      source_month, retailer, expected_unique_report_scans, actual_unique_report_scans, delta
                    ) VALUES (%s, %s, %s, %s, %s)
                    """,
                    (month, retailer_name, int(expected_count), int(actual_count), int(delta)),
                )
                inserted += 1

                if expected_count != actual_count:
                    mismatches.append(f"{month} | {retailer_name} | expected={expected_count} actual={actual_count} delta={delta}")

    return inserted, mismatches


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
    mapping_xlsx = args.mapping_xlsx or (raw_root / "RS2tool_mapping" / "RS2_Tool_05122025_01.xlsx")
    geo_compact = project_root / "data" / "geo" / "us_zip_centroids.csv"
    default_geo = raw_root / "zipmap.csv"
    validation_dir = raw_root / "data_validation"

    if args.geo_csv is not None:
        geo_csv = args.geo_csv
    elif geo_lookup_csv_has_payload(geo_compact):
        geo_csv = geo_compact
    else:
        geo_csv = default_geo

    if not raw_root.exists():
        raise FileNotFoundError(f"Raw root not found: {raw_root}")
    if not mapping_xlsx.exists():
        raise FileNotFoundError(f"Missing mapping workbook: {mapping_xlsx}")
    if not geo_csv.exists():
        raise FileNotFoundError(f"Missing geo lookup csv: {geo_csv}")

    bundles = discover_month_bundles(raw_root)
    expected_validation = load_validation_expected(validation_dir)

    print(f"Using raw root: {raw_root}", flush=True)
    print(f"Using months: {[bundle.month for bundle in bundles]}", flush=True)
    print("Using fix-part as scan-level source (dedup by DiagnosticReportId).", flush=True)
    print(f"Using mapping: {mapping_xlsx}", flush=True)
    print(f"Using geo source: {geo_csv}", flush=True)
    if expected_validation:
        print(f"Using validation files for months: {sorted(expected_validation.keys())}", flush=True)

    mapping = load_mapping(mapping_xlsx)
    geo_lookup = load_geo_lookup(geo_csv)
    generated_at = datetime.now(timezone.utc)
    schema_sql = load_schema_sql(project_root)

    with psycopg.connect(args.db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(schema_sql)
            cur.execute(
                """
                TRUNCATE
                  rs2_scan,
                  rs2_report_part_counts,
                  rs2_buynow_click,
                  rs2_zip_lookup,
                  rs2_quality,
                  rs2_validation_retailer_month
                """
            )

        part_rows = copy_report_part_counts(conn, bundles)
        print(f"Loaded rs2_report_part_counts rows: {part_rows}", flush=True)

        zip_rows = copy_zip_lookup(conn, geo_lookup)
        print(f"Loaded rs2_zip_lookup rows: {zip_rows}", flush=True)

        scan_rows = copy_scan_rows_from_fix_part(conn, bundles, mapping, geo_lookup)
        print(f"Loaded rs2_scan rows: {scan_rows}", flush=True)

        buynow_rows = copy_buynow_rows(conn, bundles)
        print(f"Loaded rs2_buynow_click rows: {buynow_rows}", flush=True)

        validation_rows, validation_mismatches = run_validation_checks(conn, expected_validation)
        print(f"Loaded rs2_validation_retailer_month rows: {validation_rows}", flush=True)
        if validation_mismatches:
            print("Validation mismatches detected:", flush=True)
            for line in validation_mismatches[:40]:
                print(f"  - {line}", flush=True)
            if len(validation_mismatches) > 40:
                print(f"  ... {len(validation_mismatches) - 40} more", flush=True)
        else:
            print("Validation check passed: expected vs actual retailer/month counts match.", flush=True)

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
            cur.execute("ANALYZE rs2_validation_retailer_month")
        conn.commit()

    print("RS2 PostgreSQL load complete.", flush=True)


if __name__ == "__main__":
    main()
