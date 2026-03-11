#!/usr/bin/env python3
"""Build RS2 dashboard artifacts optimized for fast interactive queries."""

from __future__ import annotations

import argparse
import csv
import json
import re
import zipfile
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo
import xml.etree.ElementTree as ET


DATE_FORMATS = ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S")
NULL_TOKENS = {"", "NULL", "null", "None"}
ZIP5_RE = re.compile(r"^\d{5}$")
ZIP9_RE = re.compile(r"^\d{5}-\d{4}$")
SHEET_MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
SHEET_REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
PKG_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"
DEFAULT_START = "2026-02-01"
DEFAULT_END = "2026-02-28"
MAX_SNAPSHOT_TOP_N = 500
MAX_HEAT_POINTS = 8000

STATE_CENTROIDS: Dict[str, Tuple[float, float]] = {
    "AL": (32.806671, -86.79113),
    "AK": (61.370716, -152.404419),
    "AZ": (33.729759, -111.431221),
    "AR": (34.969704, -92.373123),
    "CA": (36.116203, -119.681564),
    "CO": (39.059811, -105.311104),
    "CT": (41.597782, -72.755371),
    "DE": (39.318523, -75.507141),
    "FL": (27.766279, -81.686783),
    "GA": (33.040619, -83.643074),
    "HI": (21.094318, -157.498337),
    "ID": (44.240459, -114.478828),
    "IL": (40.349457, -88.986137),
    "IN": (39.849426, -86.258278),
    "IA": (42.011539, -93.210526),
    "KS": (38.5266, -96.726486),
    "KY": (37.66814, -84.670067),
    "LA": (31.169546, -91.867805),
    "ME": (44.693947, -69.381927),
    "MD": (39.063946, -76.802101),
    "MA": (42.230171, -71.530106),
    "MI": (43.326618, -84.536095),
    "MN": (45.694454, -93.900192),
    "MS": (32.741646, -89.678696),
    "MO": (38.456085, -92.288368),
    "MT": (46.921925, -110.454353),
    "NE": (41.12537, -98.268082),
    "NV": (38.313515, -117.055374),
    "NH": (43.452492, -71.563896),
    "NJ": (40.298904, -74.521011),
    "NM": (34.840515, -106.248482),
    "NY": (42.165726, -74.948051),
    "NC": (35.630066, -79.806419),
    "ND": (47.528912, -99.784012),
    "OH": (40.388783, -82.764915),
    "OK": (35.565342, -96.928917),
    "OR": (44.572021, -122.070938),
    "PA": (40.590752, -77.209755),
    "RI": (41.680893, -71.51178),
    "SC": (33.856892, -80.945007),
    "SD": (44.299782, -99.438828),
    "TN": (35.747845, -86.692345),
    "TX": (31.054487, -97.563461),
    "UT": (40.150032, -111.862434),
    "VT": (44.045876, -72.710686),
    "VA": (37.769337, -78.169968),
    "WA": (47.400902, -121.490494),
    "WV": (38.491226, -80.954453),
    "WI": (44.268543, -89.616508),
    "WY": (42.755966, -107.30249),
    "DC": (38.897438, -77.026817),
}

# USPS ZIP prefix ranges.
ZIP_PREFIX_STATE_RANGES: List[Tuple[int, int, str]] = [
    (5, 5, "NY"),
    (6, 9, "PR"),
    (10, 27, "MA"),
    (28, 29, "RI"),
    (30, 38, "NH"),
    (39, 49, "ME"),
    (50, 59, "VT"),
    (60, 69, "CT"),
    (70, 89, "NJ"),
    (100, 149, "NY"),
    (150, 196, "PA"),
    (197, 199, "DE"),
    (200, 205, "DC"),
    (206, 219, "MD"),
    (220, 246, "VA"),
    (247, 268, "WV"),
    (270, 289, "NC"),
    (290, 299, "SC"),
    (300, 319, "GA"),
    (320, 349, "FL"),
    (350, 369, "AL"),
    (370, 385, "TN"),
    (386, 397, "MS"),
    (398, 399, "GA"),
    (400, 427, "KY"),
    (430, 459, "OH"),
    (460, 479, "IN"),
    (480, 499, "MI"),
    (500, 528, "IA"),
    (530, 549, "WI"),
    (550, 567, "MN"),
    (570, 577, "SD"),
    (580, 588, "ND"),
    (590, 599, "MT"),
    (600, 629, "IL"),
    (630, 658, "MO"),
    (660, 679, "KS"),
    (680, 693, "NE"),
    (700, 715, "LA"),
    (716, 729, "AR"),
    (730, 749, "OK"),
    (750, 799, "TX"),
    (800, 816, "CO"),
    (820, 831, "WY"),
    (832, 838, "ID"),
    (840, 847, "UT"),
    (850, 865, "AZ"),
    (870, 884, "NM"),
    (885, 885, "TX"),
    (889, 898, "NV"),
    (900, 961, "CA"),
    (967, 968, "HI"),
    (970, 979, "OR"),
    (980, 994, "WA"),
    (995, 999, "AK"),
]


@dataclass
class MappingData:
    exact: Dict[Tuple[str, str], str]
    blank_usb: Dict[str, str]
    conflicts: Dict[Tuple[str, str], List[str]]


def parse_datetime(raw: str) -> Optional[datetime]:
    value = (raw or "").strip()
    if not value or value in NULL_TOKENS:
        return None
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def to_pt_date(dt: datetime, tz: ZoneInfo) -> str:
    return dt.astimezone(tz).date().isoformat()


def normalize_nullable(raw: str) -> str:
    value = (raw or "").strip()
    if value in NULL_TOKENS:
        return ""
    return value


def normalize_customer_id(raw: str) -> str:
    value = normalize_nullable(raw)
    if value.startswith("[") and value.endswith("]"):
        return value[1:-1].strip()
    return value


def normalize_vin(report_vin: str, vehicle_vin: str) -> str:
    report = normalize_nullable(report_vin).upper()
    if report:
        return report
    return normalize_nullable(vehicle_vin).upper()


def normalize_zip_postal(raw_zip: str) -> str:
    return normalize_nullable(raw_zip).replace(" ", "").upper()


def extract_zip5(zip_postal: str) -> str:
    if ZIP5_RE.match(zip_postal):
        return zip_postal
    if ZIP9_RE.match(zip_postal):
        return zip_postal[:5]
    return ""


def stable_hash(text: str) -> int:
    h = 2166136261
    for ch in text:
        h ^= ord(ch)
        h += (h << 1) + (h << 4) + (h << 7) + (h << 8) + (h << 24)
    return abs(h & 0xFFFFFFFF)


def infer_state_from_zip5(zip5: str) -> str:
    if not ZIP5_RE.match(zip5):
        return ""
    prefix = int(zip5[:3])
    for low, high, state in ZIP_PREFIX_STATE_RANGES:
        if low <= prefix <= high:
            return state
    return ""


def centroid_for_zip(zip5: str, state: str) -> Tuple[float, float]:
    base = STATE_CENTROIDS.get(state.upper())
    if base is None:
        return (0.0, 0.0)
    h = stable_hash(zip5)
    offset_lat = ((h % 2001) - 1000) / 2500.0
    offset_lng = (((h // 2001) % 2001) - 1000) / 2500.0
    return (base[0] + offset_lat, base[1] + offset_lng)


def load_geo_lookup(geo_csv: Path) -> Dict[str, Dict[str, object]]:
    if not geo_csv.exists():
        return {}

    out: Dict[str, Dict[str, object]] = {}
    with geo_csv.open(newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            zip5 = normalize_nullable(row.get("zip5", ""))
            if not zip5:
                zip5 = normalize_nullable(row.get("zip", ""))
            if zip5.isdigit():
                zip5 = zip5.zfill(5)
            if not ZIP5_RE.match(zip5):
                continue
            lat_raw = normalize_nullable(row.get("lat", ""))
            lng_raw = normalize_nullable(row.get("lng", ""))
            state = normalize_nullable(row.get("state", ""))
            if not state:
                state = normalize_nullable(row.get("state_id", ""))
            state = state.upper()
            city = normalize_nullable(row.get("city", ""))
            lat = 0.0
            lng = 0.0
            try:
                lat = float(lat_raw) if lat_raw else 0.0
                lng = float(lng_raw) if lng_raw else 0.0
            except ValueError:
                lat = 0.0
                lng = 0.0
            out[zip5] = {"city": city, "state": state, "lat": lat, "lng": lng}
    return out


def xlsx_cell_value(cell: ET.Element, shared_strings: List[str]) -> str:
    cell_type = cell.attrib.get("t")
    v = cell.find(f"{{{SHEET_MAIN_NS}}}v")
    if v is None:
        inline = cell.find(f"{{{SHEET_MAIN_NS}}}is")
        if inline is None:
            return ""
        return "".join(node.text or "" for node in inline.iter(f"{{{SHEET_MAIN_NS}}}t"))
    raw = v.text or ""
    if cell_type == "s":
        try:
            return shared_strings[int(raw)]
        except (ValueError, IndexError):
            return raw
    return raw


def load_mapping(mapping_path: Path) -> MappingData:
    with zipfile.ZipFile(mapping_path) as workbook_zip:
        workbook = ET.fromstring(workbook_zip.read("xl/workbook.xml"))
        sheets_node = workbook.find(f"{{{SHEET_MAIN_NS}}}sheets")
        if sheets_node is None:
            raise RuntimeError("Could not find sheets collection in mapping workbook")

        sheet_rid = None
        for sheet in sheets_node:
            if sheet.attrib.get("name") == "Tools":
                sheet_rid = sheet.attrib.get(f"{{{SHEET_REL_NS}}}id")
                break
        if not sheet_rid:
            raise RuntimeError("Could not find 'Tools' sheet in mapping workbook")

        rels = ET.fromstring(workbook_zip.read("xl/_rels/workbook.xml.rels"))
        target = None
        for rel in rels.findall(f"{{{PKG_REL_NS}}}Relationship"):
            if rel.attrib.get("Id") == sheet_rid:
                target = rel.attrib.get("Target")
                break
        if not target:
            raise RuntimeError("Could not resolve 'Tools' sheet target path")

        sheet_xml_path = ("xl/" + target).replace("xl/xl/", "xl/")
        shared_strings: List[str] = []
        if "xl/sharedStrings.xml" in workbook_zip.namelist():
            sst = ET.fromstring(workbook_zip.read("xl/sharedStrings.xml"))
            for si in sst:
                shared_strings.append(
                    "".join(node.text or "" for node in si.iter(f"{{{SHEET_MAIN_NS}}}t"))
                )

        sheet_xml = ET.fromstring(workbook_zip.read(sheet_xml_path))
        rows = sheet_xml.findall(f".//{{{SHEET_MAIN_NS}}}row")
        if not rows:
            raise RuntimeError("Tools sheet is empty")

        header_map: Dict[str, str] = {}
        for cell in rows[0].findall(f"{{{SHEET_MAIN_NS}}}c"):
            ref = cell.attrib.get("r", "")
            col = "".join(ch for ch in ref if ch.isalpha())
            header_map[col] = xlsx_cell_value(cell, shared_strings).strip()

        required = {
            "Options.Attribute:bleCustomerId": None,
            "UsbProductId": None,
            "Attribute:CurrentDisplayName": None,
        }
        for col, title in header_map.items():
            if title in required:
                required[title] = col
        if any(value is None for value in required.values()):
            raise RuntimeError(f"Missing required mapping columns: {required}")

        col_customer = required["Options.Attribute:bleCustomerId"] or ""
        col_usb = required["UsbProductId"] or ""
        col_display = required["Attribute:CurrentDisplayName"] or ""

        exact: Dict[Tuple[str, str], str] = {}
        blank_usb: Dict[str, str] = {}
        conflict_sets: Dict[Tuple[str, str], set[str]] = defaultdict(set)

        for row in rows[1:]:
            row_cells: Dict[str, str] = {}
            for cell in row.findall(f"{{{SHEET_MAIN_NS}}}c"):
                ref = cell.attrib.get("r", "")
                col = "".join(ch for ch in ref if ch.isalpha())
                row_cells[col] = xlsx_cell_value(cell, shared_strings).strip()

            usb = normalize_nullable(row_cells.get(col_usb, ""))
            customer = normalize_customer_id(row_cells.get(col_customer, ""))
            display = normalize_nullable(row_cells.get(col_display, ""))
            if not usb or not display:
                continue

            key = (usb, customer)
            conflict_sets[key].add(display)
            if key not in exact:
                exact[key] = display
            if customer == "" and usb not in blank_usb:
                blank_usb[usb] = display

        conflicts: Dict[Tuple[str, str], List[str]] = {}
        for key, values in conflict_sets.items():
            if len(values) > 1:
                conflicts[key] = sorted(values)

        return MappingData(exact=exact, blank_usb=blank_usb, conflicts=conflicts)


def resolve_tool_name(mapping: MappingData, usb: str, customer: str) -> str:
    key = (usb, customer)
    if key in mapping.exact:
        return mapping.exact[key]
    if usb in mapping.blank_usb:
        return mapping.blank_usb[usb]
    return "UNMAPPED"


def classify_fix_part(row: Dict[str, str]) -> Tuple[int, int, int]:
    raw_fix_type = normalize_nullable(row.get("FixType", ""))
    if not raw_fix_type:
        return 0, 0, 0
    fix_type_code: Optional[int] = None
    try:
        fix_type_code = int(float(raw_fix_type))
    except ValueError:
        return 0, 0, 0

    # Rule from business definition:
    # FixType 2 = ABS, 0 = CEL, 3 = SRS
    if fix_type_code == 2:
        return 1, 0, 0
    if fix_type_code == 0:
        return 0, 0, 1
    if fix_type_code == 3:
        return 0, 1, 0
    return 0, 0, 0


def build_report_part_counts(fix_csv: Path) -> Dict[str, Tuple[int, int, int]]:
    counts: Dict[str, List[int]] = defaultdict(lambda: [0, 0, 0])
    with fix_csv.open(newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            report_id = normalize_nullable(row.get("DiagnosticReportId", ""))
            if not report_id:
                continue
            abs_hit, srs_hit, cel_hit = classify_fix_part(row)
            bucket = counts[report_id]
            bucket[0] += abs_hit
            bucket[1] += srs_hit
            bucket[2] += cel_hit
    out: Dict[str, Tuple[int, int, int]] = {}
    for report_id, vals in counts.items():
        out[report_id] = (vals[0], vals[1], vals[2])
    return out


def ensure_geo_placeholder(geo_csv: Path) -> None:
    if geo_csv.exists():
        return
    geo_csv.parent.mkdir(parents=True, exist_ok=True)
    geo_csv.write_text("zip5,lat,lng,state,city\n", encoding="utf-8")


def pick_primary(counter_by_key: Dict[str, Counter]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for key, counter in counter_by_key.items():
        if not counter:
            continue
        value = sorted(counter.items(), key=lambda item: (-item[1], item[0]))[0][0]
        out[key] = value
    return out


def process_scan_main(
    scan_csv: Path,
    mapping: MappingData,
    report_parts: Dict[str, Tuple[int, int, int]],
    geo_lookup: Dict[str, Dict[str, object]],
    tz: ZoneInfo,
) -> Tuple[Dict[str, Dict[str, object]], Dict[str, Dict[str, object]], Dict[str, object]]:
    day_agg: Dict[str, Dict[str, object]] = {}
    daily_sets: Dict[str, Dict[str, object]] = {}

    user_email_counter: Dict[str, Counter] = defaultdict(Counter)
    vin_info_counter: Dict[str, Counter] = defaultdict(Counter)
    account_vin_info_counter: Dict[str, Counter] = defaultdict(Counter)
    zip_info_counter: Dict[str, Counter] = defaultdict(Counter)
    zip3_city_counter: Dict[str, Counter] = defaultdict(Counter)

    row_count = 0
    distinct_reports = set()
    min_dt = None
    max_dt = None
    unmapped_usb = Counter()

    with scan_csv.open(newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            row_count += 1
            report_id = normalize_nullable(row.get("DiagnosticReportId", ""))
            if not report_id:
                continue

            created = parse_datetime(row.get("CreatedDateTimeUTC", ""))
            if created is None:
                continue
            if min_dt is None or created < min_dt:
                min_dt = created
            if max_dt is None or created > max_dt:
                max_dt = created

            date_pt = to_pt_date(created, tz)
            slot = day_agg.setdefault(
                date_pt,
                {
                    "scans": 0,
                    "users": Counter(),
                    "user_emails": Counter(),
                    "tool_stats": defaultdict(lambda: [0, 0, 0, 0]),  # inspections, abs, srs, cel
                    "vins": Counter(),
                    "account_vins": Counter(),
                    "zip_postal": Counter(),
                    "zip5": Counter(),
                },
            )

            user_id = normalize_nullable(row.get("UserId", ""))
            account_id = normalize_nullable(row.get("AccountId", ""))
            email = normalize_nullable(row.get("Email", "")).lower()
            vin = normalize_vin(row.get("ReportVIN", ""), row.get("VehicleVIN", ""))
            zip_postal = normalize_zip_postal(row.get("Zip", ""))
            zip5 = extract_zip5(zip_postal)
            city = normalize_nullable(row.get("City", ""))
            state = normalize_nullable(row.get("UserState", "")).upper()
            geo = geo_lookup.get(zip5, {}) if zip5 else {}
            if not city:
                city = str(geo.get("city", ""))
            if not state:
                state = str(geo.get("state", "")).upper()
            if not state:
                state = infer_state_from_zip5(zip5)
            if city and zip5:
                zip3_city_counter[zip5[:3]][city] += 1
            usb = normalize_nullable(row.get("UsbProductId", ""))
            customer = normalize_customer_id(row.get("CustomerId", ""))
            tool_name = resolve_tool_name(mapping, usb, customer)
            if tool_name == "UNMAPPED":
                unmapped_usb[usb] += 1

            year = normalize_nullable(row.get("VehicleYear", ""))
            make = normalize_nullable(row.get("VehicleMake", ""))
            model = normalize_nullable(row.get("VehicleModel", ""))
            engine = ""  # no engine column in scan extract

            abs_cnt, srs_cnt, cel_cnt = report_parts.get(report_id, (0, 0, 0))

            slot["scans"] = int(slot["scans"]) + 1
            if user_id:
                slot["users"][user_id] += 1
                if email:
                    slot["user_emails"][f"{user_id}|||{email}"] += 1
                    user_email_counter[user_id][email] += 1
            if tool_name:
                tool_bucket = slot["tool_stats"][tool_name]
                tool_bucket[0] += 1
                tool_bucket[1] += abs_cnt
                tool_bucket[2] += srs_cnt
                tool_bucket[3] += cel_cnt

            if vin:
                slot["vins"][vin] += 1
                slot["account_vins"][f"{account_id}|||{vin}"] += 1
                vin_info_counter[vin][f"{year}|||{make}|||{model}|||{engine}"] += 1
                account_vin_info_counter[f"{account_id}|||{vin}"][
                    f"{email}|||{year}|||{make}|||{model}|||{engine}"
                ] += 1

            if zip_postal:
                slot["zip_postal"][zip_postal] += 1
                zip_info_counter[zip_postal][f"{city}|||{state}|||{zip5}"] += 1
            if zip5:
                slot["zip5"][zip5] += 1

            daily_slot = daily_sets.setdefault(date_pt, {"scans": 0, "users": set(), "vins": set()})
            daily_slot["scans"] = int(daily_slot["scans"]) + 1
            if user_id:
                daily_slot["users"].add(user_id)
            if vin:
                daily_slot["vins"].add(vin)

            distinct_reports.add(report_id)

    user_lookup = {user: email for user, email in pick_primary(user_email_counter).items()}
    vin_lookup: Dict[str, Dict[str, str]] = {}
    for vin, info in pick_primary(vin_info_counter).items():
        year, make, model, engine = (info.split("|||") + ["", "", "", ""])[:4]
        vin_lookup[vin] = {"year": year, "make": make, "model": model, "engine": engine}

    account_vin_lookup: Dict[str, Dict[str, str]] = {}
    for key, info in pick_primary(account_vin_info_counter).items():
        email, year, make, model, engine = (info.split("|||") + ["", "", "", "", ""])[:5]
        account_vin_lookup[key] = {
            "email": email,
            "year": year,
            "make": make,
            "model": model,
            "engine": engine,
        }

    zip_lookup: Dict[str, Dict[str, str]] = {}
    for zip_postal, info in pick_primary(zip_info_counter).items():
        city, state, zip5 = (info.split("|||") + ["", "", ""])[:3]
        geo = geo_lookup.get(zip5, {}) if zip5 else {}
        lat_raw = geo.get("lat", 0.0)
        lng_raw = geo.get("lng", 0.0)
        try:
            lat = float(lat_raw)
            lng = float(lng_raw)
        except (TypeError, ValueError):
            lat = 0.0
            lng = 0.0
        if (not city) and geo.get("city"):
            city = str(geo.get("city", ""))
        if (not city) and zip5 and zip5[:3] in zip3_city_counter and zip3_city_counter[zip5[:3]]:
            city = sorted(zip3_city_counter[zip5[:3]].items(), key=lambda item: (-item[1], item[0]))[0][0]
        if (not state) and geo.get("state"):
            state = str(geo.get("state", "")).upper()
        if not state:
            state = infer_state_from_zip5(zip5)
        if (not lat or not lng) and state in STATE_CENTROIDS and zip5:
            lat, lng = centroid_for_zip(zip5, state)
        zip_lookup[zip_postal] = {
            "city": city,
            "state": state,
            "zip5": zip5,
            "lat": f"{lat:.6f}" if lat else "",
            "lng": f"{lng:.6f}" if lng else "",
        }

    lookup = {
        "user_lookup": user_lookup,
        "vin_lookup": vin_lookup,
        "account_vin_lookup": account_vin_lookup,
        "zip_lookup": zip_lookup,
    }

    summary = {
        "rows": row_count,
        "rows_written": sum(int(item["scans"]) for item in day_agg.values()),
        "distinct_report_ids": len(distinct_reports),
        "min_created_utc": min_dt.isoformat() if min_dt else None,
        "max_created_utc": max_dt.isoformat() if max_dt else None,
        "top_unmapped_usb": unmapped_usb.most_common(20),
    }
    return day_agg, daily_sets, summary | lookup


def process_buynow_clicks(
    buynow_csv: Path,
    tz: ZoneInfo,
) -> Tuple[Dict[str, Dict[str, object]], Dict[str, object]]:
    day_reports: Dict[str, Dict[str, object]] = {}
    clicked_account_email_counter: Dict[str, Counter] = defaultdict(Counter)
    row_count = 0
    row_written = 0
    distinct_reports = set()
    min_dt = None
    max_dt = None

    with buynow_csv.open(newline="", encoding="utf-8-sig") as fh:
        reader = csv.reader(fh)
        header = next(reader, None)
        if not header:
            return {}, {"rows": 0, "rows_written": 0}

        idx_report = 0
        idx_created = 1
        idx_email = 11
        idx_buy_from = 22
        idx_click_id = 23
        idx_clicked_account = 29
        idx_clicked_dt = 30

        for row in reader:
            row_count += 1
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
            if min_dt is None or created < min_dt:
                min_dt = created
            if max_dt is None or created > max_dt:
                max_dt = created

            date_pt = to_pt_date(created, tz)
            slot = day_reports.setdefault(
                date_pt,
                {
                    "buy_from_reports": defaultdict(set),
                    "buy_from_clicks": Counter(),
                    "clicked_reports": defaultdict(set),
                    "clicked_clicks": Counter(),
                    "clicked_emails": Counter(),
                },
            )

            buy_from = normalize_nullable(row[idx_buy_from] if idx_buy_from < len(row) else "")
            clicked_account = normalize_nullable(row[idx_clicked_account] if idx_clicked_account < len(row) else "")
            email = normalize_nullable(row[idx_email] if idx_email < len(row) else "").lower()

            if buy_from:
                slot["buy_from_reports"][buy_from].add(report_id)
                slot["buy_from_clicks"][buy_from] += 1
            if clicked_account:
                slot["clicked_reports"][clicked_account].add(report_id)
                slot["clicked_clicks"][clicked_account] += 1
                if email:
                    slot["clicked_emails"][f"{clicked_account}|||{email}"] += 1
                    clicked_account_email_counter[clicked_account][email] += 1

            row_written += 1
            distinct_reports.add(report_id)

    clicked_account_email = pick_primary(clicked_account_email_counter)
    summary = {
        "rows": row_count,
        "rows_written": row_written,
        "distinct_report_ids": len(distinct_reports),
        "min_created_utc": min_dt.isoformat() if min_dt else None,
        "max_created_utc": max_dt.isoformat() if max_dt else None,
        "clicked_account_email_lookup": clicked_account_email,
    }
    return day_reports, summary


def to_daily_metrics(daily_sets: Dict[str, Dict[str, object]]) -> List[Dict[str, object]]:
    out: List[Dict[str, object]] = []
    for day in sorted(daily_sets.keys()):
        slot = daily_sets[day]
        out.append(
            {
                "date_pt": day,
                "scans": int(slot["scans"]),
                "unique_users": len(slot["users"]),
                "unique_vins": len(slot["vins"]),
            }
        )
    return out


def convert_scan_day_agg_for_json(day_agg: Dict[str, Dict[str, object]]) -> Dict[str, object]:
    out: Dict[str, object] = {"dates": {}}
    for day in sorted(day_agg.keys()):
        slot = day_agg[day]
        tool_stats = []
        for tool, vals in slot["tool_stats"].items():
            tool_stats.append([tool, int(vals[0]), int(vals[1]), int(vals[2]), int(vals[3])])
        out["dates"][day] = {
            "scans": int(slot["scans"]),
            "users": sorted(slot["users"].items()),
            "user_emails": sorted(slot["user_emails"].items()),
            "tool_stats": sorted(tool_stats, key=lambda row: row[0]),
            "vins": sorted(slot["vins"].items()),
            "account_vins": sorted(slot["account_vins"].items()),
            "zip_postal": sorted(slot["zip_postal"].items()),
            "zip5": sorted(slot["zip5"].items()),
        }
    return out


def convert_buynow_day_reports_for_json(day_reports: Dict[str, Dict[str, object]]) -> Dict[str, object]:
    out: Dict[str, object] = {"dates": {}}
    for day in sorted(day_reports.keys()):
        slot = day_reports[day]
        buy_from = []
        for key, reports in slot["buy_from_reports"].items():
            buy_from.append([key, sorted(reports), int(slot["buy_from_clicks"].get(key, 0))])

        clicked = []
        email_by_account: Dict[str, List[List[object]]] = defaultdict(list)
        for pair, count in slot["clicked_emails"].items():
            account, email = pair.split("|||", 1)
            email_by_account[account].append([email, int(count)])
        for key, reports in slot["clicked_reports"].items():
            clicked.append(
                [
                    key,
                    sorted(reports),
                    int(slot["clicked_clicks"].get(key, 0)),
                    sorted(email_by_account.get(key, []), key=lambda item: (-item[1], item[0])),
                ]
            )

        out["dates"][day] = {
            "buy_from": buy_from,
            "clicked_account": clicked,
        }
    return out


def write_geo_rows_if_missing(geo_csv: Path, zip_lookup: Dict[str, Dict[str, str]]) -> bool:
    existing_data_rows = 0
    if geo_csv.exists():
        with geo_csv.open(newline="", encoding="utf-8-sig") as fh:
            reader = csv.DictReader(fh)
            for _ in reader:
                existing_data_rows += 1
                if existing_data_rows > 0:
                    break
    if existing_data_rows > 0:
        return False

    by_zip5: Dict[str, Dict[str, str]] = {}
    for info in zip_lookup.values():
        zip5 = normalize_nullable(info.get("zip5", ""))
        if not ZIP5_RE.match(zip5):
            continue
        state = normalize_nullable(info.get("state", "")).upper()
        city = normalize_nullable(info.get("city", ""))
        lat = normalize_nullable(info.get("lat", ""))
        lng = normalize_nullable(info.get("lng", ""))
        if zip5 not in by_zip5:
            by_zip5[zip5] = {"zip5": zip5, "lat": lat, "lng": lng, "state": state, "city": city}
            continue
        if not by_zip5[zip5]["city"] and city:
            by_zip5[zip5]["city"] = city
        if not by_zip5[zip5]["state"] and state:
            by_zip5[zip5]["state"] = state
        if not by_zip5[zip5]["lat"] and lat:
            by_zip5[zip5]["lat"] = lat
        if not by_zip5[zip5]["lng"] and lng:
            by_zip5[zip5]["lng"] = lng

    for zip5, row in by_zip5.items():
        state = row.get("state", "")
        if not state:
            state = infer_state_from_zip5(zip5)
            row["state"] = state
        if (not row.get("lat") or not row.get("lng")) and state in STATE_CENTROIDS:
            lat, lng = centroid_for_zip(zip5, state)
            row["lat"] = f"{lat:.6f}"
            row["lng"] = f"{lng:.6f}"

    geo_csv.parent.mkdir(parents=True, exist_ok=True)
    with geo_csv.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=["zip5", "lat", "lng", "state", "city"])
        writer.writeheader()
        for zip5 in sorted(by_zip5.keys()):
            writer.writerow(by_zip5[zip5])
    return True


def make_prior_range(start: str, end: str) -> Dict[str, object]:
    start_d = date.fromisoformat(start)
    end_d = date.fromisoformat(end)
    days = (end_d - start_d).days + 1
    prior_end = start_d - timedelta(days=1)
    prior_start = prior_end - timedelta(days=days - 1)
    return {"start": prior_start.isoformat(), "end": prior_end.isoformat(), "days": days}


def compute_four_week_lift_summary(day_agg: Dict[str, Dict[str, object]]) -> Dict[str, object]:
    if not day_agg:
        return {"status": "empty"}

    max_date = max(date.fromisoformat(day) for day in day_agg.keys())
    recent_end = max_date
    recent_start = max_date - timedelta(days=27)
    prior_end = recent_start - timedelta(days=1)
    prior_start = prior_end - timedelta(days=27)

    def aggregate_window(start_d: date, end_d: date) -> Dict[str, object]:
        scans = 0
        users = set()
        vins = set()
        tools = Counter()
        zip5 = Counter()
        for day, slot in day_agg.items():
            d = date.fromisoformat(day)
            if d < start_d or d > end_d:
                continue
            scans += int(slot["scans"])
            users.update(slot["users"].keys())
            vins.update(slot["vins"].keys())
            for tool, vals in slot["tool_stats"].items():
                tools[tool] += int(vals[0])
            zip5.update(slot["zip5"])
        return {
            "scans": scans,
            "users": users,
            "vins": vins,
            "tools": tools,
            "zip5": zip5,
        }

    recent = aggregate_window(recent_start, recent_end)
    prior = aggregate_window(prior_start, prior_end)

    def top_delta(recent_counter: Counter, prior_counter: Counter, limit: int = 12) -> List[Dict[str, object]]:
        values = []
        for key in set(recent_counter.keys()) | set(prior_counter.keys()):
            r = int(recent_counter.get(key, 0))
            p = int(prior_counter.get(key, 0))
            values.append((r - p, key, r, p))
        values.sort(reverse=True)
        return [{"key": key, "delta": d, "recent": r, "prior": p} for d, key, r, p in values[:limit]]

    scan_lift_pct = ((recent["scans"] - prior["scans"]) / prior["scans"] * 100) if prior["scans"] else None
    return {
        "status": "ok",
        "windows": {
            "recent": {"start": recent_start.isoformat(), "end": recent_end.isoformat()},
            "prior": {"start": prior_start.isoformat(), "end": prior_end.isoformat()},
        },
        "summary": {
            "recent_scans": int(recent["scans"]),
            "prior_scans": int(prior["scans"]),
            "scan_delta": int(recent["scans"]) - int(prior["scans"]),
            "scan_lift_pct": scan_lift_pct,
            "recent_unique_users": len(recent["users"]),
            "prior_unique_users": len(prior["users"]),
            "recent_unique_vins": len(recent["vins"]),
            "prior_unique_vins": len(prior["vins"]),
        },
        "drivers": {
            "tools": top_delta(recent["tools"], prior["tools"]),
            "zip5": top_delta(recent["zip5"], prior["zip5"]),
            "buy_from": [],  # computed in API from buy click data
        },
    }


def take_top(counter: Counter, top_n: int) -> List[Tuple[str, int]]:
    return sorted(counter.items(), key=lambda item: (-item[1], item[0]))[:top_n]


def percentile_counter(counter: Counter) -> Dict[str, int]:
    entries = sorted(counter.items(), key=lambda item: item[1])
    if not entries:
        return {}
    if len(entries) == 1:
        return {entries[0][0]: 100}
    out: Dict[str, int] = {}
    for idx, (key, _) in enumerate(entries):
        out[key] = int(round((idx / (len(entries) - 1)) * 100))
    return out


def build_default_snapshot(
    scan_day_agg: Dict[str, Dict[str, object]],
    daily_metrics: List[Dict[str, object]],
    buynow_day_reports: Dict[str, Dict[str, object]],
    lookups: Dict[str, object],
    four_week_lift_summary: Dict[str, object],
    quality_generated_at_utc: str,
    top_unmapped_usb: List[Tuple[str, int]],
) -> Dict[str, object]:
    start = DEFAULT_START
    end = DEFAULT_END
    top_n = MAX_SNAPSHOT_TOP_N

    users = Counter()
    user_emails = Counter()
    user_max_day = Counter()
    tool_inspections = Counter()
    tool_abs = Counter()
    tool_srs = Counter()
    tool_cel = Counter()
    vins = Counter()
    account_vins = Counter()
    zip_postal = Counter()
    zip5 = Counter()
    scans = 0

    for day, slot in scan_day_agg.items():
        if day < start or day > end:
            continue
        scans += int(slot["scans"])
        for user, cnt in slot["users"].items():
            users[user] += cnt
            if cnt > user_max_day[user]:
                user_max_day[user] = cnt
        user_emails.update(slot["user_emails"])
        for tool, vals in slot["tool_stats"].items():
            tool_inspections[tool] += int(vals[0])
            tool_abs[tool] += int(vals[1])
            tool_srs[tool] += int(vals[2])
            tool_cel[tool] += int(vals[3])
        vins.update(slot["vins"])
        account_vins.update(slot["account_vins"])
        zip_postal.update(slot["zip_postal"])
        zip5.update(slot["zip5"])

    retailer_reports: Dict[str, set] = defaultdict(set)
    retailer_clicks = Counter()
    account_reports: Dict[str, set] = defaultdict(set)
    account_clicks = Counter()
    account_email = Counter()

    for day, slot in buynow_day_reports.items():
        if day < start or day > end:
            continue
        for buy_from, reports in slot["buy_from_reports"].items():
            retailer_reports[buy_from].update(reports)
            retailer_clicks[buy_from] += int(slot["buy_from_clicks"].get(buy_from, 0))
        for account, reports in slot["clicked_reports"].items():
            account_reports[account].update(reports)
            account_clicks[account] += int(slot["clicked_clicks"].get(account, 0))
        account_email.update(slot["clicked_emails"])

    user_lookup = lookups["user_lookup"]
    vin_lookup = lookups["vin_lookup"]
    account_vin_lookup = lookups["account_vin_lookup"]
    zip_lookup = lookups["zip_lookup"]
    clicked_account_email_lookup = lookups["clicked_account_email_lookup"]

    top_users = []
    for user_id, inspections in take_top(users, top_n):
        email = user_lookup.get(user_id, "")
        top_users.append(
            {
                "user_id": user_id,
                "user_email": email,
                "same_day_reports": int(user_max_day.get(user_id, 0)),
                "inspections": int(inspections),
            }
        )

    top_tools = []
    for tool_name, inspections in take_top(tool_inspections, top_n):
        top_tools.append(
            {
                "tool_name": tool_name,
                "inspections": int(inspections),
                "abs_parts": int(tool_abs.get(tool_name, 0)),
                "srs_parts": int(tool_srs.get(tool_name, 0)),
                "cel_parts": int(tool_cel.get(tool_name, 0)),
            }
        )

    top_vins = []
    for vin, inspections in take_top(vins, top_n):
        info = vin_lookup.get(vin, {"year": "", "make": "", "model": "", "engine": ""})
        top_vins.append(
            {
                "vin": vin,
                "vehicle_year": info.get("year", ""),
                "vehicle_make": info.get("make", ""),
                "vehicle_model": info.get("model", ""),
                "vehicle_engine": info.get("engine", ""),
                "inspections": int(inspections),
            }
        )

    top_account_vins = []
    for key, inspections in take_top(account_vins, top_n):
        account_id, vin = (key.split("|||") + ["", ""])[:2]
        info = account_vin_lookup.get(
            key, {"email": user_lookup.get(account_id, ""), "year": "", "make": "", "model": "", "engine": ""}
        )
        top_account_vins.append(
            {
                "account_id": account_id,
                "user_email": info.get("email", ""),
                "vin": vin,
                "vehicle_year": info.get("year", ""),
                "vehicle_make": info.get("make", ""),
                "vehicle_model": info.get("model", ""),
                "vehicle_engine": info.get("engine", ""),
                "inspections": int(inspections),
            }
        )

    top_zip = []
    for value, inspections in take_top(zip_postal, top_n):
        info = zip_lookup.get(value, {"city": "", "state": "", "zip5": ""})
        top_zip.append(
            {
                "zip_postal": value,
                "city": info.get("city", ""),
                "state": info.get("state", ""),
                "zip5": info.get("zip5", ""),
                "inspections": int(inspections),
            }
        )

    ltl_retailers = sorted(
        ((key, len(value), retailer_clicks.get(key, 0)) for key, value in retailer_reports.items()),
        key=lambda item: (-item[1], item[0]),
    )[:top_n]
    ltl_clicked = sorted(
        ((key, len(value), account_clicks.get(key, 0)) for key, value in account_reports.items()),
        key=lambda item: (-item[1], item[0]),
    )[:top_n]

    zip_percentiles = percentile_counter(zip_postal)
    heat_points: List[Dict[str, object]] = []
    for zip_postal_key, inspections in zip_postal.items():
        info = zip_lookup.get(zip_postal_key, {})
        zip5_value = normalize_nullable(info.get("zip5", ""))
        state = normalize_nullable(info.get("state", "")).upper()
        city = normalize_nullable(info.get("city", ""))
        lat_raw = normalize_nullable(info.get("lat", ""))
        lng_raw = normalize_nullable(info.get("lng", ""))
        lat = 0.0
        lng = 0.0
        try:
            lat = float(lat_raw) if lat_raw else 0.0
            lng = float(lng_raw) if lng_raw else 0.0
        except ValueError:
            lat = 0.0
            lng = 0.0
        if (not lat or not lng) and state in STATE_CENTROIDS and zip5_value:
            lat, lng = centroid_for_zip(zip5_value, state)
        if not lat or not lng:
            continue
        heat_points.append(
            {
                "zip_postal": zip_postal_key,
                "zip5": zip5_value or zip_postal_key,
                "city": city,
                "state": state,
                "lat": round(lat, 6),
                "lng": round(lng, 6),
                "scans": int(inspections),
                "percentile": int(zip_percentiles.get(zip_postal_key, 0)),
            }
        )
    heat_points.sort(key=lambda item: (-int(item["scans"]), str(item["zip_postal"])))
    heat_truncated = len(heat_points) > MAX_HEAT_POINTS
    heat_points_out = heat_points[:MAX_HEAT_POINTS] if heat_truncated else heat_points

    prior_range = make_prior_range(start, end)
    prior_scans = 0
    for item in daily_metrics:
        if prior_range["start"] <= item["date_pt"] <= prior_range["end"]:
            prior_scans += int(item["scans"])
    has_prior_data = prior_scans > 0
    scan_lift_pct = ((scans - prior_scans) / prior_scans * 100) if has_prior_data else None

    return {
        "meta": {
            "start": start,
            "end": end,
            "topN": top_n,
            "available_start": min(scan_day_agg.keys()) if scan_day_agg else start,
            "available_end": max(scan_day_agg.keys()) if scan_day_agg else end,
            "prior_equal_range": prior_range,
            "has_prior_data": has_prior_data,
            "snapshot_ready": True,
        },
        "overview": {
            "scans": scans,
            "unique_users": len(users),
            "unique_vins": len(vins),
            "prior_scans": prior_scans if has_prior_data else None,
            "scan_lift_pct_vs_prior": scan_lift_pct,
        },
        "dailyTrend": [item for item in daily_metrics if start <= item["date_pt"] <= end],
        "topUsers": top_users,
        "topTools": top_tools,
        "topVins": top_vins,
        "topAccountVins": top_account_vins,
        "topZipPostal": top_zip,
        "ltlRetailers": [
            {"buy_from": key, "inspections": int(inspections), "total_buy_clicks": int(clicks)}
            for key, inspections, clicks in ltl_retailers
        ],
        "ltlClickedAccounts": [
            {
                "account_id": key,
                "user_email": clicked_account_email_lookup.get(key, ""),
                "inspections": int(inspections),
                "total_buy_clicks": int(clicks),
            }
            for key, inspections, clicks in ltl_clicked
        ],
        "zipHeatPoints": heat_points_out,
        "zipHeatMeta": {
            "points_with_centroid": len(heat_points),
            "points_total_zip5": len(zip5),
            "truncated": heat_truncated,
            "missing_centroid_lookup": len(heat_points) == 0,
        },
        "fourWeekLift": four_week_lift_summary,
        "dataGaps": [
            {
                "question": "Vehicle Care section usage instances last month",
                "status": "blocked",
                "reason": "GA4/event telemetry extract not loaded in this dashboard workspace.",
                "required_source": "GA4 Vehicle Care event export",
            },
            {
                "question": "Scan limit policy by user/time frame",
                "status": "skipped",
                "reason": "Policy/config source intentionally not provided for this dashboard scope.",
                "required_source": "Policy/config documentation",
            },
            {
                "question": "Report retention duration / forever availability",
                "status": "skipped",
                "reason": "Policy/config source intentionally not provided for this dashboard scope.",
                "required_source": "Policy/config documentation",
            },
        ],
        "quality": {
            "generated_at_utc": quality_generated_at_utc,
            "mapping_fallback_usb937": lookups["mapping_fallback_usb937"],
            "ambiguous_mapping_keys": int(lookups["ambiguous_mapping_keys"]),
            "top_unmapped_usb": [{"usb_product_id": str(usb), "rows": int(rows)} for usb, rows in top_unmapped_usb],
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Build RS2 precomputed dashboard artifacts")
    parser.add_argument("--raw-dir", type=Path, default=None, help="Directory with RS2 raw CSV extracts")
    parser.add_argument("--mapping-xlsx", type=Path, default=None, help="Path to RS2 tool mapping workbook")
    parser.add_argument("--output-dir", type=Path, default=None, help="Output directory for RS2 artifacts")
    parser.add_argument("--geo-csv", type=Path, default=None, help="ZIP lookup CSV path (zip/zip5 + city/state + lat/lng)")
    parser.add_argument("--timezone", default="UTC", help="Timezone for daily buckets")
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parents[1]
    workspace_root = project_root.parent
    raw_dir = args.raw_dir or workspace_root / "raw_data" / "202602"
    mapping_xlsx = args.mapping_xlsx or workspace_root / "RS2tool_mapping" / "RS2_Tool_05122025_01.xlsx"
    output_dir = args.output_dir or project_root / "data" / "rs2"
    default_geo_source = workspace_root / "zipmap.csv"
    geo_source_csv = args.geo_csv or (default_geo_source if default_geo_source.exists() else project_root / "data" / "geo" / "us_zip_centroids.csv")
    geo_output_csv = project_root / "data" / "geo" / "us_zip_centroids.csv"
    tz = ZoneInfo(args.timezone)

    # Use 4-week scan extract as primary inspection source to cover 2026-01-01..2026-02-28.
    scan_csv = raw_dir / "4-week lift scan.csv"
    fix_csv = raw_dir / "fix-part raw extract.csv"
    buynow_csv = raw_dir / "BuyNow raw extract.csv"

    missing_inputs = [str(path) for path in [scan_csv, fix_csv, buynow_csv, mapping_xlsx] if not path.exists()]
    if missing_inputs:
        raise FileNotFoundError(f"Missing required inputs: {missing_inputs}")

    output_dir.mkdir(parents=True, exist_ok=True)
    ensure_geo_placeholder(geo_output_csv)

    mapping = load_mapping(mapping_xlsx)
    geo_lookup = load_geo_lookup(geo_source_csv)
    report_parts = build_report_part_counts(fix_csv)
    scan_day_agg, daily_sets, scan_summary_plus = process_scan_main(scan_csv, mapping, report_parts, geo_lookup, tz)
    buynow_day_reports, buynow_summary_plus = process_buynow_clicks(buynow_csv, tz)
    daily_metrics = to_daily_metrics(daily_sets)
    four_week_lift_summary = compute_four_week_lift_summary(scan_day_agg)

    generated_at = datetime.now(timezone.utc).isoformat()
    lookups = {
        "user_lookup": scan_summary_plus.pop("user_lookup"),
        "vin_lookup": scan_summary_plus.pop("vin_lookup"),
        "account_vin_lookup": scan_summary_plus.pop("account_vin_lookup"),
        "zip_lookup": scan_summary_plus.pop("zip_lookup"),
        "clicked_account_email_lookup": buynow_summary_plus.pop("clicked_account_email_lookup"),
        "mapping_fallback_usb937": mapping.blank_usb.get("937"),
        "ambiguous_mapping_keys": len(mapping.conflicts),
    }
    geo_bootstrapped = write_geo_rows_if_missing(geo_output_csv, lookups["zip_lookup"])

    snapshot = build_default_snapshot(
        scan_day_agg=scan_day_agg,
        daily_metrics=daily_metrics,
        buynow_day_reports=buynow_day_reports,
        lookups=lookups,
        four_week_lift_summary=four_week_lift_summary,
        quality_generated_at_utc=generated_at,
        top_unmapped_usb=scan_summary_plus.get("top_unmapped_usb", []),
    )

    quality = {
        "generated_at_utc": generated_at,
        "timezone_for_daily_buckets": args.timezone,
        "inputs": {
            "raw_dir": str(raw_dir),
            "scan_csv": str(scan_csv),
            "fix_csv": str(fix_csv),
            "buynow_csv": str(buynow_csv),
            "mapping_xlsx": str(mapping_xlsx),
            "geo_source_csv": str(geo_source_csv),
            "geo_output_csv": str(geo_output_csv),
        },
        "row_counts": {
            "scan_level": scan_summary_plus,
            "buynow_clicks": buynow_summary_plus,
        },
        "mapping": {
            "exact_key_count": len(mapping.exact),
            "blank_usb_key_count": len(mapping.blank_usb),
            "geo_csv_bootstrapped": geo_bootstrapped,
            "ambiguous_mapping_keys": [
                {"usb_product_id": key[0], "customer_id": key[1], "display_names": names}
                for key, names in sorted(mapping.conflicts.items())
            ],
            "fallback_example_usb_937_blank_customer": mapping.blank_usb.get("937"),
        },
    }

    (output_dir / "scan_day_agg.json").write_text(
        json.dumps(convert_scan_day_agg_for_json(scan_day_agg), ensure_ascii=True, separators=(",", ":")),
        encoding="utf-8",
    )
    (output_dir / "buynow_click_day_reports.json").write_text(
        json.dumps(convert_buynow_day_reports_for_json(buynow_day_reports), ensure_ascii=True, separators=(",", ":")),
        encoding="utf-8",
    )
    (output_dir / "daily_scan_metrics.json").write_text(
        json.dumps(daily_metrics, ensure_ascii=True, separators=(",", ":")),
        encoding="utf-8",
    )
    (output_dir / "four_week_lift_summary.json").write_text(
        json.dumps(four_week_lift_summary, ensure_ascii=True, separators=(",", ":")),
        encoding="utf-8",
    )
    (output_dir / "snapshot_default.json").write_text(
        json.dumps(snapshot, ensure_ascii=True, separators=(",", ":")),
        encoding="utf-8",
    )
    (output_dir / "lookups.json").write_text(
        json.dumps(lookups, ensure_ascii=True, separators=(",", ":")),
        encoding="utf-8",
    )
    (output_dir / "quality_audit.json").write_text(
        json.dumps(quality, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )

    # Cleanup legacy artifacts.
    for legacy in [
        output_dir / "scan_fact_min.jsonl",
        output_dir / "scan_4week_fact_min.jsonl",
        output_dir / "buynow_click_fact_min.jsonl",
        output_dir / "daily_scan_metrics_4week.json",
    ]:
        if legacy.exists():
            legacy.unlink()

    print(f"Wrote RS2 artifacts to: {output_dir}")


if __name__ == "__main__":
    main()
