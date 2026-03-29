#!/usr/bin/env python3
"""
Extract weekly retail gasoline prices from the EIA bulk data file (PET.txt).

Downloads PET.zip from EIA, extracts all weekly retail gasoline price series
(state-level, regional, national, and metro), and writes CSVs compatible
with the gas_etl.py pipeline.

Usage:
    python scrapers/eia_bulk_scraper.py
    python scrapers/eia_bulk_scraper.py --no-download        # reuse existing PET.txt
    python scrapers/eia_bulk_scraper.py --start 2022-01-01   # filter by start date
    python scrapers/eia_bulk_scraper.py -o eia_prices/       # custom output dir

Data source: https://www.eia.gov/opendata/bulk/PET.zip
Series pattern: PET.EMM_EPM0_PTE_*_DPG.W (weekly retail gasoline, all grades)

Requires: requests
"""

import argparse
import csv
import json
import logging
import os
import re
import sys
import zipfile
from typing import Optional

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

BULK_URL = "https://www.eia.gov/opendata/bulk/PET.zip"
DATA_DIR = "data"
ZIP_PATH = os.path.join(DATA_DIR, "PET.zip")
TXT_PATH = os.path.join(DATA_DIR, "PET.txt")

# Match weekly retail gasoline "all grades all formulations" series
SERIES_RE = re.compile(r"^PET\.EMM_EPM0_PTE_(.+)_DPG\.W$")

# Map EIA area codes to human-readable names and types
# S** = individual state, R** = PADD region, NUS = national, Y** = metro
STATE_CODES = {
    "SCA": ("CA", "California"),
    "SCO": ("CO", "Colorado"),
    "SFL": ("FL", "Florida"),
    "SMA": ("MA", "Massachusetts"),
    "SMN": ("MN", "Minnesota"),
    "SNY": ("NY", "New York"),
    "SOH": ("OH", "Ohio"),
    "STX": ("TX", "Texas"),
    "SWA": ("WA", "Washington"),
}

REGION_CODES = {
    "NUS":   ("US",       "U.S. Average"),
    "R10":   ("PADD1",    "East Coast"),
    "R1X":   ("PADD1A",   "New England"),
    "R1Y":   ("PADD1B",   "Central Atlantic"),
    "R1Z":   ("PADD1C",   "Lower Atlantic"),
    "R20":   ("PADD2",    "Midwest"),
    "R30":   ("PADD3",    "Gulf Coast"),
    "R40":   ("PADD4",    "Rocky Mountain"),
    "R50":   ("PADD5",    "West Coast"),
    "R5XCA": ("PADD5X",   "West Coast ex-CA"),
}

METRO_CODES = {
    "Y05LA": ("LA",  "Los Angeles"),
    "Y05SF": ("SF",  "San Francisco"),
    "Y35NY": ("NYC", "New York City"),
    "Y44HO": ("HOU", "Houston"),
    "Y48SE": ("SEA", "Seattle"),
    "YBOS":  ("BOS", "Boston"),
    "YCLE":  ("CLE", "Cleveland"),
    "YDEN":  ("DEN", "Denver"),
    "YMIA":  ("MIA", "Miami"),
    "YORD":  ("CHI", "Chicago"),
}


def download_bulk_file() -> None:
    """Download and extract the EIA bulk petroleum data file."""
    os.makedirs(DATA_DIR, exist_ok=True)
    log.info("Downloading %s ...", BULK_URL)
    resp = requests.get(BULK_URL, stream=True, timeout=120)
    resp.raise_for_status()
    with open(ZIP_PATH, "wb") as f:
        for chunk in resp.iter_content(chunk_size=1 << 20):
            f.write(chunk)
    size_mb = os.path.getsize(ZIP_PATH) / 1_048_576
    log.info("Downloaded %.1f MB", size_mb)

    log.info("Extracting...")
    with zipfile.ZipFile(ZIP_PATH, "r") as zf:
        zf.extractall(DATA_DIR)
    log.info("Extracted %s", TXT_PATH)


def parse_date(eia_date: str) -> str:
    """Convert EIA date format '20260323' to 'YYYY-MM-DD'."""
    return f"{eia_date[:4]}-{eia_date[4:6]}-{eia_date[6:8]}"


def extract_weekly_prices(start_date: Optional[str] = None) -> dict:
    """
    Read PET.txt and extract all weekly retail gasoline price series.

    Returns a dict keyed by area code with:
        {
            "area_code": str,
            "area_name": str,
            "area_type": str,  # "state", "region", "metro", "national"
            "series_id": str,
            "data": [(date_str, price), ...],  # sorted by date
        }
    """
    log.info("Reading %s ...", TXT_PATH)
    results = {}
    matched = 0
    total = 0

    with open(TXT_PATH, "r", encoding="utf-8") as f:
        for line in f:
            total += 1
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue

            series_id = record.get("series_id", "")
            m = SERIES_RE.match(series_id)
            if not m:
                continue

            area_code = m.group(1)

            # Classify the area
            if area_code in STATE_CODES:
                abbr, name = STATE_CODES[area_code]
                area_type = "state"
            elif area_code in REGION_CODES:
                abbr, name = REGION_CODES[area_code]
                area_type = "national" if area_code == "NUS" else "region"
            elif area_code in METRO_CODES:
                abbr, name = METRO_CODES[area_code]
                area_type = "metro"
            else:
                log.debug("Unknown area code: %s", area_code)
                continue

            # Extract date/price pairs
            raw_data = record.get("data", [])
            data_points = []
            for point in raw_data:
                if len(point) < 2 or point[1] is None:
                    continue
                date_str = parse_date(str(point[0]))
                if start_date and date_str < start_date:
                    continue
                try:
                    price = float(point[1])
                except (ValueError, TypeError):
                    continue
                data_points.append((date_str, price))

            data_points.sort(key=lambda x: x[0])

            results[area_code] = {
                "area_code": abbr,
                "area_name": name,
                "area_type": area_type,
                "series_id": series_id,
                "data": data_points,
            }
            matched += 1

    log.info("Scanned %d records, matched %d weekly gasoline series", total, matched)
    return results


def write_output(results: dict, output_dir: str, regions_dir: str) -> None:
    """Write extracted data as summary CSVs and per-week region CSVs."""
    os.makedirs(output_dir, exist_ok=True)

    # ── Summary CSVs (data/eia/) ────────────────────────────────────────

    national_path = os.path.join(output_dir, "eia_weekly_national.csv")
    with open(national_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "area_code", "area_name", "area_type", "price", "series_id"])
        for area in sorted(results.values(), key=lambda x: (x["area_type"], x["area_code"])):
            if area["area_type"] in ("state", "region", "national"):
                for date_str, price in area["data"]:
                    writer.writerow([
                        date_str, area["area_code"], area["area_name"],
                        area["area_type"], f"{price:.3f}", area["series_id"],
                    ])
    log.info("Written national/state/region data to %s", national_path)

    metro_path = os.path.join(output_dir, "eia_weekly_metro.csv")
    with open(metro_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "metro_code", "metro_name", "price", "series_id"])
        for area in sorted(results.values(), key=lambda x: x["area_code"]):
            if area["area_type"] == "metro":
                for date_str, price in area["data"]:
                    writer.writerow([
                        date_str, area["area_code"], area["area_name"],
                        f"{price:.3f}", area["series_id"],
                    ])
    log.info("Written metro data to %s", metro_path)

    # ── Per-week region CSVs (prices/regions/) ──────────────────────────
    # One CSV per week date, matching the scraper output pattern.
    # Format: area_code, area_name, price, source_url

    os.makedirs(regions_dir, exist_ok=True)

    # Collect all region/national/state data grouped by date
    by_date: dict[str, list] = {}
    for area in results.values():
        for date_str, price in area["data"]:
            if date_str not in by_date:
                by_date[date_str] = []
            sid_parts = area['series_id'].split('_')
            eia_code = sid_parts[3].lower() if len(sid_parts) > 3 else "nus"
            source = f"https://www.eia.gov/dnav/pet/pet_pri_gnd_dcus_{eia_code}_w.htm"
            by_date[date_str].append([
                area["area_code"],
                area["area_name"],
                area["area_type"],
                f"${price:.3f}",
                source,
            ])

    existing = set(os.listdir(regions_dir))
    written = 0
    skipped = 0
    for date_str, rows in sorted(by_date.items()):
        filename = f"{date_str}T12:00:00+00:00.csv"
        if filename in existing:
            skipped += 1
            continue
        filepath = os.path.join(regions_dir, filename)
        rows.sort(key=lambda r: (r[2], r[0]))  # sort by type, then code
        with open(filepath, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(rows)
        written += 1

    log.info("Written %d weekly CSVs to %s (%d skipped, already exist)", written, regions_dir, skipped)

    # Summary
    total_points = sum(len(a["data"]) for a in results.values())
    log.info("Total: %d series, %d data points", len(results), total_points)

    for area in sorted(results.values(), key=lambda x: (x["area_type"], x["area_code"])):
        if area["data"]:
            start = area["data"][0][0]
            end = area["data"][-1][0]
            log.info("  %-8s %-20s %s to %s (%d points)",
                     area["area_code"], area["area_name"], start, end, len(area["data"]))


def main() -> None:
    ap = argparse.ArgumentParser(description="Extract weekly gasoline prices from EIA bulk data")
    ap.add_argument("--no-download", action="store_true", help="Skip download, reuse existing PET.txt")
    ap.add_argument("--start", default=None, help="Start date filter (YYYY-MM-DD)")
    ap.add_argument("-o", "--output", default="data/eia", help="Summary CSV output directory")
    ap.add_argument("--regions-dir", default="prices/regions", help="Per-week region CSV directory")
    args = ap.parse_args()

    if not args.no_download or not os.path.exists(TXT_PATH):
        download_bulk_file()
    else:
        log.info("Reusing existing %s", TXT_PATH)

    results = extract_weekly_prices(start_date=args.start)

    if not results:
        log.error("FATAL: No weekly gasoline price series found in bulk data.")
        sys.exit(1)

    write_output(results, args.output, args.regions_dir)


if __name__ == "__main__":
    main()
