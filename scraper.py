#!/usr/bin/env python3
"""
Scrape national and state/metro gas prices from GasBuddy.

Collects:
  - National prices: one row per state (abbr, name, price, source URL)
  - State prices: metro/city-level prices per state (abbr, name, city, price)

Usage:
    python scraper.py
    python scraper.py --debug
    python scraper.py -f prices/
"""

import argparse
import csv
import datetime
import logging
import os
import re
import sys
import time
from typing import List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

BASE_URL = "https://www.gasbuddy.com"
NATIONAL_URL = f"{BASE_URL}/usa"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/134.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
}

# Map GasBuddy's two-letter state codes to standard abbreviations and full names.
# GasBuddy uses lowercase in URLs: /usa/ca, /usa/tx, etc.
# Most match standard USPS codes; DC = /usa/dc.
STATE_NAMES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DC": "District of Columbia",
    "DE": "Delaware", "FL": "Florida", "GA": "Georgia", "HI": "Hawaii",
    "IA": "Iowa", "ID": "Idaho", "IL": "Illinois", "IN": "Indiana",
    "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana", "MA": "Massachusetts",
    "MD": "Maryland", "ME": "Maine", "MI": "Michigan", "MN": "Minnesota",
    "MO": "Missouri", "MS": "Mississippi", "MT": "Montana", "NC": "North Carolina",
    "ND": "North Dakota", "NE": "Nebraska", "NH": "New Hampshire", "NJ": "New Jersey",
    "NM": "New Mexico", "NV": "Nevada", "NY": "New York", "OH": "Ohio",
    "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island",
    "SC": "South Carolina", "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas",
    "UT": "Utah", "VA": "Virginia", "VT": "Vermont", "WA": "Washington",
    "WI": "Wisconsin", "WV": "West Virginia", "WY": "Wyoming",
}

# GasBuddy uses full state names in /gasprices/ URLs
STATE_SLUGS = {
    abbr: name.lower().replace(" ", "-")
    for abbr, name in STATE_NAMES.items()
    if abbr != "DC"  # DC doesn't have a county page
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape gas prices from GasBuddy.")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("-f", "--filepath", default="prices/", help="Base filepath to store data")
    return parser.parse_args()


def fetch(url: str, session: requests.Session) -> Optional[BeautifulSoup]:
    """Fetch a page and return parsed HTML, or None on failure."""
    try:
        resp = session.get(url, timeout=15)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "html.parser")
    except requests.RequestException as e:
        log.warning(f"Failed to fetch {url}: {e}")
        return None


def scrape_national_prices(session: requests.Session) -> List[List[str]]:
    """
    Scrape state-level gas prices from the GasBuddy national page.
    Returns rows of [state_abbr, state_name, price, source_url].
    """
    soup = fetch(NATIONAL_URL, session)
    if not soup:
        return []

    results: List[List[str]] = []
    links = soup.find_all("a", href=re.compile(r"^/usa/[a-z]{2}$"))

    for link in links:
        href = link["href"]
        abbr = href.split("/")[-1].upper()
        text = link.get_text(strip=True)

        # Text is like "Oklahoma3.208-0.016" or "California5.838+0.012"
        m = re.match(r"^(.+?)(\d+\.\d{3})", text)
        if not m:
            log.debug(f"Could not parse price from: {text!r}")
            continue

        name = m.group(1).strip()
        price = m.group(2)
        source_url = f"{BASE_URL}{href}"

        results.append([abbr, name, f"${price}", source_url])

    return results


def scrape_state_prices(
    national_prices: List[List[str]], session: requests.Session
) -> List[List[str]]:
    """
    Scrape metro/city-level gas prices for each state.
    Returns rows of [state_abbr, state_name, city, price, source_url].
    """
    all_prices: List[List[str]] = []

    for state_row in national_prices:
        abbr = state_row[0]
        name = state_row[1]

        if abbr == "DC":
            continue

        slug = STATE_SLUGS.get(abbr)
        if not slug:
            log.debug(f"No slug for {abbr}, skipping county scrape")
            continue

        state_url = f"{BASE_URL}/usa/{abbr.lower()}"
        log.info(f"Scraping {name} ({abbr})...")

        soup = fetch(state_url, session)
        if not soup:
            continue

        # Metro prices are in <a> tags whose text is just "X.XXX".
        # The city name is in a parent container: "CityName|price|change".
        price_links = [
            a for a in soup.find_all("a")
            if re.match(r"^\d\.\d{3}$", a.get_text(strip=True))
        ]

        for pl in price_links:
            price = pl.get_text(strip=True)
            row = pl
            for _ in range(8):
                row = row.parent
                if row is None:
                    break
                text = row.get_text("|", strip=True)
                parts = [p.strip() for p in text.split("|")]
                if len(parts) >= 2 and parts[1] == price:
                    city = parts[0]
                    all_prices.append([abbr, name, city, f"${price}", state_url])
                    break

    return all_prices


def save_prices_to_csv(filename: str, data: List[List[str]]) -> None:
    with open(filename, "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerows(data)


def main() -> None:
    args = parse_args()

    if args.debug:
        log.setLevel(logging.DEBUG)
        log.debug("Debug mode enabled")

    log.info(f"Running {__file__}")
    start_time: float = time.perf_counter()

    session = requests.Session()
    session.headers.update(HEADERS)

    # National prices
    log.info("Fetching national prices...")
    national_prices = scrape_national_prices(session)

    if not national_prices:
        log.error(
            "FATAL: No national price data collected. "
            "GasBuddy may be blocking requests or its structure changed."
        )
        sys.exit(1)

    log.info(f"Found {len(national_prices)} state prices")

    # State/metro prices
    log.info("Fetching metro-level prices...")
    state_prices = scrape_state_prices(national_prices, session)
    log.info(f"Found {len(state_prices)} metro prices")

    if not state_prices:
        log.error("FATAL: No metro-level price data collected.")
        sys.exit(1)

    # Save
    base_path: str = args.filepath
    national_path: str = os.path.join(base_path, "national")
    state_path: str = os.path.join(base_path, "states")

    os.makedirs(national_path, exist_ok=True)
    os.makedirs(state_path, exist_ok=True)

    timestamp: str = datetime.datetime.now(datetime.timezone.utc).isoformat()
    save_prices_to_csv(os.path.join(national_path, f"{timestamp}.csv"), national_prices)
    save_prices_to_csv(os.path.join(state_path, f"{timestamp}.csv"), state_prices)

    elapsed: float = time.perf_counter() - start_time
    log.info(f"{__file__} executed in {elapsed:.2f} seconds.")


if __name__ == "__main__":
    main()
