#!/usr/bin/env python3
# vim: set ts=4 sw=4 ts=4 et :

import argparse
import csv
import json
import logging
import re
import time
import datetime
import os
from typing import List, Dict, Any

from bs4 import BeautifulSoup, Tag
import requests

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape national and state gas prices."
    )
    parser.add_argument(
        "--debug",
        help="Enable debug logging",
        action="store_true",
    )
    parser.add_argument(
        "-f",
        "--filepath",
        help="Base filepath to store data",
        default="prices/",
    )

    return parser.parse_args()


def get_national_prices(soup: BeautifulSoup) -> List[List[str]]:
    """
    Extract national gas price data from the final <script> tag.

    Returns:
        List of lists, each with [State Abbr, State Name, Gas Price, State Page URL].
    """
    script_tags: List[Tag] = soup.find_all("script")
    for script in reversed(script_tags):
        for line in script:
            if "iwmparam[0].placestxt" in line:
                match = re.search(r'iwmparam\[0\].placestxt\s*=\s*"(.*)"', line)
                if match:
                    raw_data: List[str] = match.group(1).strip().split(";")[:-1]
                    return [entry.strip().split(",")[:-1] for entry in raw_data]

    return []


def get_state_prices(national_prices: List[List[str]]) -> List[List[str]]:
    """
    Retrieve county-level gas prices for each state.

    Args:
        national_prices: List of state-level data.

    Returns:
        List of county-level gas price records: [State Abbr, State Name, County Name, Comment].
    """
    all_prices: List[List[str]] = []

    with requests.Session() as session:
        session.headers = {"User-Agent": "insomnia/2022.4.2"}

        for state_data in national_prices:
            if len(state_data) < 4:
                continue

            state_abbr, state_name, *_, state_url = state_data

            if state_abbr == "DC":
                continue

            resp = session.get(state_url)
            soup = BeautifulSoup(resp.text, "html.parser")

            script_tag = soup.find("script", src=re.compile(r"premiumhtml5map_js_data"))
            if not script_tag:
                log.warning(f"No map script found for {state_name}")
                continue

            data_url: str = script_tag["src"]
            resp = session.get(data_url)

            map_data: Dict[str, Dict[str, Any]] = {}
            for line in resp.text.strip().split("\n"):
                if "map_data" in line:
                    match = re.search(r"map_data\s*:\s*({.*?),\s*groups", line)
                    if match:
                        map_data = json.loads(match.group(1).strip())
                        break

            for county in map_data.values():
                all_prices.append(
                    [
                        state_abbr,
                        state_name,
                        county.get("name", ""),
                        county.get("comment", ""),
                    ]
                )

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

    base_url: str = "https://gasprices.aaa.com"
    headers: Dict[str, str] = {"User-Agent": "insomnia/2022.4.2"}

    response = requests.get(base_url, headers=headers)
    soup = BeautifulSoup(response.text, "html.parser")

    national_prices: List[List[str]] = get_national_prices(soup)
    state_prices: List[List[str]] = get_state_prices(national_prices)

    base_path: str = args.filepath
    national_path: str = base_path + "/national/"
    state_path: str = base_path + "/states/"

    print(os.path.dirname(national_path))
    os.makedirs(os.path.dirname(national_path), exist_ok=True)
    os.makedirs(os.path.dirname(state_path), exist_ok=True)

    timestamp: str = datetime.datetime.now(datetime.timezone.utc).isoformat()
    save_prices_to_csv(f"{national_path}/{timestamp}.csv", national_prices)
    save_prices_to_csv(f"{state_path}/{timestamp}.csv", state_prices)

    elapsed: float = time.perf_counter() - start_time
    log.info(f"{__file__} executed in {elapsed:.5f} seconds.")


if __name__ == "__main__":
    main()
