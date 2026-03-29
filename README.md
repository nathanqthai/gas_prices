<!-- vim: set tabstop=2 expandtab softtabstop=2 shiftwidth=2:-->

[![scraper](https://github.com/nathanqthai/gas_prices/actions/workflows/scraper.yml/badge.svg?branch=main)](https://github.com/nathanqthai/gas_prices/actions/workflows/scraper.yml)

# Gas Prices

Daily U.S. gas price data collected via automated scraper. Includes state-level
averages and sub-state (county or metro) prices, plus weekly regional data from
the EIA.

## Data

- **Date range:** 2022-09-30 to present
- **National prices:** 51 states + DC, one price per state per day
- **Sub-state prices:** County-level (AAA, through 2026-03-27) and metro-level (GasBuddy, 2026-03-29 onward)
- **Regional prices:** Weekly EIA data for PADD regions, select states, and metro areas (2022-01-03 onward)

### Sources

| Period | Source | Data | Frequency | Notes |
|--------|--------|------|-----------|-------|
| 2022-09-30 to 2026-03-27 | [AAA Gas Prices](https://gasprices.aaa.com) | 51 states + DC, ~3,100 counties | Daily | County-level averages from AAA's map widget |
| 2026-03-29 onward | [GasBuddy](https://www.gasbuddy.com/usa) | 52 states, ~166 metros | Daily | Metro-area averages; AAA added Cloudflare blocking |
| 2022-01-03 onward | [EIA](https://www.eia.gov/opendata/bulk/PET.zip) | U.S. avg, 10 PADD regions, 9 states, 10 metros | Weekly | Bulk petroleum data; added 2026-03-29 |

**Scraper source changed on 2026-03-29** from AAA to GasBuddy due to Cloudflare
Turnstile bot protection being added to the AAA website. The original AAA scraper
is preserved in `archive/aaa_scraper.py`.

**EIA regional data added on 2026-03-29** using the EIA bulk petroleum download.
Provides weekly retail gasoline prices (all grades, all formulations) for PADD
regions, select states (CA, CO, FL, MA, MN, NY, OH, TX, WA), and 10 metro areas.
Updated weekly via GitHub Actions.

### Missing data

The following dates have no data (scraper failures or source outages):

| Date(s) | Reason |
|---------|--------|
| 2023-12-18 | Scraper failure |
| 2023-12-30 | Scraper failure |
| 2024-02-05 | Scraper failure |
| 2024-04-29 | Scraper failure |
| 2025-05-29 | Scraper failure |
| 2025-06-13 | Scraper failure |
| 2025-06-17 to 2025-06-21 | Scraper broken, repaired on 2025-06-22 |
| 2026-03-28 | AAA Cloudflare blocking; scraper returned empty data |

State-level data additionally has empty files for 2025-06-17 through 2025-06-22
(6 files).

## Dashboard

A Flask-based dashboard for browsing and visualizing the data. See `dashboard/`.

```bash
make venv       # create virtualenv, install deps
make db         # run ETL to build SQLite database
make dashboard  # run dashboard locally at http://localhost:8080
make docker     # run dashboard in Docker
```

## Events CSV

The dashboard supports uploading a CSV of events to overlay on the chart as
vertical markers. Each event appears in a collapsible panel below the chart with
the national average price and day-over-day change.

### Schema

| Column | Required | Description |
|--------|----------|-------------|
| `date` | Yes | Event date in `YYYY-MM-DD` format |
| `title` | Yes | Short headline (displayed in the event list) |
| `description` | No | Full text shown when the event is expanded |
| `source` | No | URL linking to the original source (must be `http://` or `https://`) |

Column order is flexible — the parser matches by header name, not position.
Fields containing commas, newlines, or quotes must be quoted per RFC 4180.

### Example

```csv
date,title,description,source
2025-01-20,Trump inaugurated,Donald Trump sworn in as 47th President,https://en.wikipedia.org/wiki/Inauguration_of_Donald_Trump
2025-04-02,Reciprocal tariffs announced,Executive order on reciprocal tariffs,https://www.presidency.ucsb.edu/documents/app-categories/written-presidential-orders/presidential/executive-orders
2026-02-28,Iran war begins,US-Israeli airstrikes begin against Iran,https://en.wikipedia.org/wiki/Timeline_of_the_2026_Iran_war#28_February
```

## Project structure

```
gas_prices/
  scraper.py              # Active scraper (GasBuddy)
  scrapers/
    eia_bulk_scraper.py   # EIA bulk data scraper
  archive/
    aaa_scraper.py        # Original AAA scraper (archived)
  utilities/
    gas_etl.py            # CSV -> SQLite ETL
  dashboard/
    dashboard.py          # Flask app
    static/               # CSS + JS
    templates/            # HTML
    Dockerfile
  prices/
    national/             # Daily state-level CSVs
    states/               # Daily county/metro-level CSVs
    regions/              # Weekly EIA regional CSVs
  Makefile
```
