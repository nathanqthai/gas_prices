#!/usr/bin/env python3
"""
Reads the cloned nathanqthai/gas_prices repo and loads all CSVs into SQLite.
Safe to re-run: already-loaded files are skipped, new files are appended.

Usage:
    python gas_etl.py
    python gas_etl.py --repo ./gas_prices --db gas_prices.db
    python gas_etl.py --workers 16
    python gas_etl.py --dry-run
    python gas_etl.py --log-level DEBUG
"""

import argparse
import csv
import io
import logging
import os
import re
import sqlite3
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import NamedTuple, Optional

log = logging.getLogger(__name__)

# —— Constants ————————————————————————————————————————————————————————————————

SNAP_NATIONAL = "national"
SNAP_STATES   = "states"
SNAP_REGIONS  = "regions"
SNAP_TYPES    = (SNAP_NATIONAL, SNAP_STATES, SNAP_REGIONS)

# —— Named types ——————————————————————————————————————————————————————————————

class Job(NamedTuple):
    path_str:  str
    snap_type: str   # SNAP_NATIONAL | SNAP_STATES
    snap_date: str   # YYYY-MM-DD
    rel_path:  str   # repo-relative, used as the stable DB key


class ParseResult(NamedTuple):
    snap_type: str
    nat_rows:  list   # list[tuple] — rows for national_prices
    st_rows:   list   # list[tuple] — rows for state_prices
    reg_rows:  list   # list[tuple] — rows for regional_prices
    error:     Optional[str]


class TrackingRow(NamedTuple):
    file_path: str
    snap_date: str
    snap_type: str
    row_count: int
    loaded_at: str


# —— Schema ———————————————————————————————————————————————————————————————————

SCHEMA = """
CREATE TABLE IF NOT EXISTS loaded_files (
    file_path TEXT    PRIMARY KEY,
    snap_date TEXT    NOT NULL,
    snap_type TEXT    NOT NULL,
    row_count INTEGER NOT NULL,
    loaded_at TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS national_prices (
    id         INTEGER PRIMARY KEY,
    snap_date  TEXT NOT NULL,
    state_abbr TEXT NOT NULL,
    state_name TEXT,
    price      REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS state_prices (
    id         INTEGER PRIMARY KEY,
    snap_date  TEXT NOT NULL,
    state_abbr TEXT NOT NULL,
    state_name TEXT,
    county     TEXT,
    price      REAL
);

CREATE TABLE IF NOT EXISTS regional_prices (
    id         INTEGER PRIMARY KEY,
    snap_date  TEXT NOT NULL,
    area_code  TEXT NOT NULL,
    area_name  TEXT,
    area_type  TEXT NOT NULL,
    price      REAL NOT NULL
);
"""

INDEXES_DDL = """
CREATE UNIQUE INDEX IF NOT EXISTS idx_nat_unique
    ON national_prices(snap_date, state_abbr);

CREATE UNIQUE INDEX IF NOT EXISTS idx_st_unique
    ON state_prices(snap_date, state_abbr, COALESCE(county, ''));

CREATE INDEX IF NOT EXISTS idx_nat_date ON national_prices(snap_date);
CREATE INDEX IF NOT EXISTS idx_nat_abbr ON national_prices(state_abbr);
CREATE INDEX IF NOT EXISTS idx_st_date  ON state_prices(snap_date);
CREATE INDEX IF NOT EXISTS idx_st_abbr  ON state_prices(state_abbr);

CREATE UNIQUE INDEX IF NOT EXISTS idx_reg_unique
    ON regional_prices(snap_date, area_code);

CREATE INDEX IF NOT EXISTS idx_reg_date ON regional_prices(snap_date);
CREATE INDEX IF NOT EXISTS idx_reg_type ON regional_prices(area_type);
"""

FRESH_DB_PRAGMAS = """
PRAGMA journal_mode=OFF;
PRAGMA synchronous=OFF;
PRAGMA cache_size=-512000;
PRAGMA temp_store=MEMORY;
"""

INCREMENTAL_PRAGMAS = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA cache_size=-512000;
PRAGMA temp_store=MEMORY;
"""

SAFE_PRAGMAS = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
"""

DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")


# —— Helpers ——————————————————————————————————————————————————————————————————

def parse_price(s: str) -> Optional[float]:
    """Parse a price string like '$3.459' or '3.459' into a float."""
    s = s.strip().lstrip("$")
    if not s:
        return None
    try:
        v = float(s)
        return v if v > 0 else None
    except ValueError:
        return None


def parse_date(filename: str) -> Optional[str]:
    m = DATE_RE.search(filename)
    return m.group(1) if m else None


def db_is_fresh(con: sqlite3.Connection) -> bool:
    row = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='loaded_files'"
    ).fetchone()
    return row is None


def already_loaded(con: sqlite3.Connection) -> set[str]:
    rows = con.execute("SELECT file_path FROM loaded_files").fetchall()
    return {r[0] for r in rows}


# —— Worker (runs in a child process — no logging) ———————————————————————————

def parse_file(job: Job) -> ParseResult:
    try:
        with open(job.path_str, newline="", encoding="utf-8", errors="replace") as f:
            raw = f.read()

        nat_rows: list[tuple] = []
        st_rows:  list[tuple] = []
        reg_rows: list[tuple] = []
        reader = csv.reader(io.StringIO(raw))

        if job.snap_type == SNAP_NATIONAL:
            for row in reader:
                if len(row) < 3:
                    continue
                abbr = row[0].strip()
                name = row[1].strip()
                price = parse_price(row[2])
                if abbr and price is not None:
                    nat_rows.append((job.snap_date, abbr, name, price))

        elif job.snap_type == SNAP_STATES:
            for row in reader:
                if len(row) < 3:
                    continue
                abbr   = row[0].strip()
                name   = row[1].strip()
                county = row[2].strip()
                price  = parse_price(row[3]) if len(row) > 3 else None
                if abbr:
                    st_rows.append((job.snap_date, abbr, name, county, price))

        elif job.snap_type == SNAP_REGIONS:
            # Format: area_code, area_name, area_type, price, source_url
            for row in reader:
                if len(row) < 4:
                    continue
                area_code = row[0].strip()
                area_name = row[1].strip()
                area_type = row[2].strip()
                price = parse_price(row[3])
                if area_code and price is not None:
                    reg_rows.append((job.snap_date, area_code, area_name, area_type, price))

        else:
            return ParseResult(job.snap_type, [], [], [], f"Unknown snap_type: {job.snap_type!r}")

        return ParseResult(job.snap_type, nat_rows, st_rows, reg_rows, None)

    except Exception as e:
        return ParseResult(job.snap_type, [], [], [], str(e))


# —— Deduplication ————————————————————————————————————————————————————————————

def dedup_national(rows: list[tuple]) -> list[tuple]:
    seen: dict[tuple, tuple] = {}
    for row in rows:
        seen[(row[0], row[1])] = row
    return list(seen.values())


def dedup_states(rows: list[tuple]) -> list[tuple]:
    seen: dict[tuple, tuple] = {}
    for row in rows:
        seen[(row[0], row[1], row[3] or "")] = row
    return list(seen.values())


def dedup_regions(rows: list[tuple]) -> list[tuple]:
    """Keep last-seen row per (snap_date, area_code)."""
    seen: dict[tuple, tuple] = {}
    for row in rows:
        seen[(row[0], row[1])] = row
    return list(seen.values())


# —— Main ————————————————————————————————————————————————————————————————————

def run(repo: Path, db_path: str, workers: int, batch_size: int,
        dry_run: bool) -> None:
    t0 = time.perf_counter()

    log.info("Gas Prices ETL starting")
    log.info("repo    = %s", repo)
    log.info("db      = %s", db_path)
    log.info("workers = %d", workers)
    if dry_run:
        log.info("DRY RUN — no data will be written")

    if not repo.is_dir():
        log.error("Repo path does not exist or is not a directory: %s", repo)
        raise SystemExit(1)

    con = sqlite3.connect(db_path, isolation_level=None)
    try:
        fresh = db_is_fresh(con)
        if fresh:
            log.info("Fresh DB — applying fast pragmas")
            con.executescript(FRESH_DB_PRAGMAS)
        else:
            log.info("Existing DB — applying safe pragmas")
            con.executescript(INCREMENTAL_PRAGMAS)

        con.executescript(SCHEMA)

        log.info("Ensuring indexes exist")
        con.executescript(INDEXES_DDL)

        done_paths = already_loaded(con)
        log.info("%d files already in DB", len(done_paths))

        all_jobs: list[Job] = []
        for snap_type in SNAP_TYPES:
            d = repo / "prices" / snap_type
            if not d.exists():
                log.warning("Directory not found, skipping: %s", d)
                continue
            files = sorted(d.glob("*.csv"))
            log.info("Found %d CSV files in %s", len(files), d)
            for p in files:
                date = parse_date(p.name)
                if not date:
                    log.debug("Could not parse date from filename, skipping: %s", p.name)
                    continue
                rel = str(p.relative_to(repo))
                if rel in done_paths:
                    log.debug("Already loaded, skipping: %s", rel)
                    continue
                all_jobs.append(Job(str(p), snap_type, date, rel))

        if not all_jobs:
            log.info("Nothing new to load — DB is up to date")
            con.executescript(SAFE_PRAGMAS)
            return

        log.info("%d new file(s) to load", len(all_jobs))

        if dry_run:
            for job in all_jobs:
                log.info("  would load: %s", job.rel_path)
            con.executescript(SAFE_PRAGMAS)
            return

        log.info("Parsing %d files across %d workers", len(all_jobs), workers)

        results: dict[int, ParseResult] = {}
        errors = 0
        done   = 0

        with ProcessPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(parse_file, job): idx
                for idx, job in enumerate(all_jobs)
            }
            for fut in as_completed(futures):
                idx = futures[fut]
                try:
                    result = fut.result()
                except Exception as e:
                    errors += 1
                    done += 1
                    log.error("Worker process crashed (%d/%d): %s", done, len(all_jobs), e)
                    results[idx] = ParseResult(all_jobs[idx].snap_type, [], [], [], str(e))
                    continue

                if result.error:
                    errors += 1
                    log.error("Parse error in %s: %s", Path(all_jobs[idx].path_str).name, result.error)

                results[idx] = result
                done += 1
                if done % max(1, len(all_jobs) // 20) == 0 or done == len(all_jobs):
                    log.debug("Parsed %d/%d files", done, len(all_jobs))

        nat_buf:  list[tuple] = []
        st_buf:   list[tuple] = []
        reg_buf:  list[tuple] = []
        tracking: list[TrackingRow] = []
        now = datetime.now(timezone.utc).isoformat()

        for idx in range(len(all_jobs)):
            if idx not in results:
                continue
            job    = all_jobs[idx]
            result = results[idx]
            if result.error:
                continue
            nat_buf.extend(result.nat_rows)
            st_buf.extend(result.st_rows)
            reg_buf.extend(result.reg_rows)
            tracking.append(TrackingRow(
                file_path=job.rel_path,
                snap_date=job.snap_date,
                snap_type=job.snap_type,
                row_count=len(result.nat_rows) + len(result.st_rows) + len(result.reg_rows),
                loaded_at=now,
            ))

        del results

        log.info("Raw rows: %d national, %d county, %d regional", len(nat_buf), len(st_buf), len(reg_buf))

        nat_buf = dedup_national(nat_buf)
        st_buf  = dedup_states(st_buf)
        reg_buf = dedup_regions(reg_buf)
        log.info("After dedup: %d national, %d county, %d regional", len(nat_buf), len(st_buf), len(reg_buf))

        if errors:
            log.warning("%d file(s) had errors and will be retried next run", errors)

        log.info("Writing to SQLite")
        con.execute("BEGIN")

        for i in range(0, len(nat_buf), batch_size):
            con.executemany(
                "INSERT OR IGNORE INTO national_prices"
                " (snap_date, state_abbr, state_name, price) VALUES (?,?,?,?)",
                nat_buf[i:i + batch_size],
            )

        for i in range(0, len(st_buf), batch_size):
            con.executemany(
                "INSERT OR IGNORE INTO state_prices"
                " (snap_date, state_abbr, state_name, county, price) VALUES (?,?,?,?,?)",
                st_buf[i:i + batch_size],
            )

        for i in range(0, len(reg_buf), batch_size):
            con.executemany(
                "INSERT OR IGNORE INTO regional_prices"
                " (snap_date, area_code, area_name, area_type, price) VALUES (?,?,?,?,?)",
                reg_buf[i:i + batch_size],
            )

        con.executemany(
            "INSERT OR IGNORE INTO loaded_files"
            " (file_path, snap_date, snap_type, row_count, loaded_at) VALUES (?,?,?,?,?)",
            tracking,
        )

        con.execute("COMMIT")

        log.info("Running ANALYZE")
        con.execute("ANALYZE")

        con.executescript(SAFE_PRAGMAS)

        elapsed = time.perf_counter() - t0
        total   = len(nat_buf) + len(st_buf) + len(reg_buf)
        size_mb = Path(db_path).stat().st_size / 1_048_576
        rows_per_sec = total / elapsed if elapsed > 0 else float("inf")
        log.info(
            "Done — %d rows in %.1fs (%.0f rows/sec) | db: %s (%.1f MB)",
            total, elapsed, rows_per_sec, db_path, size_mb,
        )

    finally:
        con.close()


# —— CLI ——————————————————————————————————————————————————————————————————————

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Load nathanqthai/gas_prices CSVs into SQLite incrementally."
    )
    ap.add_argument("--repo",       default=".",             help="Path to cloned repo (default: current dir)")
    ap.add_argument("--db",         default="gas_prices.db", help="Output SQLite file")
    ap.add_argument("--workers",    type=int, default=os.cpu_count() or 4,
                    help="Parallel parse workers (default: CPU count)")
    ap.add_argument("--batch-size", type=int, default=50_000,
                    help="executemany batch size (default: 50000)")
    ap.add_argument("--dry-run",    action="store_true",
                    help="Print what would be loaded without writing anything")
    ap.add_argument("--log-level",  default="INFO",
                    choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                    help="Logging verbosity (default: INFO)")
    args = ap.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
    )

    if args.batch_size < 1:
        log.error("--batch-size must be >= 1, got %d", args.batch_size)
        raise SystemExit(1)

    if args.workers < 1:
        log.error("--workers must be >= 1, got %d", args.workers)
        raise SystemExit(1)

    run(
        repo=Path(args.repo).expanduser().resolve(),
        db_path=args.db,
        workers=args.workers,
        batch_size=args.batch_size,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
