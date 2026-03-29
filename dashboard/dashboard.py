#!/usr/bin/env python3
"""Minimal Flask dashboard for browsing gas price data from SQLite."""

import os
import re
import sqlite3
from pathlib import Path

from flask import Flask, g, jsonify, render_template, request

# Input validation patterns
_STATE_RE = re.compile(r"^[A-Z]{2}$")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

app = Flask(__name__)

MAX_STATES = 51  # all US states + DC


@app.after_request
def set_security_headers(response):
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; "
        "script-src 'self' https://cdn.plot.ly; "
        "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; "
        "font-src https://fonts.gstatic.com; "
        "img-src 'self' data:; "
        "connect-src 'self'"
    )
    return response

DB_PATH = Path(
    os.environ.get(
        "DB_PATH",
        Path(__file__).resolve().parent.parent / "gas_prices.db",
    )
)


# ── DB helpers ───────────────────────────────────────────────────────────────

def get_db() -> sqlite3.Connection:
    """Return a per-request DB connection (created once, reused, auto-closed)."""
    if "db" not in g:
        g.db = sqlite3.connect(str(DB_PATH))
        g.db.row_factory = sqlite3.Row
    return g.db


@app.teardown_appcontext
def close_db(exc: BaseException | None) -> None:
    db = g.pop("db", None)
    if db is not None:
        db.close()


def _valid_date(s: str) -> str:
    """Return the string if it's a valid YYYY-MM-DD date, else empty string."""
    return s if _DATE_RE.match(s) else ""


def _valid_states(raw: str) -> list[str]:
    """Parse and validate a comma-separated list of state abbreviations."""
    states = [s for s in (t.strip().upper() for t in raw.split(",")) if _STATE_RE.match(s)]
    return states[:MAX_STATES]


def _date_filters(q: str, params: list, start: str, end: str) -> str:
    """Append optional snap_date range filters to a query."""
    if start:
        q += " AND snap_date >= ?"
        params.append(start)
    if end:
        q += " AND snap_date <= ?"
        params.append(end)
    return q


# ── Routes ───────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/states")
def api_states():
    """List all state abbreviations and names."""
    rows = get_db().execute(
        "SELECT DISTINCT state_abbr, state_name"
        " FROM national_prices ORDER BY state_abbr"
    ).fetchall()
    return jsonify([{"abbr": r["state_abbr"], "name": r["state_name"]} for r in rows])


@app.route("/api/date-range")
def api_date_range():
    """Min and max snap_date in the dataset."""
    row = get_db().execute(
        "SELECT MIN(snap_date) AS min_date, MAX(snap_date) AS max_date"
        " FROM national_prices"
    ).fetchone()
    return jsonify({"min": row["min_date"], "max": row["max_date"]})


@app.route("/api/national")
def api_national():
    """State prices grouped by state. ?states=CA,TX&start=...&end=..."""
    states = _valid_states(request.args.get("states", ""))
    start = _valid_date(request.args.get("start", ""))
    end = _valid_date(request.args.get("end", ""))

    # Require explicit state selection — don't return everything on empty param
    if "states" in request.args and not states:
        return jsonify({})

    q = "SELECT snap_date, state_abbr, state_name, price FROM national_prices WHERE 1=1"
    params: list = []

    if states:
        q += f" AND state_abbr IN ({','.join('?' * len(states))})"
        params.extend(states)

    q = _date_filters(q, params, start, end)
    q += " ORDER BY state_abbr, snap_date"

    grouped: dict = {}
    for r in get_db().execute(q, params):
        abbr = r["state_abbr"]
        if abbr not in grouped:
            grouped[abbr] = {"name": r["state_name"], "dates": [], "prices": []}
        grouped[abbr]["dates"].append(r["snap_date"])
        grouped[abbr]["prices"].append(r["price"])

    return jsonify(grouped)


@app.route("/api/national-avg")
def api_national_avg():
    """National average price per day (across all states)."""
    start = _valid_date(request.args.get("start", ""))
    end = _valid_date(request.args.get("end", ""))

    q = "SELECT snap_date, ROUND(AVG(price), 3) AS avg_price FROM national_prices WHERE 1=1"
    params: list = []
    q = _date_filters(q, params, start, end)
    q += " GROUP BY snap_date ORDER BY snap_date"

    rows = get_db().execute(q, params).fetchall()
    return jsonify({
        "dates": [r["snap_date"] for r in rows],
        "prices": [r["avg_price"] for r in rows],
    })


@app.route("/api/regions")
def api_regions():
    """List all EIA region/area codes and names."""
    rows = get_db().execute(
        "SELECT DISTINCT area_code, area_name, area_type"
        " FROM regional_prices ORDER BY area_type, area_code"
    ).fetchall()
    return jsonify([
        {"code": r["area_code"], "name": r["area_name"], "type": r["area_type"]}
        for r in rows
    ])


@app.route("/api/regional")
def api_regional():
    """EIA regional prices grouped by area. ?areas=US,PADD1,CA&start=...&end=..."""
    areas = [a for a in (s.strip() for s in request.args.get("areas", "").split(",")) if re.match(r"^[A-Z0-9]{1,8}$", a)]
    start = _valid_date(request.args.get("start", ""))
    end = _valid_date(request.args.get("end", ""))

    # Require explicit area selection — don't return everything on empty param
    if "areas" in request.args and not areas:
        return jsonify({})

    q = "SELECT snap_date, area_code, area_name, area_type, price FROM regional_prices WHERE 1=1"
    params: list = []

    if areas:
        q += f" AND area_code IN ({','.join('?' * len(areas))})"
        params.extend(areas)

    q = _date_filters(q, params, start, end)
    q += " ORDER BY area_code, snap_date"

    grouped: dict = {}
    for r in get_db().execute(q, params):
        code = r["area_code"]
        if code not in grouped:
            grouped[code] = {
                "name": r["area_name"], "type": r["area_type"],
                "dates": [], "prices": [],
            }
        grouped[code]["dates"].append(r["snap_date"])
        grouped[code]["prices"].append(r["price"])

    return jsonify(grouped)


# ── Entrypoint ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app.run(
        debug=os.environ.get("FLASK_DEBUG", "0") == "1",
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 8080)),
    )
