"use strict";

// ── Helpers ─────────────────────────────────────────────────────────────────

const $ = (s) => document.querySelector(s);

function esc(s) {
  const d = document.createElement("div");
  d.textContent = s;
  return d.innerHTML;
}

function setStatus(msg) {
  $("#status").textContent = msg;
}

async function fetchJSON(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json();
}

// ── Color maps ──────────────────────────────────────────────────────────────

const STATE_COLOR_MAP = {};

function buildStateColorMap(states) {
  const hueRanges = [[25, 95], [155, 275]];
  const totalSpan = hueRanges.reduce((s, [a, b]) => s + (b - a), 0);
  states.forEach((s, i) => {
    const t = states.length > 1 ? i / (states.length - 1) : 0;
    let target = t * totalSpan;
    let hue = 0;
    for (const [lo, hi] of hueRanges) {
      const span = hi - lo;
      if (target <= span) { hue = lo + target; break; }
      target -= span;
    }
    STATE_COLOR_MAP[s.abbr] = `hsl(${Math.round(hue)}, 80%, 65%)`;
  });
}

function stateColor(abbr) {
  const c = STATE_COLOR_MAP[abbr];
  if (c && /^hsl\(\d+, \d+%, \d+%\)$/.test(c)) return c;
  return "#999";
}

// EIA colors — no red or green, distinct per area type
const EIA_COLORS = {
  national: ["#ffffff"],
  region:   ["#ff9f1c", "#fbbf24", "#e6b422", "#f59e0b", "#d97706",
             "#fdba74", "#fcd34d", "#fde68a", "#b45309", "#92400e"],
  state:    ["#38bdf8", "#818cf8", "#c084fc", "#f472b6", "#60a5fa",
             "#22d3ee", "#a78bfa", "#e879f9", "#67e8f9"],
  metro:    ["#c084fc", "#a855f7", "#7c3aed", "#d946ef", "#9333ea",
             "#d8b4fe", "#e9d5ff", "#f0abfc", "#a78bfa", "#6d28d9"],
};

function eiaColor(type, idx) {
  const palette = EIA_COLORS[type] || EIA_COLORS.region;
  return palette[idx % palette.length];
}

// ── Plotly config ───────────────────────────────────────────────────────────

const PLOTLY_LAYOUT = {
  paper_bgcolor: "rgba(0,0,0,0)",
  plot_bgcolor: "rgba(0,0,0,0)",
  font: { family: "JetBrains Mono, monospace", color: "#aaa", size: 10 },
  xaxis: {
    gridcolor: "rgba(255,255,255,0.04)",
    linecolor: "rgba(255,255,255,0.06)",
    tickcolor: "rgba(255,255,255,0.06)",
    zerolinecolor: "rgba(255,255,255,0.06)",
  },
  yaxis: {
    title: { text: "Price", font: { size: 10, color: "#aaa" } },
    tickprefix: "$",
    gridcolor: "rgba(255,255,255,0.04)",
    linecolor: "rgba(255,255,255,0.06)",
    tickcolor: "rgba(255,255,255,0.06)",
    zerolinecolor: "rgba(255,255,255,0.06)",
  },
  margin: { t: 8, b: 36, l: 52, r: 12 },
  legend: {
    orientation: "h", y: -0.12,
    font: { size: 9, color: "#aaa" },
    bgcolor: "rgba(0,0,0,0)",
  },
  hovermode: "x unified",
  hoverlabel: {
    bgcolor: "#13131a",
    bordercolor: "rgba(255,255,255,0.12)",
    font: { family: "JetBrains Mono, monospace", size: 11, color: "#e0e0e0" },
  },
  dragmode: "select",
  selectdirection: "h",
};

const PLOTLY_CONFIG = { responsive: true, displayModeBar: false };
const MISSING_DATA_FILL = "rgba(255,0,40,0.18)";
const MISSING_DATA_BORDER = "rgba(255,0,40,0.4)";

// ── App state ───────────────────────────────────────────────────────────────

let dateRange = { min: "", max: "" };
let showAvg = true;
let showGaps = false;
let events = [];
let threatSmoothing = "day"; // "day" | "week" | "month"
let loadTimer = null; // debounce timer

let lastGrouped = null;
let lastAvgData = null;
let lastEIAData = null;
let lastAllDates = [];
let lastAbbrs = [];

// ── Date utilities ──────────────────────────────────────────────────────────

function buildDateSequence(first, last) {
  const dates = [];
  const d = new Date(first + "T00:00:00");
  const end = new Date(last + "T00:00:00");
  while (d <= end) {
    dates.push(d.toISOString().slice(0, 10));
    d.setDate(d.getDate() + 1);
  }
  return dates;
}

// ── Debounced loader ────────────────────────────────────────────────────────

function scheduleLoad() {
  clearTimeout(loadTimer);
  loadTimer = setTimeout(loadData, 150);
}

// ── Data loading ────────────────────────────────────────────────────────────

async function init() {
  try {
    const [states, range, regions] = await Promise.all([
      fetchJSON("/api/states"),
      fetchJSON("/api/date-range"),
      fetchJSON("/api/regions"),
    ]);

    dateRange = range;
    buildStateColorMap(states);

    const sel = $("#state-select");
    states.forEach((s) =>
      sel.add(new Option(`${s.abbr} \u2014 ${s.name}`, s.abbr))
    );

    const eiaSel = $("#eia-select");
    regions.forEach((r) => {
      const opt = new Option(`${r.code} \u2014 ${r.name} (${r.type})`, r.code);
      opt.dataset.type = r.type;
      eiaSel.add(opt);
    });
    // Default: EIA US national average
    // Default: no EIA regions selected
    updateEIATags();

    const today = new Date().toISOString().slice(0, 10);
    $("#start-date").value = range.min;
    $("#end-date").value = today > range.max ? range.max : today;
    $("#date-range-label").textContent = `${range.min} \u2014 ${range.max}`;

    // Default: no states selected (only calculated national average shows)
    updateTags();

    await loadData();
  } catch (err) {
    setStatus(`Init error: ${err.message}`);
  }
}

async function loadData() {
  const states = Array.from($("#state-select").selectedOptions).map((o) => o.value);
  const start = $("#start-date").value;
  const end = $("#end-date").value;
  setStatus("Loading\u2026");

  try {
    const dateParams = new URLSearchParams({ start, end });

    const fetches = [fetchJSON(`/api/national-avg?${dateParams}`)];
    if (states.length) {
      const stateParams = new URLSearchParams({ states: states.join(","), start, end });
      fetches.push(fetchJSON(`/api/national?${stateParams}`));
    } else {
      fetches.push(Promise.resolve({}));
    }

    const eiaAreas = Array.from($("#eia-select").selectedOptions).map((o) => o.value);
    if (eiaAreas.length) {
      const eiaParams = new URLSearchParams({ areas: eiaAreas.join(","), start, end });
      fetches.push(fetchJSON(`/api/regional?${eiaParams}`));
    } else {
      fetches.push(Promise.resolve({}));
    }

    const [avgData, grouped, eiaData] = await Promise.all(fetches);

    const abbrs = Object.keys(grouped).sort();
    const totalRows = abbrs.reduce((n, k) => n + grouped[k].dates.length, 0);
    const eiaCount = Object.keys(eiaData).length;
    const parts = [];
    if (abbrs.length) parts.push(`${totalRows} rows / ${abbrs.length} states`);
    if (eiaCount) parts.push(`${eiaCount} EIA regions`);
    setStatus(parts.length ? parts.join(" | ") : "No data selected");

    const allDatesSet = new Set();
    if (abbrs.length) abbrs.forEach((k) => grouped[k].dates.forEach((d) => allDatesSet.add(d)));
    avgData.dates.forEach((d) => allDatesSet.add(d));
    Object.values(eiaData).forEach((r) => r.dates.forEach((d) => allDatesSet.add(d)));

    const sorted = [...allDatesSet].sort();
    const allDates = sorted.length ? buildDateSequence(sorted[0], sorted[sorted.length - 1]) : [];

    lastGrouped = grouped;
    lastAvgData = avgData;
    lastEIAData = eiaData;
    lastAllDates = allDates;
    lastAbbrs = abbrs;

    renderChart();
    renderStats(grouped, abbrs);
    updateEventsUI();
  } catch (err) {
    setStatus(`Error: ${err.message}`);
  }
}

// ── Rendering ───────────────────────────────────────────────────────────────

function buildLookups(grouped, abbrs) {
  const lookups = {};
  abbrs.forEach((abbr) => {
    const s = grouped[abbr];
    const m = new Map();
    for (let j = 0; j < s.dates.length; j++) m.set(s.dates[j], s.prices[j]);
    lookups[abbr] = m;
  });
  return lookups;
}

function buildMissingShapes(allDates, missingSet) {
  const shapes = [];
  let gapStart = null;
  for (let i = 0; i < allDates.length; i++) {
    if (missingSet.has(allDates[i])) {
      if (!gapStart) gapStart = allDates[i];
    } else if (gapStart) {
      shapes.push({
        type: "rect", xref: "x", yref: "paper",
        x0: gapStart, x1: allDates[i - 1], y0: 0, y1: 1,
        fillcolor: MISSING_DATA_FILL,
        line: { color: MISSING_DATA_BORDER, width: 1 },
        layer: "below",
      });
      gapStart = null;
    }
  }
  if (gapStart) {
    shapes.push({
      type: "rect", xref: "x", yref: "paper",
      x0: gapStart, x1: allDates[allDates.length - 1], y0: 0, y1: 1,
      fillcolor: MISSING_DATA_FILL,
      line: { color: MISSING_DATA_BORDER, width: 1 },
      layer: "below",
    });
  }
  return shapes;
}

function renderChart() {
  if (!lastGrouped) return;

  const grouped = lastGrouped;
  const allDates = lastAllDates;
  const abbrs = lastAbbrs;
  const lookups = buildLookups(grouped, abbrs);

  // State traces
  const traces = abbrs.map((abbr) => ({
    x: allDates,
    y: allDates.map((d) => lookups[abbr].get(d) ?? null),
    name: abbr,
    type: "scatter",
    mode: "lines",
    connectgaps: false,
    line: { width: 1.5, color: stateColor(abbr) },
  }));

  // National average
  if (showAvg && lastAvgData) {
    const avgLookup = new Map();
    for (let i = 0; i < lastAvgData.dates.length; i++)
      avgLookup.set(lastAvgData.dates[i], lastAvgData.prices[i]);
    traces.push({
      x: allDates,
      y: allDates.map((d) => avgLookup.get(d) ?? null),
      name: "Natl. Avg (calc)",
      type: "scatter",
      mode: "lines",
      connectgaps: false,
      line: { width: 2, color: "#fff", dash: "dot" },
    });
  }

  // EIA regional — use actual weekly dates, markers to show weekly cadence,
  // separate color per type, no legendgroup so they flow normally.
  if (lastEIAData && Object.keys(lastEIAData).length) {
    const typeCounters = {};
    Object.entries(lastEIAData).forEach(([code, r]) => {
      const t = r.type || "region";
      if (!(t in typeCounters)) typeCounters[t] = 0;
      const color = eiaColor(t, typeCounters[t]++);
      traces.push({
        x: r.dates,
        y: r.prices,
        name: `${code} (EIA)`,
        type: "scatter",
        mode: "lines+markers",
        connectgaps: true,
        marker: { size: 3, color },
        line: { width: 1.5, color },
      });
    });
  }

  // Missing-data overlay
  let shapes = [];
  if (showGaps && abbrs.length) {
    // Flag dates where ALL selected states are missing (true outage),
    // not dates where just one state happens to lack data.
    const missingSet = new Set(
      allDates.filter((d) => abbrs.every((a) => !lookups[a]?.has(d)))
    );
    shapes = buildMissingShapes(allDates, missingSet);
  }

  // Event markers + threat score trace
  let hasThreatScore = false;
  if (events.length && allDates.length) {
    const first = allDates[0];
    const last = allDates[allDates.length - 1];
    const rawThreats = []; // { date, score }

    events.forEach((evt) => {
      if (evt.date < first || evt.date > last) return;
      if (evt.threatScore != null) {
        rawThreats.push({ date: evt.date, score: evt.threatScore });
      }
    });

    if (rawThreats.length) {
      hasThreatScore = true;

      // Group by bucket based on smoothing mode
      function bucketKey(date) {
        if (threatSmoothing === "month") return date.slice(0, 7) + "-15"; // mid-month
        if (threatSmoothing === "week") {
          const d = new Date(date + "T00:00:00");
          const day = d.getDay();
          d.setDate(d.getDate() - day); // start of week (Sunday)
          return d.toISOString().slice(0, 10);
        }
        return date; // day = no smoothing
      }

      const buckets = new Map();
      rawThreats.forEach((t) => {
        const key = bucketKey(t.date);
        if (!buckets.has(key)) buckets.set(key, []);
        buckets.get(key).push(t.score);
      });

      const smoothedDates = [];
      const smoothedValues = [];
      [...buckets.entries()].sort((a, b) => a[0].localeCompare(b[0])).forEach(([date, scores]) => {
        smoothedDates.push(date);
        smoothedValues.push(+(scores.reduce((s, v) => s + v, 0) / scores.length).toFixed(1));
      });

      const useSpline = threatSmoothing !== "day";
      traces.push({
        x: smoothedDates,
        y: smoothedValues,
        name: `Threat (${threatSmoothing})`,
        type: "scatter",
        mode: useSpline ? "lines" : "lines+markers",
        connectgaps: true,
        yaxis: "y2",
        marker: { size: 3, color: "rgba(180,60,60,0.5)" },
        line: {
          width: useSpline ? 2 : 1.5,
          color: "rgba(180,60,60,0.4)",
          shape: useSpline ? "spline" : "linear",
          smoothing: 1.3,
        },
      });
    }
  }

  const layout = { ...PLOTLY_LAYOUT, shapes };
  if (hasThreatScore) {
    layout.yaxis2 = {
      title: { text: "Threat Score", font: { size: 10, color: "rgba(180,60,60,0.6)" } },
      overlaying: "y",
      side: "right",
      range: [0, 100],
      showgrid: false,
      tickcolor: "rgba(255,34,34,0.3)",
      tickfont: { color: "rgba(180,60,60,0.6)", size: 9 },
      linecolor: "rgba(255,34,34,0.3)",
      zerolinecolor: "rgba(255,34,34,0.1)",
    };
  }

  Plotly.newPlot("chart", traces, layout, PLOTLY_CONFIG);
  bindChartEvents();
}

function bindChartEvents() {
  const chartEl = document.getElementById("chart");
  chartEl.removeAllListeners("plotly_selected");
  chartEl.removeAllListeners("plotly_deselect");

  chartEl.on("plotly_selected", (eventData) => {
    if (!eventData || !eventData.range) return;
    $("#start-date").value = eventData.range.x[0].slice(0, 10);
    $("#end-date").value = eventData.range.x[1].slice(0, 10);
    Plotly.update("chart", { selectedpoints: [null] });
    loadData();
  });

  chartEl.on("plotly_deselect", () => {
    const today = new Date().toISOString().slice(0, 10);
    $("#start-date").value = dateRange.min;
    $("#end-date").value = today > dateRange.max ? dateRange.max : today;
    loadData();
  });
}

function statCard(label, color, prices) {
  if (!prices.length) return "";
  const first = prices[0];
  const last = prices[prices.length - 1];
  const pct = ((last - first) / first * 100).toFixed(1);
  const up = last >= first;
  const sign = up ? "+" : "";
  const pctClass = up ? "stat-pct-up" : "stat-pct-down";
  return `<div class="stat-card">
    <div class="stat-label">${esc(label)}</div>
    <div class="stat-row">
      <span class="stat-now" style="color:${color}">$${last.toFixed(3)}</span>
      <span class="stat-pct ${pctClass}">${sign}${pct}%</span>
    </div>
    <div class="stat-from">from $${first.toFixed(3)}</div>
  </div>`;
}

function renderStats(grouped, abbrs) {
  // States section
  const statesSection = $("#states-stats-section");
  const statesGrid = $("#states-stats-grid");
  let statesHtml = "";

  if (showAvg && lastAvgData && lastAvgData.prices.length) {
    statesHtml += statCard("Natl. Avg (calc)", "#fff", lastAvgData.prices);
  }
  statesHtml += abbrs
    .map((abbr) => statCard(`${abbr} \u2014 ${grouped[abbr].name}`, stateColor(abbr), grouped[abbr].prices))
    .join("");

  if (statesHtml) {
    statesGrid.innerHTML = statesHtml;
    statesSection.hidden = false;
  } else {
    statesSection.hidden = true;
  }

  // EIA section
  const eiaSection = $("#eia-stats-section");
  const eiaGrid = $("#eia-stats-grid");
  let eiaHtml = "";

  if (lastEIAData && Object.keys(lastEIAData).length) {
    const typeCounters = {};
    Object.entries(lastEIAData).forEach(([code, r]) => {
      const t = r.type || "region";
      if (!(t in typeCounters)) typeCounters[t] = 0;
      const color = eiaColor(t, typeCounters[t]++);
      eiaHtml += statCard(`${code} \u2014 ${r.name} (EIA)`, color, r.prices);
    });
  }

  if (eiaHtml) {
    eiaGrid.innerHTML = eiaHtml;
    eiaSection.hidden = false;
  } else {
    eiaSection.hidden = true;
  }
}


// ── Sidebar: tag helpers ────────────────────────────────────────────────────

function buildTagUI(selectId, countId, tagsId, onChange) {
  const sel = $(selectId);
  const selected = Array.from(sel.selectedOptions);
  $(countId).textContent = selected.length ? `${selected.length} selected` : "None selected";
  $(tagsId).innerHTML = selected
    .map((o) =>
      `<span class="tag">${esc(o.value)}<span class="tag-x" data-val="${esc(o.value)}">\u00d7</span></span>`
    )
    .join("");

  $(tagsId).querySelectorAll(".tag-x").forEach((x) => {
    x.addEventListener("click", () => {
      const opt = sel.querySelector(`option[value="${x.dataset.val}"]`);
      if (opt) opt.selected = false;
      onChange();
    });
  });
}

function updateTags() {
  buildTagUI("#state-select", "#selected-count", "#selected-tags", () => {
    updateTags();
    scheduleLoad();
  });
}

function updateEIATags() {
  buildTagUI("#eia-select", "#eia-selected-count", "#eia-selected-tags", () => {
    updateEIATags();
    scheduleLoad();
  });
}

// ── Event wiring ────────────────────────────────────────────────────────────

// Section folding
document.querySelectorAll(".section-header[data-toggle]").forEach((header) => {
  header.addEventListener("click", () => {
    header.closest(".sidebar-section").classList.toggle("collapsed");
  });
});

// Sidebar collapse — trigger Plotly resize after transition
$("#sidebar-toggle").addEventListener("click", () => {
  document.querySelector(".sidebar").classList.toggle("collapsed");
  setTimeout(() => {
    const chart = document.getElementById("chart");
    if (chart && chart.data) Plotly.Plots.resize(chart);
  }, 250);
});

// State/EIA selects auto-reload
$("#state-select").addEventListener("change", () => { updateTags(); scheduleLoad(); });
$("#eia-select").addEventListener("change", () => { updateEIATags(); scheduleLoad(); });

// State select all / none
$("#states-select-all").addEventListener("click", () => {
  for (const opt of $("#state-select").options) opt.selected = true;
  updateTags();
  scheduleLoad();
});
$("#states-select-none").addEventListener("click", () => {
  for (const opt of $("#state-select").options) opt.selected = false;
  updateTags();
  scheduleLoad();
});

// EIA type toggles — toggle all options of a given type
function toggleEIAType(type) {
  const sel = $("#eia-select");
  const opts = Array.from(sel.options).filter((o) => o.dataset.type === type);
  const allSelected = opts.every((o) => o.selected);
  opts.forEach((o) => (o.selected = !allSelected));
  updateEIATags();
  scheduleLoad();
}
$("#eia-toggle-national").addEventListener("click", () => toggleEIAType("national"));
$("#eia-toggle-region").addEventListener("click", () => toggleEIAType("region"));
$("#eia-toggle-state").addEventListener("click", () => toggleEIAType("state"));
$("#eia-toggle-metro").addEventListener("click", () => toggleEIAType("metro"));

// Date inputs — clamp and auto-reload
$("#start-date").addEventListener("change", () => {
  const start = $("#start-date").value;
  const end = $("#end-date").value;
  if (end && start > end) $("#end-date").value = start;
  scheduleLoad();
});
$("#end-date").addEventListener("change", () => {
  const start = $("#start-date").value;
  const end = $("#end-date").value;
  if (start && end < start) $("#start-date").value = end;
  scheduleLoad();
});

// Overlay toggles
$("#avg-btn").addEventListener("click", () => {
  showAvg = !showAvg;
  $("#avg-btn").classList.toggle("active", showAvg);
  renderChart();
});

$("#gaps-btn").addEventListener("click", () => {
  showGaps = !showGaps;
  $("#gaps-btn").classList.toggle("active", showGaps);
  renderChart();
});

$("#reset-btn").addEventListener("click", () => {
  const today = new Date().toISOString().slice(0, 10);
  $("#start-date").value = dateRange.min;
  $("#end-date").value = today > dateRange.max ? dateRange.max : today;
  loadData();
});


// Stats section collapse
$("#states-stats-toggle").addEventListener("click", () => {
  $("#states-stats-section").classList.toggle("collapsed");
});
$("#eia-stats-toggle").addEventListener("click", () => {
  $("#eia-stats-section").classList.toggle("collapsed");
});

// ── Events CSV upload ───────────────────────────────────────────────────────

const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;

function isValidURL(s) {
  try { const u = new URL(s); return u.protocol === "http:" || u.protocol === "https:"; }
  catch { return false; }
}

function parseCSVRows(text) {
  const rows = [];
  let row = [];
  let field = "";
  let inQuotes = false;
  const len = text.length;

  for (let i = 0; i < len; i++) {
    const ch = text[i];
    if (inQuotes) {
      if (ch === '"') {
        if (i + 1 < len && text[i + 1] === '"') { field += '"'; i++; }
        else inQuotes = false;
      } else field += ch;
    } else if (ch === '"') inQuotes = true;
    else if (ch === ",") { row.push(field); field = ""; }
    else if (ch === "\n" || (ch === "\r" && text[i + 1] === "\n")) {
      if (ch === "\r") i++;
      row.push(field); field = "";
      if (row.length > 1 || row[0] !== "") rows.push(row);
      row = [];
    } else field += ch;
  }
  row.push(field);
  if (row.length > 1 || row[0] !== "") rows.push(row);
  return rows;
}

function parseEventsCSV(text) {
  const rows = parseCSVRows(text.trim());
  if (rows.length < 2) return [];

  const header = rows[0].map((h) => h.trim().toLowerCase());
  const dateIdx = header.indexOf("date");
  const titleIdx = header.indexOf("title");
  const descIdx = header.indexOf("description");
  const sourceIdx = header.indexOf("source");
  const threatIdx = header.indexOf("threat_score");

  if (dateIdx === -1 || titleIdx === -1) throw new Error('CSV must have "date" and "title" columns');

  const parsed = [];
  for (let i = 1; i < rows.length; i++) {
    const cols = rows[i];
    const date = (cols[dateIdx] || "").trim();
    const title = (cols[titleIdx] || "").trim();
    const description = descIdx !== -1 ? (cols[descIdx] || "").trim() : "";
    const source = sourceIdx !== -1 ? (cols[sourceIdx] || "").trim() : "";
    let threatScore = null;
    if (threatIdx !== -1) {
      const raw = parseFloat((cols[threatIdx] || "").trim());
      if (!isNaN(raw) && raw >= 0 && raw <= 100) threatScore = raw;
    }
    if (!DATE_RE.test(date) || !title) continue;
    parsed.push({ date, title, description, source: isValidURL(source) ? source : "", threatScore });
  }
  return parsed;
}

let currentEventPage = "";

function getEventMonths() {
  return [...new Set(events.map((e) => e.date.slice(0, 7)))].sort();
}

function updateEventsUI() {
  const info = $("#events-info");
  const clearBtn = $("#clear-events-btn");
  const panel = $("#events-panel");
  const list = $("#events-list");

  if (!events.length) {
    info.textContent = "";
    clearBtn.hidden = true;
    panel.hidden = true;
    list.innerHTML = "";
    return;
  }

  info.textContent = `${events.length} event${events.length > 1 ? "s" : ""} loaded`;
  clearBtn.hidden = false;
  panel.hidden = false;

  const months = getEventMonths();
  if (!currentEventPage || !months.includes(currentEventPage)) currentEventPage = months[0];

  const sorted = events.slice().sort((a, b) => a.date.localeCompare(b.date));
  const pageEvents = sorted.filter((e) => e.date.slice(0, 7) === currentEventPage);

  const monthIdx = months.indexOf(currentEventPage);
  const prevMonth = monthIdx > 0 ? months[monthIdx - 1] : null;
  const nextMonth = monthIdx < months.length - 1 ? months[monthIdx + 1] : null;

  const formatMonth = (ym) => {
    const [y, m] = ym.split("-");
    const names = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
    return `${names[parseInt(m, 10) - 1]} ${y}`;
  };

  const paginationHTML = `<div class="events-pagination">
    <button class="events-page-btn" id="events-prev" ${prevMonth ? "" : "disabled"}>&#9664; ${prevMonth ? formatMonth(prevMonth) : ""}</button>
    <span class="events-page-label">${formatMonth(currentEventPage)} (${pageEvents.length})</span>
    <button class="events-page-btn" id="events-next" ${nextMonth ? "" : "disabled"}>${nextMonth ? formatMonth(nextMonth) : ""} &#9654;</button>
  </div>`;

  // Build sorted avg lookup for day-over-day and first-event change
  const avgLookup = new Map();
  const avgDates = [];
  if (lastAvgData) {
    for (let i = 0; i < lastAvgData.dates.length; i++) {
      avgLookup.set(lastAvgData.dates[i], lastAvgData.prices[i]);
      avgDates.push(lastAvgData.dates[i]);
    }
  }
  avgDates.sort();

  // Find the price for the previous available date
  function prevPrice(date) {
    let prev = null;
    for (const d of avgDates) {
      if (d >= date) break;
      prev = avgLookup.get(d);
    }
    return prev;
  }

  // First event price for "change from first event"
  const firstEventDate = pageEvents.length ? pageEvents[0].date : null;
  const firstEventPrice = firstEventDate ? avgLookup.get(firstEventDate) : null;

  function pctBadge(from, to, title) {
    if (from == null || to == null || from === 0) return "";
    const pct = ((to - from) / from * 100).toFixed(1);
    const up = to >= from;
    const cls = up ? "stat-pct-up" : "stat-pct-down";
    return `<span class="event-pct ${cls}" title="${title}">${up ? "+" : ""}${pct}%</span>`;
  }

  // Threat score thresholds — thirds of max score in current page
  const maxThreat = Math.max(0, ...pageEvents.map((e) => e.threatScore ?? 0));
  const lowThreshold = maxThreat / 3;
  const highThreshold = (maxThreat * 2) / 3;

  function threatBadge(score) {
    if (score == null) return "";
    let cls = "threat-low";
    if (score > highThreshold) cls = "threat-high";
    else if (score > lowThreshold) cls = "threat-med";
    return `<span class="event-threat ${cls}" title="Threat score: ${score}/100">${score}</span>`;
  }

  const itemsHTML = pageEvents
    .map((evt, i) => {
      const price = avgLookup.get(evt.date);
      const priceStr = price != null ? `$${price.toFixed(3)}` : "\u2014";
      const prev = prevPrice(evt.date);
      const dayBadge = pctBadge(prev, price, "1-day change");
      const fromFirstBadge = pctBadge(firstEventPrice, price, "Change from first event");
      const threat = threatBadge(evt.threatScore);
      return `<div class="event-item" data-event-idx="${i}">
        <div class="event-header">
          <span class="event-left">
            <span class="event-date">${esc(evt.date)}</span>
            <span class="event-price-group">
              <span class="event-price">${priceStr}</span>
              ${dayBadge}${fromFirstBadge}
            </span>
          </span>
          <span class="event-title">${esc(evt.title)}${evt.source ? ` <a class="event-source" href="${esc(evt.source)}" target="_blank" rel="noopener noreferrer">&#8599;</a>` : ""}</span>
          ${threat}
          <span class="event-chevron">&#9654;</span>
        </div>
        <div class="event-body"><div class="event-desc">${esc(evt.description || "No description provided.")}</div></div>
      </div>`;
    })
    .join("");

  const hasThreatScores = pageEvents.some((e) => e.threatScore != null);
  const legendHTML = `<div class="events-legend">
    <span><b>Date</b></span>
    <span><b>Price</b> = Natl. avg</span>
    <span><b>1st %</b> = 1-day change</span>
    <span><b>2nd %</b> = change from first event</span>
    ${hasThreatScores ? `<span><span class="event-threat threat-low">Lo</span> <span class="event-threat threat-med">Med</span> <span class="event-threat threat-high">Hi</span> = Threat score</span>` : ""}
  </div>`;

  list.innerHTML = legendHTML + paginationHTML + itemsHTML;

  const prevBtn = $("#events-prev");
  const nextBtn = $("#events-next");
  if (prevBtn && prevMonth) prevBtn.addEventListener("click", () => { currentEventPage = prevMonth; updateEventsUI(); });
  if (nextBtn && nextMonth) nextBtn.addEventListener("click", () => { currentEventPage = nextMonth; updateEventsUI(); });

  list.querySelectorAll(".event-item").forEach((el) => {
    el.addEventListener("click", (e) => {
      if (e.target.closest(".event-source")) return;
      el.classList.toggle("expanded");
    });
  });
}

$("#events-file").addEventListener("change", (e) => {
  const file = e.target.files[0];
  if (!file) return;
  const reader = new FileReader();
  reader.onload = () => {
    try {
      events = parseEventsCSV(reader.result);
      $("#threat-smoothing-group").hidden = !events.some((e) => e.threatScore != null);
      updateEventsUI();
      renderChart();
      setStatus(`Loaded ${events.length} event(s) from ${esc(file.name)}`);
    } catch (err) {
      setStatus(`CSV error: ${err.message}`);
      events = [];
      updateEventsUI();
    }
  };
  reader.readAsText(file);
  e.target.value = "";
});

$("#events-panel-toggle").addEventListener("click", () => {
  $("#events-panel").classList.toggle("collapsed");
});

$("#clear-events-btn").addEventListener("click", () => {
  events = [];
  $("#threat-smoothing-group").hidden = true;
  updateEventsUI();
  renderChart();
  setStatus("Events cleared");
});

// Threat smoothing toggles
["day", "week", "month"].forEach((mode) => {
  $(`#smooth-${mode}`).addEventListener("click", () => {
    threatSmoothing = mode;
    $(`#smooth-day`).classList.toggle("active", mode === "day");
    $(`#smooth-week`).classList.toggle("active", mode === "week");
    $(`#smooth-month`).classList.toggle("active", mode === "month");
    renderChart();
  });
});

// ── Boot ────────────────────────────────────────────────────────────────────

init();
