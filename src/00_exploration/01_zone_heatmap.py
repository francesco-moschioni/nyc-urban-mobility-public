"""
src/00_exploration/01_zone_heatmap.py
=====================================
Generates a self-contained interactive HTML choropleth map of trip
origin/destination frequency per TLC zone, split by transport mode.

Features:
  - Multi-mode selection (toggle any combination of transport modes)
  - Date-range selector (from/to by year+quarter)
  - Top-15 OD routes panel (right sidebar)
  - Choropleth map coloured by aggregated trip count

Strategy to avoid OOM on 8-16 GB RAM:
  - DuckDB reads parquet files directly via SQL (on-disk temp dir)
  - Aggregation happens entirely in DuckDB
  - Only the resulting small summary tables are loaded into Python
  - TLC zone shapefile is read once via geopandas (lightweight)

Output: cfg.figures / "exploration" / "01_zone_heatmap.html"

parents[N] = 2  (src/00_exploration/ → src/ → Tesi/)
"""

import importlib.util
import json
import sys
import warnings
from pathlib import Path

import duckdb
import geopandas as gpd
import pandas as pd

warnings.filterwarnings("ignore")

# ── config ────────────────────────────────────────────────────────────────────
_cfg_path = Path(__file__).parents[2] / "00_config.py"
_spec     = importlib.util.spec_from_file_location("config", _cfg_path)
_mod      = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
cfg = _mod.cfg

OUT_DIR = Path(cfg.figures) / "exploration"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_HTML = OUT_DIR / "01_zone_heatmap.html"

SHAPEFILE = Path(cfg.nyc_zones) / "taxi_zones.shp"

# DuckDB temp nella stessa cartella data (stesso disco)
DUCKDB_TEMP = str(Path(cfg.data) / "duckdb_tmp")
Path(DUCKDB_TEMP).mkdir(parents=True, exist_ok=True)

# ── dataset registry ──────────────────────────────────────────────────────────
DATASETS = {
    "Yellow Taxi": {
        "path":    Path(cfg.interim) / "tlc" / "yellow_zone_hour.parquet",
        "pu":      "PULocationID",
        "do":      "DOLocationID",
        "date":    "date",
        "dataset": "yellow",
    },
    "Green Taxi": {
        "path":    Path(cfg.interim) / "tlc" / "green_zone_hour.parquet",
        "pu":      "PULocationID",
        "do":      "DOLocationID",
        "date":    "date",
        "dataset": "green",
    },
    "Uber": {
        "path":    Path(cfg.interim) / "tlc" / "uber_zone_hour.parquet",
        "pu":      "PULocationID",
        "do":      "DOLocationID",
        "date":    "date",
        "dataset": "uber",
    },
    "Lyft": {
        "path":    Path(cfg.interim) / "tlc" / "lyft_zone_hour.parquet",
        "pu":      "PULocationID",
        "do":      "DOLocationID",
        "date":    "date",
        "dataset": "lyft",
    },
    "Via": {
        "path":    Path(cfg.interim) / "tlc" / "via_zone_hour.parquet",
        "pu":      "PULocationID",
        "do":      "DOLocationID",
        "date":    "date",
        "dataset": "via",
    },
    "Citi Bike": {
        "path":           Path(cfg.interim_citibike) / "citibike_agg.parquet",
        "pu":             "start_tlc_zone",
        "do":             "end_tlc_zone",
        "date":           "date",       # date32 nell'agg file
        "dataset":        "citibike",
        "trip_count_col": "n_trips",
    },
    "MTA Subway": {
        "path":           Path(cfg.interim_spatial) / "mta_flows_tlc.parquet",
        "pu":             "origin_zone",
        "do":             "dest_zone",
        "date":           "date",
        "dataset":        "mta_subway",
        "trip_count_col": "estimated_flow",
    },
}

# ── helpers ───────────────────────────────────────────────────────────────────

def available_datasets() -> dict:
    """Return only datasets whose parquet file actually exists."""
    return {k: v for k, v in DATASETS.items() if Path(v["path"]).exists()}


def aggregate_dataset(con: duckdb.DuckDBPyConnection, label: str, meta: dict) -> pd.DataFrame:
    """
    Aggregate one parquet into:
        zone_id | direction | year | quarter | trip_count | mode
    direction = 'origin' or 'destination'
    """
    path     = str(meta["path"])
    pu_col   = meta["pu"]
    do_col   = meta["do"]
    date_col = meta["date"]
    ds_label = meta["dataset"]
    tc_col   = meta.get("trip_count_col", "trip_count")

    date_expr = f"CAST({date_col} AS DATE)" if ds_label == "citibike" else date_col

    sql = f"""
    WITH base AS (
        SELECT
            {pu_col}    AS pu_zone,
            {do_col}    AS do_zone,
            {date_expr} AS trip_date,
            {tc_col}    AS trip_count
        FROM read_parquet('{path}')
        WHERE {pu_col} IS NOT NULL
          AND {do_col} IS NOT NULL
          AND {tc_col} > 0
    ),
    with_period AS (
        SELECT
            pu_zone,
            do_zone,
            trip_count,
            YEAR(trip_date)                            AS year,
            CAST(CEIL(MONTH(trip_date) / 3.0) AS INT) AS quarter
        FROM base
    ),
    origins AS (
        SELECT pu_zone AS zone_id, 'origin' AS direction,
               year, quarter, SUM(trip_count) AS trip_count
        FROM with_period
        GROUP BY pu_zone, year, quarter
    ),
    destinations AS (
        SELECT do_zone AS zone_id, 'destination' AS direction,
               year, quarter, SUM(trip_count) AS trip_count
        FROM with_period
        GROUP BY do_zone, year, quarter
    )
    SELECT *, '{label}' AS mode FROM origins
    UNION ALL
    SELECT *, '{label}' AS mode FROM destinations
    """

    try:
        result = con.execute(sql).df()
        print(f"  ✓ {label}: {len(result):,} rows aggregated")
        return result
    except Exception as e:
        print(f"  ✗ {label}: skipped ({e})")
        return pd.DataFrame()


def aggregate_od_dataset(
    con: duckdb.DuckDBPyConnection,
    label: str,
    meta: dict,
    top_n: int = 100,
) -> pd.DataFrame:
    """
    Aggregate top-N OD pairs per (year, quarter) for one dataset.
    Returns DataFrame: origin_id | dest_id | year | quarter | count | mode
    Self-loops and invalid zones (264, 265) are excluded.
    """
    path     = str(meta["path"])
    pu_col   = meta["pu"]
    do_col   = meta["do"]
    date_col = meta["date"]
    ds_label = meta["dataset"]
    tc_col   = meta.get("trip_count_col", "trip_count")

    date_expr = f"CAST({date_col} AS DATE)" if ds_label == "citibike" else date_col

    sql = f"""
    WITH base AS (
        SELECT
            {pu_col}    AS oid,
            {do_col}    AS did,
            {date_expr} AS d,
            {tc_col}    AS cnt
        FROM read_parquet('{path}')
        WHERE {pu_col} IS NOT NULL
          AND {do_col} IS NOT NULL
          AND {pu_col} <> {do_col}
          AND {pu_col} NOT IN (264, 265)
          AND {do_col} NOT IN (264, 265)
          AND {tc_col} > 0
    ),
    wp AS (
        SELECT
            oid, did, cnt,
            YEAR(d)                            AS year,
            CAST(CEIL(MONTH(d) / 3.0) AS INT) AS quarter
        FROM base
    ),
    agg AS (
        SELECT oid AS origin_id, did AS dest_id,
               year, quarter,
               SUM(cnt) AS od_count
        FROM wp
        GROUP BY oid, did, year, quarter
    )
    SELECT origin_id, dest_id, year, quarter, od_count AS count
    FROM agg
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY year, quarter
        ORDER BY od_count DESC
    ) <= {top_n}
    ORDER BY year, quarter, od_count DESC
    """

    try:
        df = con.execute(sql).df()
        df["mode"] = label
        print(f"  ✓ {label} OD: {len(df):,} rows")
        return df
    except Exception as e:
        print(f"  ✗ {label} OD: skipped ({e})")
        return pd.DataFrame()


def load_geojson(shapefile: Path) -> dict:
    """Load TLC zone shapefile → simplified GeoJSON for Leaflet."""
    gdf = gpd.read_file(shapefile).to_crs(epsg=4326)
    gdf = gdf[["LocationID", "zone", "borough", "geometry"]].copy()
    gdf["geometry"] = gdf["geometry"].simplify(tolerance=0.0003, preserve_topology=True)
    return json.loads(gdf.to_json())


def build_lookup(df: pd.DataFrame) -> dict:
    """
    Build nested lookup:
      lookup[mode][direction][year][quarter][zone_id] = trip_count
    Also returns sorted lists of modes (years no longer returned separately).
    """
    lookup = {}
    for _, row in df.iterrows():
        mode      = row["mode"]
        direction = row["direction"]
        year      = str(int(row["year"]))
        quarter   = str(int(row["quarter"]))
        zone      = str(int(row["zone_id"]))
        count     = int(row["trip_count"])

        lookup.setdefault(mode, {})
        lookup[mode].setdefault(direction, {})
        lookup[mode][direction].setdefault(year, {})
        lookup[mode][direction][year].setdefault(quarter, {})
        lookup[mode][direction][year][quarter][zone] = count

    years = sorted({str(int(r["year"])) for _, r in df.iterrows()})
    modes = sorted(df["mode"].unique().tolist())
    return lookup, years, modes


def build_od_lookup(od_df: pd.DataFrame) -> dict:
    """
    Build nested lookup for OD routes:
      od_lookup[mode][year][quarter] = [[origin_id, dest_id, count], ...]
    Pre-sorted descending by count within each (year, quarter) group.
    """
    od: dict = {}
    for r in od_df.itertuples():
        m = r.mode
        y = str(int(r.year))
        q = str(int(r.quarter))
        od.setdefault(m, {}).setdefault(y, {}).setdefault(q, [])
        od[m][y][q].append([int(r.origin_id), int(r.dest_id), int(r.count)])
    return od


# ── HTML template ─────────────────────────────────────────────────────────────

HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>NYC Mobility — Zone Frequency Explorer</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=Instrument+Serif:ital@0;1&display=swap" rel="stylesheet">
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<style>
  :root {
    --bg:        #0d0f14;
    --panel:     #13161e;
    --border:    #242836;
    --accent:    #e8c84a;
    --text:      #e2e4ec;
    --muted:     #6b7280;
    --font-mono: 'DM Mono', monospace;
    --font-serif:'Instrument Serif', serif;
    --panel-w:   300px;
    --routes-w:  260px;
  }

  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

  body {
    background: var(--bg);
    color: var(--text);
    font-family: var(--font-mono);
    font-size: 13px;
    height: 100vh;
    display: flex;
    flex-direction: column;
    overflow: hidden;
  }

  /* ── header ── */
  header {
    flex-shrink: 0;
    padding: 14px 20px 12px;
    border-bottom: 1px solid var(--border);
    display: flex;
    align-items: baseline;
    gap: 16px;
    background: var(--panel);
    z-index: 1000;
  }
  header h1 {
    font-family: var(--font-serif);
    font-size: 22px;
    font-weight: 400;
    font-style: italic;
    color: var(--accent);
    letter-spacing: -0.3px;
  }
  header span.sub {
    font-size: 11px;
    color: var(--muted);
    letter-spacing: 0.08em;
    text-transform: uppercase;
  }

  /* ── layout ── */
  .main {
    flex: 1;
    display: flex;
    overflow: hidden;
  }

  /* ── sidebar (left) ── */
  .sidebar {
    width: var(--panel-w);
    flex-shrink: 0;
    background: var(--panel);
    border-right: 1px solid var(--border);
    display: flex;
    flex-direction: column;
    overflow-y: auto;
    z-index: 500;
  }

  .sidebar-section {
    padding: 16px;
    border-bottom: 1px solid var(--border);
  }
  .sidebar-section:last-child { border-bottom: none; }

  .section-label {
    font-size: 10px;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: var(--muted);
    margin-bottom: 10px;
  }

  /* ── controls ── */
  select {
    width: 100%;
    background: var(--bg);
    color: var(--text);
    border: 1px solid var(--border);
    border-radius: 4px;
    padding: 7px 10px;
    font-family: var(--font-mono);
    font-size: 12px;
    cursor: pointer;
    appearance: none;
    background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='10' height='6'%3E%3Cpath d='M0 0l5 6 5-6z' fill='%236b7280'/%3E%3C/svg%3E");
    background-repeat: no-repeat;
    background-position: right 10px center;
    transition: border-color .15s;
  }
  select:hover  { border-color: var(--accent); }
  select:focus  { outline: none; border-color: var(--accent); }

  .pill-group {
    display: flex;
    gap: 6px;
    width: 100%;
  }
  .pill {
    flex: 1;
    padding: 6px 0;
    border: 1px solid var(--border);
    border-radius: 4px;
    background: var(--bg);
    color: var(--muted);
    font-family: var(--font-mono);
    font-size: 11px;
    cursor: pointer;
    text-align: center;
    letter-spacing: 0.05em;
    transition: all .15s;
  }
  .pill.active {
    background: var(--accent);
    color: var(--bg);
    border-color: var(--accent);
    font-weight: 500;
  }
  .pill:hover:not(.active) { border-color: var(--accent); color: var(--text); }

  /* mode pills — vertical stack, left-aligned, no flex stretch */
  .mode-pill {
    flex: none;
    text-align: left;
    padding: 6px 10px;
  }

  /* ── color scale legend ── */
  .legend-bar {
    height: 10px;
    border-radius: 3px;
    background: linear-gradient(to right,
      #1a1f2e, #1d3a5c, #1e5fa8, #1a8fc4,
      #0fb5b5, #4cde8a, #f0e84a, #f07c20, #d42020);
    margin: 8px 0 4px;
  }
  .legend-labels {
    display: flex;
    justify-content: space-between;
    font-size: 10px;
    color: var(--muted);
  }

  /* ── stats box ── */
  .stats { display: flex; flex-direction: column; gap: 8px; }
  .stat-row { display: flex; justify-content: space-between; align-items: baseline; }
  .stat-label { color: var(--muted); font-size: 11px; }
  .stat-val   { color: var(--accent); font-size: 13px; font-weight: 500; }

  /* ── zone tooltip ── */
  .zone-tooltip {
    background: var(--panel) !important;
    border: 1px solid var(--border) !important;
    color: var(--text) !important;
    font-family: var(--font-mono) !important;
    font-size: 12px !important;
    border-radius: 4px !important;
    padding: 8px 12px !important;
    box-shadow: 0 4px 20px rgba(0,0,0,.5) !important;
  }
  .zone-tooltip strong {
    color: var(--accent);
    display: block;
    margin-bottom: 4px;
    font-family: var(--font-serif);
    font-style: italic;
    font-size: 14px;
  }

  /* ── map ── */
  #map {
    flex: 1;
    background: #0a0c10;
  }

  /* ── routes panel (right) ── */
  .routes-panel {
    width: var(--routes-w);
    flex-shrink: 0;
    background: var(--panel);
    border-left: 1px solid var(--border);
    display: flex;
    flex-direction: column;
    overflow: hidden;
    z-index: 500;
  }
  .routes-panel > .sidebar-section {
    flex-shrink: 0;
  }
  #routes-list {
    flex: 1;
    overflow-y: auto;
  }

  .route-item {
    padding: 10px 16px;
    border-bottom: 1px solid var(--border);
    transition: background .1s;
  }
  .route-item:hover { background: rgba(255,255,255,.03); }
  .route-rank  { font-size: 10px; color: var(--muted); margin-bottom: 3px; }
  .route-label {
    font-size: 11px; color: var(--text); margin-bottom: 4px;
    white-space: nowrap; overflow: hidden; text-overflow: ellipsis; line-height: 1.5;
  }
  .route-label .arr { color: var(--accent); margin: 0 3px; }
  .route-count { font-size: 11px; color: var(--accent); margin-bottom: 5px; }
  .route-bar      { height: 3px; background: var(--border); border-radius: 2px; }
  .route-bar-fill { height: 3px; background: var(--accent); border-radius: 2px; transition: width .3s; }

  /* ── loading overlay ── */
  #loading {
    position: fixed;
    inset: 0;
    background: rgba(13,15,20,.92);
    display: flex;
    align-items: center;
    justify-content: center;
    z-index: 9999;
    flex-direction: column;
    gap: 16px;
    font-family: var(--font-mono);
  }
  #loading p { color: var(--muted); font-size: 13px; letter-spacing: 0.08em; }
  .spinner {
    width: 36px; height: 36px;
    border: 2px solid var(--border);
    border-top-color: var(--accent);
    border-radius: 50%;
    animation: spin .7s linear infinite;
  }
  @keyframes spin { to { transform: rotate(360deg); } }

  /* ── no-data notice ── */
  #no-data {
    display: none;
    position: absolute;
    top: 50%;
    left: 50%;
    transform: translate(-50%, -50%);
    color: var(--muted);
    font-size: 13px;
    text-align: center;
    z-index: 400;
    pointer-events: none;
  }
</style>
</head>
<body>

<div id="loading">
  <div class="spinner"></div>
  <p>Initialising map data…</p>
</div>

<header>
  <h1>NYC Mobility Explorer</h1>
  <span class="sub">Zone frequency · choropleth</span>
</header>

<div class="main">

  <!-- ── SIDEBAR (left) ────────────────────────────────── -->
  <aside class="sidebar">

    <div class="sidebar-section">
      <div class="section-label">Transport Mode</div>
      <div id="mode-toggles" style="display:flex;flex-direction:column;gap:5px;"></div>
    </div>

    <div class="sidebar-section">
      <div class="section-label">Direction</div>
      <div class="pill-group">
        <button class="pill active" data-dir="origin">Origin</button>
        <button class="pill"        data-dir="destination">Destination</button>
      </div>
    </div>

    <div class="sidebar-section">
      <div class="section-label">Time Range</div>
      <div style="display:flex;gap:8px;">
        <div style="flex:1;">
          <div style="font-size:10px;color:var(--muted);margin-bottom:4px;">From</div>
          <select id="sel-from"></select>
        </div>
        <div style="flex:1;">
          <div style="font-size:10px;color:var(--muted);margin-bottom:4px;">To</div>
          <select id="sel-to"></select>
        </div>
      </div>
    </div>

    <div class="sidebar-section">
      <div class="section-label">Color Scale</div>
      <div class="legend-bar"></div>
      <div class="legend-labels"><span>low</span><span>high</span></div>
      <div style="margin-top:10px">
        <div class="section-label" style="margin-bottom:6px">Scale Type</div>
        <div class="pill-group">
          <button class="pill active" data-scale="log">Log</button>
          <button class="pill"        data-scale="linear">Linear</button>
        </div>
      </div>
    </div>

    <div class="sidebar-section">
      <div class="section-label">Statistics</div>
      <div class="stats">
        <div class="stat-row">
          <span class="stat-label">zones with data</span>
          <span class="stat-val" id="st-zones">—</span>
        </div>
        <div class="stat-row">
          <span class="stat-label">total trips</span>
          <span class="stat-val" id="st-total">—</span>
        </div>
        <div class="stat-row">
          <span class="stat-label">top zone</span>
          <span class="stat-val" id="st-top">—</span>
        </div>
        <div class="stat-row">
          <span class="stat-label">max trips</span>
          <span class="stat-val" id="st-max">—</span>
        </div>
      </div>
    </div>

  </aside>

  <!-- ── MAP ──────────────────────────────────────────── -->
  <div style="position:relative;flex:1;display:flex;flex-direction:column;">
    <div id="map"></div>
    <div id="no-data">No data available<br>for this selection.</div>
  </div>

  <!-- ── ROUTES PANEL (right) ─────────────────────────── -->
  <aside class="routes-panel">
    <div class="sidebar-section">
      <div class="section-label">Top Routes</div>
      <div id="routes-subtitle"
           style="font-size:10px;color:var(--muted);line-height:1.5;margin-top:-4px;"></div>
    </div>
    <div id="routes-list"></div>
  </aside>

</div>

<!-- ═══════════════════════════════════════════════
     EMBEDDED DATA  (injected by Python)
═══════════════════════════════════════════════ -->
<script>
const GEOJSON     = __GEOJSON__;
const LOOKUP      = __LOOKUP__;
const MODES       = __MODES__;
const OD_DATA     = __OD_DATA__;
const ALL_PERIODS = __ALL_PERIODS__;
</script>

<!-- ═══════════════════════════════════════════════
     APP LOGIC
═══════════════════════════════════════════════ -->
<script>
// ── state ────────────────────────────────────────────────────────────
let state = {
  modes:      [MODES[0]],
  direction:  'origin',
  periodFrom: ALL_PERIODS[0],
  periodTo:   ALL_PERIODS[ALL_PERIODS.length - 1],
  scale:      'log',
};

// ── colour ramp (cool → warm, 9 stops) ───────────────────────────────
const RAMP = [
  [26, 31, 46],
  [29, 58, 92],
  [30, 95, 168],
  [26,143,196],
  [15,181,181],
  [76,222,138],
  [240,232, 74],
  [240,124, 32],
  [212, 32, 32],
];

function lerp(a, b, t) { return a + (b - a) * t; }

function rampColor(t) {
  const seg = (RAMP.length - 1) * Math.min(t, 0.9999);
  const i   = Math.floor(seg);
  const f   = seg - i;
  const c   = RAMP[i].map((v, idx) => Math.round(lerp(v, RAMP[i + 1][idx], f)));
  return 'rgb(' + c[0] + ',' + c[1] + ',' + c[2] + ')';
}

// ── data helpers ──────────────────────────────────────────────────────
function getZoneCountsMulti(modes, direction, from, to) {
  const counts  = {};
  const inRange = ALL_PERIODS.filter(function(p) { return p >= from && p <= to; });
  for (var mi = 0; mi < modes.length; mi++) {
    var src = LOOKUP[modes[mi]] && LOOKUP[modes[mi]][direction];
    if (!src) continue;
    for (var pi = 0; pi < inRange.length; pi++) {
      var parts = inRange[pi].split(' ');
      var y = parts[0];
      var q = parts[1][1]; // "Q3" -> "3"
      var qd = src[y] && src[y][q];
      if (!qd) continue;
      var keys = Object.keys(qd);
      for (var ki = 0; ki < keys.length; ki++) {
        var z = keys[ki];
        counts[z] = (counts[z] || 0) + qd[z];
      }
    }
  }
  return counts;
}

function getTopRoutes(modes, from, to, topN) {
  topN = topN || 15;
  const inRange = ALL_PERIODS.filter(function(p) { return p >= from && p <= to; });
  var od = {};
  for (var mi = 0; mi < modes.length; mi++) {
    var ms = OD_DATA[modes[mi]];
    if (!ms) continue;
    for (var pi = 0; pi < inRange.length; pi++) {
      var parts = inRange[pi].split(' ');
      var y = parts[0];
      var q = parts[1][1];
      var entries = ms[y] && ms[y][q];
      if (!entries) continue;
      for (var ei = 0; ei < entries.length; ei++) {
        var oid = entries[ei][0];
        var did = entries[ei][1];
        var cnt = entries[ei][2];
        var k = oid + '|' + did;
        od[k] = (od[k] || 0) + cnt;
      }
    }
  }
  var arr = Object.keys(od).map(function(k) { return [k, od[k]]; });
  arr.sort(function(a, b) { return b[1] - a[1]; });
  arr = arr.slice(0, topN);
  return arr.map(function(item) {
    var parts = item[0].split('|');
    return { originId: parts[0], destId: parts[1], count: item[1] };
  });
}

// ── map setup ─────────────────────────────────────────────────────────
const map = L.map('map', {
  center: [40.73, -73.95],
  zoom: 11,
  zoomControl: true,
  preferCanvas: true,
});

L.tileLayer(
  'https://{s}.basemaps.cartocdn.com/dark_nolabels/{z}/{x}/{y}{r}.png',
  { attribution: '\u00a9 OpenStreetMap \u00a9 CARTO', maxZoom: 19 }
).addTo(map);

L.tileLayer(
  'https://{s}.basemaps.cartocdn.com/dark_only_labels/{z}/{x}/{y}{r}.png',
  { attribution: '', maxZoom: 19, pane: 'overlayPane' }
).addTo(map);

var geojsonLayer = null;
var currentCounts = {};

var zoneNames = {};
for (var fi = 0; fi < GEOJSON.features.length; fi++) {
  var fp = GEOJSON.features[fi].properties;
  zoneNames[String(fp.LocationID)] = fp.zone + ' (' + fp.borough + ')';
}

function styleFeature(feature) {
  var zid   = String(feature.properties.LocationID);
  var count = currentCounts[zid] || 0;
  if (count === 0) {
    return { fillColor: '#1a1f2e', fillOpacity: 0.55, color: '#242836', weight: 0.5, opacity: 0.6 };
  }
  var vals = Object.values(currentCounts).filter(function(v) { return v > 0; });
  var minV = Math.min.apply(null, vals);
  var maxV = Math.max.apply(null, vals);
  var t;
  if (state.scale === 'log') {
    var logMin = Math.log1p(minV);
    var logMax = Math.log1p(maxV);
    t = logMax > logMin ? (Math.log1p(count) - logMin) / (logMax - logMin) : 1;
  } else {
    t = maxV > minV ? (count - minV) / (maxV - minV) : 1;
  }
  return {
    fillColor:   rampColor(t),
    fillOpacity: 0.80,
    color:       '#0d0f14',
    weight:      0.5,
    opacity:     0.8,
  };
}

function onEachFeature(feature, layer) {
  layer.on({
    mouseover: function(e) {
      var l     = e.target;
      l.setStyle({ weight: 2, color: '#e8c84a', fillOpacity: 0.95 });
      l.bringToFront();
      var zid   = String(feature.properties.LocationID);
      var count = currentCounts[zid] || 0;
      var name  = zoneNames[zid] || zid;
      l.bindTooltip(
        '<strong>' + name + '</strong>' +
        'Zone ID: ' + zid + '<br>' +
        'Trips: <b>' + count.toLocaleString() + '</b>',
        { className: 'zone-tooltip', sticky: true }
      ).openTooltip();
    },
    mouseout: function(e) {
      geojsonLayer.resetStyle(e.target);
      e.target.closeTooltip();
    },
  });
}

// ── render ────────────────────────────────────────────────────────────
function renderMap() {
  currentCounts = getZoneCountsMulti(
    state.modes, state.direction, state.periodFrom, state.periodTo
  );

  if (geojsonLayer) map.removeLayer(geojsonLayer);
  geojsonLayer = L.geoJSON(GEOJSON, {
    style:         styleFeature,
    onEachFeature: onEachFeature,
  }).addTo(map);

  var vals = Object.values(currentCounts);
  if (vals.length === 0) {
    document.getElementById('no-data').style.display = 'block';
    document.getElementById('st-zones').textContent = '0';
    document.getElementById('st-total').textContent = '0';
    document.getElementById('st-top').textContent   = '—';
    document.getElementById('st-max').textContent   = '0';
    return;
  }
  document.getElementById('no-data').style.display = 'none';

  var total  = vals.reduce(function(a, b) { return a + b; }, 0);
  var maxVal = Math.max.apply(null, vals);
  var topZone = null;
  var entries = Object.entries(currentCounts);
  for (var i = 0; i < entries.length; i++) {
    if (entries[i][1] === maxVal) { topZone = entries[i][0]; break; }
  }
  document.getElementById('st-zones').textContent = vals.length.toLocaleString();
  document.getElementById('st-total').textContent = total.toLocaleString();
  document.getElementById('st-top').textContent   = zoneNames[topZone] || topZone;
  document.getElementById('st-max').textContent   = maxVal.toLocaleString();
}

function renderRoutes() {
  var routes = getTopRoutes(state.modes, state.periodFrom, state.periodTo);
  var el = document.getElementById('routes-list');
  el.innerHTML = '';

  if (!routes.length) {
    el.innerHTML = '<div style="padding:16px;color:var(--muted);font-size:11px;">No route data available.</div>';
    document.getElementById('routes-subtitle').textContent = '';
    return;
  }

  var maxC = routes[0].count;
  for (var i = 0; i < routes.length; i++) {
    var r   = routes[i];
    var on  = zoneNames[r.originId] || ('Zone ' + r.originId);
    var dn  = zoneNames[r.destId]   || ('Zone ' + r.destId);
    var pct = (r.count / maxC * 100).toFixed(1);
    var div = document.createElement('div');
    div.className = 'route-item';
    div.innerHTML =
      '<div class="route-rank">#' + (i + 1) + '</div>' +
      '<div class="route-label" title="' + on + ' \u2192 ' + dn + '">' +
        on + '<span class="arr">\u2192</span>' + dn +
      '</div>' +
      '<div class="route-count">' + r.count.toLocaleString() + '</div>' +
      '<div class="route-bar"><div class="route-bar-fill" style="width:' + pct + '%"></div></div>';
    el.appendChild(div);
  }

  var nm = state.modes.length === MODES.length ? 'All modes' :
           state.modes.length === 1 ? state.modes[0] :
           state.modes.length + ' modes';
  document.getElementById('routes-subtitle').textContent =
    nm + ' \u00b7 ' + state.periodFrom + ' \u2013 ' + state.periodTo;
}

function renderAll() {
  renderMap();
  renderRoutes();
}

// ── UI builders ───────────────────────────────────────────────────────
function buildModeToggles() {
  var c = document.getElementById('mode-toggles');
  c.innerHTML = '';
  for (var i = 0; i < MODES.length; i++) {
    var mode = MODES[i];
    var btn  = document.createElement('button');
    btn.className = 'pill mode-pill' + (state.modes.indexOf(mode) >= 0 ? ' active' : '');
    btn.dataset.mode = mode;
    btn.textContent  = mode;
    c.appendChild(btn);
  }
}

function populatePeriodSelects() {
  var ids = ['sel-from', 'sel-to'];
  for (var j = 0; j < ids.length; j++) {
    var el = document.getElementById(ids[j]);
    el.innerHTML = '';
    for (var i = 0; i < ALL_PERIODS.length; i++) {
      var o = document.createElement('option');
      o.value = o.textContent = ALL_PERIODS[i];
      el.appendChild(o);
    }
  }
  document.getElementById('sel-from').value = state.periodFrom;
  document.getElementById('sel-to').value   = state.periodTo;
}

// ── event listeners ───────────────────────────────────────────────────
document.getElementById('mode-toggles').addEventListener('click', function(e) {
  var btn = e.target;
  while (btn && !btn.dataset.mode) btn = btn.parentElement;
  if (!btn) return;
  var m = btn.dataset.mode;
  var idx = state.modes.indexOf(m);
  if (idx >= 0) {
    if (state.modes.length > 1) {
      state.modes.splice(idx, 1);
      btn.classList.remove('active');
    }
  } else {
    state.modes.push(m);
    btn.classList.add('active');
  }
  renderAll();
});

document.getElementById('sel-from').addEventListener('change', function(e) {
  state.periodFrom = e.target.value;
  if (state.periodFrom > state.periodTo) {
    state.periodTo = state.periodFrom;
    document.getElementById('sel-to').value = state.periodTo;
  }
  renderAll();
});

document.getElementById('sel-to').addEventListener('change', function(e) {
  state.periodTo = e.target.value;
  if (state.periodTo < state.periodFrom) {
    state.periodFrom = state.periodTo;
    document.getElementById('sel-from').value = state.periodFrom;
  }
  renderAll();
});

document.querySelectorAll('[data-dir]').forEach(function(btn) {
  btn.addEventListener('click', function() {
    document.querySelectorAll('[data-dir]').forEach(function(b) {
      b.classList.remove('active');
    });
    btn.classList.add('active');
    state.direction = btn.dataset.dir;
    renderAll();
  });
});

document.querySelectorAll('[data-scale]').forEach(function(btn) {
  btn.addEventListener('click', function() {
    document.querySelectorAll('[data-scale]').forEach(function(b) {
      b.classList.remove('active');
    });
    btn.classList.add('active');
    state.scale = btn.dataset.scale;
    renderAll();
  });
});

// ── boot ──────────────────────────────────────────────────────────────
buildModeToggles();
populatePeriodSelects();
document.getElementById('loading').style.display = 'none';
renderAll();
</script>
</body>
</html>
"""

# ── main ──────────────────────────────────────────────────────────────────────

def main():
    avail = available_datasets()
    if not avail:
        print("ERROR: No parquet files found. Run the cleaning pipeline first.")
        sys.exit(1)

    print(f"Found {len(avail)} dataset(s): {', '.join(avail.keys())}")

    # ── DuckDB connection ─────────────────────────────────────────────────────
    con = duckdb.connect()
    con.execute(f"SET temp_directory = '{DUCKDB_TEMP}'")
    con.execute("SET memory_limit = '6GB'")
    con.execute("SET threads = 4")

    # ── aggregate all datasets ────────────────────────────────────────────────
    frames    = []
    od_frames = []
    for label, meta in avail.items():
        print(f"Aggregating: {label} …")
        df = aggregate_dataset(con, label, meta)
        if not df.empty:
            frames.append(df)
        od_df = aggregate_od_dataset(con, label, meta, top_n=100)
        if not od_df.empty:
            od_frames.append(od_df)

    con.close()

    if not frames:
        print("ERROR: All aggregations failed.")
        sys.exit(1)

    all_df = pd.concat(frames, ignore_index=True)
    print(f"\nTotal summary rows: {len(all_df):,}")

    # ── build lookups & metadata ──────────────────────────────────────────────
    lookup, _, modes = build_lookup(all_df)

    periods_df  = all_df[["year", "quarter"]].drop_duplicates()
    all_periods = sorted(
        f"{int(r.year)} Q{int(r.quarter)}"
        for r in periods_df.itertuples()
    )
    print(f"Periods: {all_periods}")
    print(f"Modes:   {modes}")

    if od_frames:
        all_od_df = pd.concat(od_frames, ignore_index=True)
        od_lookup = build_od_lookup(all_od_df)
        print(f"OD entries: {len(all_od_df):,}")
    else:
        od_lookup = {}
        print("Warning: no OD data — Top Routes panel will be empty")

    # ── load shapefile ────────────────────────────────────────────────────────
    print(f"\nLoading shapefile: {SHAPEFILE} …")
    geojson = load_geojson(SHAPEFILE)
    print(f"  {len(geojson['features'])} zones loaded")

    # ── inject data into HTML template ───────────────────────────────────────
    html = HTML_TEMPLATE
    html = html.replace("__GEOJSON__",     json.dumps(geojson))
    html = html.replace("__LOOKUP__",      json.dumps(lookup))
    html = html.replace("__MODES__",       json.dumps(modes))
    html = html.replace("__OD_DATA__",     json.dumps(od_lookup))
    html = html.replace("__ALL_PERIODS__", json.dumps(all_periods))

    OUT_HTML.write_text(html, encoding="utf-8")
    size_mb = OUT_HTML.stat().st_size / 1_048_576
    print(f"\n✓ Written: {OUT_HTML}  ({size_mb:.1f} MB)")


if __name__ == "__main__":
    main()
