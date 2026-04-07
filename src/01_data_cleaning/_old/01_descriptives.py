"""
src/03_descriptives/01_market_thickness.py
parents[2] -> Tesi/

Market thickness diagnostics and descriptive statistics.
Sections:
    1. Sparsity by mode
    2. Temporal granularity sensitivity  (OQ-1)
    3. Spatial granularity sensitivity
    4. Mode co-presence                  (OQ-2 / OQ-2b)
    5. Lorenz concentration
    6. Temporal demand patterns (hour-of-day, day-of-week)
    7. Top origin / destination zones
    8. Temporal trend (monthly volume)
    9. Price distribution by mode

Resume support: each section checks whether its Excel sheet already exists
in the workbook. If so, the heavy DuckDB query is skipped and the sheet is
reloaded from disk. Run the script again after a crash and it picks up where
it left off.

RAM strategy: DuckDB reads parquet directly from disk. Quantiles computed in
SQL (APPROX_QUANTILE) — no raw arrays ever loaded into Python.

Dependencies: duckdb, pandas, matplotlib, openpyxl
    pip install duckdb openpyxl
"""

import importlib.util
from pathlib import Path
from datetime import datetime

import duckdb
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# ── logging ───────────────────────────────────────────────────────────────────
def _ts():
    return datetime.now().strftime("%H:%M:%S")

def log(msg):   print(f"[{_ts()}] {msg}")
def section(t): print(f"\n{'='*78}\n[{_ts()}] {t}\n{'='*78}")
def substep(m): print(f"[{_ts()}]   -> {m}")
def ok(m):      print(f"[{_ts()}]   OK {m}")
def warn(m):    print(f"[{_ts()}]   [skip] {m}")

# ── config ────────────────────────────────────────────────────────────────────
log("Loading 00_config.py ...")
_cfg_path = Path(__file__).parents[2] / "00_config.py"
_spec = importlib.util.spec_from_file_location("config", _cfg_path)
_mod  = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
cfg = _mod.cfg

# ── constants ─────────────────────────────────────────────────────────────────
N_ZONES         = 263
THRESHOLDS      = [1, 5, 10, 30]
TLC_MODES       = ["yellow", "green", "uber", "lyft", "via"]
MTA_DATASETS    = ["subway", "bus"]
QUANTILE_POINTS = [i / 200 for i in range(1, 200)]   # 199 points

COLORS = {
    "yellow": "#E6AC00", "green": "#2E8B57", "uber": "#222222",
    "lyft": "#CC00AA",   "via":   "#5B2D8E", "subway": "#0039A6",
    "bus":  "#6CBE45",
}
DOW_LABELS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

FARE_COL = {
    "yellow": "avg_fare_amount",
    "green":  "avg_fare_amount",
    "uber":   "avg_base_passenger_fare",
    "lyft":   "avg_base_passenger_fare",
    "via":    "avg_base_passenger_fare",
}

OUT_TABLES  = Path(cfg.tables)  / "descriptives"
OUT_FIGURES = Path(cfg.figures) / "descriptives"
OUT_TABLES.mkdir(parents=True, exist_ok=True)
OUT_FIGURES.mkdir(parents=True, exist_ok=True)

XL_PATH = OUT_TABLES / "market_thickness_all.xlsx"

# ── DuckDB ────────────────────────────────────────────────────────────────────
TMP_DIR = Path(cfg.interim).drive + r"\tesi\tmp"
Path(TMP_DIR).mkdir(parents=True, exist_ok=True)
con = duckdb.connect()
con.execute(f"SET temp_directory = '{TMP_DIR}'")
con.execute("SET memory_limit = '8GB'")
con.execute("SET threads = 4")
ok(f"DuckDB ready (tmp={TMP_DIR})")

# ── resume helpers ────────────────────────────────────────────────────────────
def _existing_sheets() -> dict:
    """Load all sheets already present in the Excel workbook."""
    if not XL_PATH.exists():
        return {}
    try:
        return pd.read_excel(XL_PATH, sheet_name=None)
    except Exception as e:
        warn(f"Could not read existing workbook: {e}")
        return {}

def _fig_exists(name: str) -> bool:
    return (OUT_FIGURES / name).exists()

# Load whatever is already on disk
xl_sheets = _existing_sheets()
if xl_sheets:
    log(f"Resuming — found {len(xl_sheets)} existing sheet(s): {list(xl_sheets.keys())}")
else:
    log("No existing workbook found — running from scratch")

def need_section(sheet_name: str) -> bool:
    """Return True if this section still needs to run."""
    if sheet_name in xl_sheets:
        log(f"Sheet '{sheet_name}' already exists — skipping section")
        return False
    return True

def register(df: pd.DataFrame, sheet: str):
    xl_sheets[sheet] = df
    ok(f"Registered sheet '{sheet}' ({len(df):,} rows)")

def flush():
    """Write / overwrite the workbook with all current sheets."""
    substep(f"Writing workbook -> {XL_PATH}")
    with pd.ExcelWriter(XL_PATH, engine="openpyxl") as writer:
        for sname, df in xl_sheets.items():
            df.to_excel(writer, sheet_name=sname, index=False)
    ok(f"Workbook saved ({len(xl_sheets)} sheets)")

def save_tex(df: pd.DataFrame, stem: str, caption: str, label: str,
             float_fmt: str = "%.2f"):
    out = OUT_TABLES / f"{stem}.tex"
    df.to_latex(out, index=False, caption=caption, label=label,
                float_format=float_fmt, longtable=False)
    ok(f"LaTeX saved: {out.name}")

# ── SOURCES ───────────────────────────────────────────────────────────────────
section("SOURCE DISCOVERY")
SOURCES = {}
for m in TLC_MODES:
    p = Path(cfg.interim) / "tlc" / f"{m}_zone_hour.parquet"
    if p.exists():
        SOURCES[m] = {"path": str(p).replace("\\", "/"), "filter": "", "is_float": False}
        ok(f"TLC '{m}' found")
    else:
        warn(f"{m}_zone_hour.parquet not found")

mta_path = Path(cfg.interim) / "mta" / "mta_zone_hour.parquet"
if mta_path.exists():
    mta_p = str(mta_path).replace("\\", "/")
    present = con.execute(
        f"SELECT DISTINCT dataset FROM read_parquet('{mta_p}')"
    ).df()["dataset"].tolist()
    ok(f"MTA datasets present: {present}")
    for ds in MTA_DATASETS:
        if ds in present:
            SOURCES[ds] = {"path": mta_p, "filter": f"AND dataset = '{ds}'", "is_float": True}
            ok(f"MTA '{ds}' registered")
        else:
            warn(f"MTA dataset '{ds}' not found")
else:
    warn("mta_zone_hour.parquet not found")

available = list(SOURCES.keys())
log(f"Modes available: {available}")

# ── SQL helpers ───────────────────────────────────────────────────────────────
def threshold_cols(trip_col: str = "tot") -> str:
    return ", ".join(
        f'ROUND(100.0*SUM(CASE WHEN {trip_col}>={t} THEN 1 ELSE 0 END)/COUNT(*),2) AS "ge{t}"'
        for t in THRESHOLDS
    )

def rename_thresholds(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns={f"ge{t}": f">={t} units (%)" for t in THRESHOLDS})

def q_sql(col: str = "tot") -> str:
    return ", ".join(
        f"APPROX_QUANTILE({col}, {q}) AS q{i}"
        for i, q in enumerate(QUANTILE_POINTS)
    )

# ── Lorenz helper (defined once, outside any loop) ────────────────────────────
def pairs_for(cumvol_arr: np.ndarray, n: int, target: float) -> float:
    return round(100 * np.searchsorted(cumvol_arr, target) / n, 2)

# ══════════════════════════════════════════════════════════════════════════════
# 1. SPARSITY BY MODE
# ══════════════════════════════════════════════════════════════════════════════
section("1. SPARSITY BY MODE")
SHEET1 = "1_Sparsity_by_mode"
if need_section(SHEET1):
    rows = []
    for m, s in SOURCES.items():
        p, f = s["path"], s["filter"]
        substep(f"Querying sparsity for '{m}' ...")
        r = con.execute(f"""
            SELECT
                COUNT(*)                                          AS observed_cells,
                COUNT(DISTINCT date)*{N_ZONES}*{N_ZONES}*24      AS possible_cells,
                ROUND(100.0*COUNT(*)/
                    (COUNT(DISTINCT date)*{N_ZONES}*{N_ZONES}*24),4) AS fill_rate_pct,
                ROUND(AVG(trip_count),2)                          AS mean_demand,
                APPROX_QUANTILE(trip_count,0.05)                  AS p5,
                APPROX_QUANTILE(trip_count,0.25)                  AS p25,
                APPROX_QUANTILE(trip_count,0.50)                  AS p50,
                APPROX_QUANTILE(trip_count,0.75)                  AS p75,
                APPROX_QUANTILE(trip_count,0.90)                  AS p90,
                APPROX_QUANTILE(trip_count,0.99)                  AS p99,
                SUM(trip_count)                                   AS total_demand
            FROM read_parquet('{p}')
            WHERE trip_count > 0 {f}
        """).df()
        r.insert(0, "mode", m)
        rows.append(r)
        ok(f"[{m}] fill={r['fill_rate_pct'].iloc[0]:.4f}% p50={r['p50'].iloc[0]:.1f}")
    t1 = pd.concat(rows, ignore_index=True)
    register(t1, SHEET1)
    flush()
    save_tex(t1, "sparsity_by_mode",
        caption=(
            r"Market sparsity by mode. Each cell is an (origin zone, destination zone, "
            r"date, hour) combination. \emph{Fill rate} = share of cells observed out of "
            r"the theoretical maximum ($263\times263\times24\times\text{days}$). "
            r"A low fill rate implies zero or unstable market shares, which can make the "
            r"logit likelihood diverge. For subway and bus, \texttt{trip\_count} is an "
            r"estimated flow (float) rather than a direct trip count."
        ),
        label="tab:sparsity_by_mode")
else:
    t1 = xl_sheets[SHEET1]

# ══════════════════════════════════════════════════════════════════════════════
# 2. TEMPORAL SENSITIVITY
# ══════════════════════════════════════════════════════════════════════════════
section("2. TEMPORAL GRANULARITY SENSITIVITY")
SHEET2 = "2_Temporal_sensitivity"
if need_section(SHEET2):
    sens_rows = []
    for m, s in SOURCES.items():
        p, f = s["path"], s["filter"]
        for gran_name, group_cols in [
            ("hour x date",
             "PULocationID, DOLocationID, date, hour"),
            ("dow x hour",
             "PULocationID, DOLocationID, DAYOFWEEK(date) AS dow, hour"),
            ("3h block x date",
             """PULocationID, DOLocationID, date,
                CASE WHEN hour BETWEEN 6  AND 8  THEN 'Morning Peak (6-9)'
                     WHEN hour BETWEEN 9  AND 14 THEN 'Midday (9-15)'
                     WHEN hour BETWEEN 15 AND 17 THEN 'Evening Peak (15-18)'
                     ELSE 'Night (18-6)' END AS block"""),
        ]:
            substep(f"[{m}] granularity '{gran_name}' ...")
            r = con.execute(f"""
                WITH cells AS (
                    SELECT {group_cols}, SUM(trip_count) AS tot
                    FROM read_parquet('{p}')
                    WHERE trip_count > 0 {f}
                    GROUP BY ALL
                )
                SELECT COUNT(*) AS n_cells, {threshold_cols()} FROM cells
            """).df()
            r.insert(0, "granularity", gran_name)
            r.insert(0, "mode", m)
            sens_rows.append(r)
        ok(f"[{m}] done")
    t2 = rename_thresholds(pd.concat(sens_rows, ignore_index=True))
    register(t2, SHEET2)
    flush()
    save_tex(t2, "sensitivity_temporal",
        caption=(
            r"Temporal granularity sensitivity. For each mode and time-slot definition, "
            r"the table reports the share (\%) of cells with at least $N$ demand units. "
            r"Cells below 5--10 units produce unstable empirical market shares. "
            r"Aggregating to 3-hour blocks (OQ-1) substantially reduces sparsity at "
            r"the cost of within-day variation."
        ),
        label="tab:sensitivity_temporal")
else:
    t2 = xl_sheets[SHEET2]

# ══════════════════════════════════════════════════════════════════════════════
# 3. SPATIAL SENSITIVITY
# ══════════════════════════════════════════════════════════════════════════════
section("3. SPATIAL GRANULARITY SENSITIVITY")
SHEET3 = "3_Spatial_sensitivity"
if need_section(SHEET3):
    borough_lookup = Path(cfg.nyc_zones) / "taxi_zone_lookup.csv"
    has_borough = borough_lookup.exists()
    if has_borough:
        con.execute(f"""
            CREATE OR REPLACE TABLE borough_map AS
            SELECT LocationID, Borough
            FROM read_csv_auto('{str(borough_lookup).replace(chr(92), "/")}')
        """)
        ok("borough_map loaded")
    else:
        warn("taxi_zone_lookup.csv not found; borough aggregation skipped")

    spat_rows = []
    for m, s in SOURCES.items():
        p, f = s["path"], s["filter"]
        substep(f"[{m}] OD full ...")
        r = con.execute(f"""
            WITH cells AS (
                SELECT PULocationID, DOLocationID, DAYOFWEEK(date) AS dow, hour,
                       SUM(trip_count) AS tot
                FROM read_parquet('{p}') WHERE trip_count > 0 {f}
                GROUP BY ALL
            )
            SELECT COUNT(*) AS n_cells, {threshold_cols()} FROM cells
        """).df()
        r.insert(0, "spatial", "OD full (263x263)"); r.insert(0, "mode", m)
        spat_rows.append(r)

        substep(f"[{m}] origin only ...")
        r = con.execute(f"""
            WITH cells AS (
                SELECT PULocationID, DAYOFWEEK(date) AS dow, hour,
                       SUM(trip_count) AS tot
                FROM read_parquet('{p}') WHERE trip_count > 0 {f}
                GROUP BY ALL
            )
            SELECT COUNT(*) AS n_cells, {threshold_cols()} FROM cells
        """).df()
        r.insert(0, "spatial", "Origin only (263)"); r.insert(0, "mode", m)
        spat_rows.append(r)

        if has_borough:
            substep(f"[{m}] borough OD ...")
            r = con.execute(f"""
                WITH trips AS (
                    SELECT b1.Borough AS PU_boro, b2.Borough AS DO_boro,
                           DAYOFWEEK(t.date) AS dow, t.hour, t.trip_count
                    FROM read_parquet('{p}') t
                    LEFT JOIN borough_map b1 ON t.PULocationID = b1.LocationID
                    LEFT JOIN borough_map b2 ON t.DOLocationID = b2.LocationID
                    WHERE t.trip_count > 0 {f}
                ),
                cells AS (
                    SELECT PU_boro, DO_boro, dow, hour, SUM(trip_count) AS tot
                    FROM trips GROUP BY ALL
                )
                SELECT COUNT(*) AS n_cells, {threshold_cols()} FROM cells
            """).df()
            r.insert(0, "spatial", "Borough OD (5x5)"); r.insert(0, "mode", m)
            spat_rows.append(r)
        ok(f"[{m}] done")

    t3 = rename_thresholds(pd.concat(spat_rows, ignore_index=True))
    register(t3, SHEET3)
    flush()
    save_tex(t3, "sensitivity_spatial",
        caption=(
            r"Spatial granularity sensitivity (time slot fixed at dow $\times$ hour). "
            r"``OD full'' uses all 263 TLC zones; ``Origin only'' collapses destinations; "
            r"``Borough OD'' aggregates to borough level (5 areas). Coarser aggregation "
            r"reduces sparsity but loses intra-zone variation needed for identification."
        ),
        label="tab:sensitivity_spatial")
else:
    t3 = xl_sheets[SHEET3]

# ══════════════════════════════════════════════════════════════════════════════
# 4. MODE CO-PRESENCE
# ══════════════════════════════════════════════════════════════════════════════
section("4. MODE CO-PRESENCE")
SHEET4A, SHEET4B = "4_Copresence_n_modes", "4b_Copresence_by_mode"
if need_section(SHEET4A):
    for m, s in SOURCES.items():
        p, f = s["path"], s["filter"]
        substep(f"Materialising active cells for '{m}' ...")
        con.execute(f"""
            CREATE OR REPLACE TABLE cells_{m} AS
            SELECT PULocationID, DOLocationID, DAYOFWEEK(date) AS dow, hour
            FROM read_parquet('{p}')
            WHERE trip_count > 0 {f}
            GROUP BY ALL
        """)
        n = con.execute(f"SELECT COUNT(*) FROM cells_{m}").fetchone()[0]
        ok(f"[{m}] {n:,} active cells")

    union_tables = " UNION ALL ".join(
        f"SELECT PULocationID, DOLocationID, dow, hour, '{m}' AS mode FROM cells_{m}"
        for m in available
    )

    cop = con.execute(f"""
        WITH presence AS (
            SELECT PULocationID, DOLocationID, dow, hour, mode
            FROM ({union_tables}) GROUP BY ALL
        ),
        counts AS (
            SELECT PULocationID, DOLocationID, dow, hour,
                   COUNT(DISTINCT mode) AS n_modes
            FROM presence GROUP BY ALL
        )
        SELECT n_modes,
               COUNT(*) AS n_cells,
               ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER (),2) AS share_pct
        FROM counts GROUP BY n_modes ORDER BY n_modes
    """).df()

    total_cells = con.execute(f"""
        SELECT COUNT(*) FROM (
            SELECT PULocationID, DOLocationID, dow, hour
            FROM ({union_tables}) GROUP BY ALL
        )
    """).fetchone()[0]

    cop_mode = pd.DataFrame([
        {"mode": m,
         "cells_present": con.execute(f"SELECT COUNT(*) FROM cells_{m}").fetchone()[0],
         "total_cells": total_cells,
         "presence_pct": round(
             100 * con.execute(f"SELECT COUNT(*) FROM cells_{m}").fetchone()[0] / total_cells, 2
         )}
        for m in available
    ])

    register(cop,      SHEET4A)
    register(cop_mode, SHEET4B)
    flush()
    save_tex(cop, "copresence_n_modes",
        caption=(
            r"Distribution of market cells (origin $\times$ destination $\times$ "
            r"dow $\times$ hour) by number of modes present (demand $>0$). "
            r"Modes: " + ", ".join(available) + r". "
            r"Cells with $n=1$ identify no substitution parameters. "
            r"The share with $n\geq3$ measures the sample useful for structural estimation."
        ),
        label="tab:copresence_n_modes")
    save_tex(cop_mode, "copresence_by_mode",
        caption=(
            r"Share of market cells in which each mode is present (demand $>0$). "
            r"Reference: union of all cells observed in at least one mode. "
            r"Modes with low presence require an explicit availability dummy (OQ-2b)."
        ),
        label="tab:copresence_by_mode")
else:
    cop      = xl_sheets[SHEET4A]
    cop_mode = xl_sheets.get(SHEET4B, pd.DataFrame())
    # rebuild active-cell tables for figures (needed even if section skipped)
    substep("Rebuilding active-cell tables from parquet for figure use ...")
    for m, s in SOURCES.items():
        p, f = s["path"], s["filter"]
        con.execute(f"""
            CREATE OR REPLACE TABLE cells_{m} AS
            SELECT PULocationID, DOLocationID, DAYOFWEEK(date) AS dow, hour
            FROM read_parquet('{p}')
            WHERE trip_count > 0 {f}
            GROUP BY ALL
        """)
    union_tables = " UNION ALL ".join(
        f"SELECT PULocationID, DOLocationID, dow, hour, '{m}' AS mode FROM cells_{m}"
        for m in available
    )

# ══════════════════════════════════════════════════════════════════════════════
# 5. LORENZ CONCENTRATION
# ══════════════════════════════════════════════════════════════════════════════
section("5. LORENZ CONCENTRATION")
SHEET5 = "5_Lorenz"
lorenz_curves = {}   # always rebuilt (cheap: only OD-level aggregates)
if need_section(SHEET5):
    lorenz_rows = []
    for m, s in SOURCES.items():
        p, f = s["path"], s["filter"]
        substep(f"[{m}] aggregating OD totals ...")
        od_vol = con.execute(f"""
            SELECT SUM(trip_count) AS tot
            FROM read_parquet('{p}')
            WHERE trip_count > 0 {f}
            GROUP BY PULocationID, DOLocationID
            ORDER BY tot ASC
        """).df()["tot"].values
        total    = od_vol.sum()
        cumvol   = np.cumsum(od_vol) / total
        cumpairs = np.arange(1, len(od_vol)+1) / len(od_vol)
        lorenz_curves[m] = (cumpairs, cumvol)
        n = len(od_vol)
        lorenz_rows.append({
            "mode": m,
            "OD pairs observed": n,
            "pairs for 50% demand (%)": pairs_for(cumvol, n, 0.50),
            "pairs for 80% demand (%)": pairs_for(cumvol, n, 0.80),
            "pairs for 95% demand (%)": pairs_for(cumvol, n, 0.95),
        })
        ok(f"[{m}] {n:,} OD pairs")
    t5 = pd.DataFrame(lorenz_rows)
    register(t5, SHEET5)
    flush()
    save_tex(t5, "lorenz_concentration",
        caption=(
            r"Geographic concentration of demand by mode. "
            r"Share (\%) of OD pairs needed to cover 50\%, 80\%, 95\% of total demand. "
            r"Low values indicate high concentration on a few corridors "
            r"(Manhattan--Manhattan, airports). The long tail is a candidate for trimming."
        ),
        label="tab:lorenz_concentration")
else:
    t5 = xl_sheets[SHEET5]
    # rebuild lorenz_curves from the saved table (for figure)
    substep("Rebuilding Lorenz curves from saved table ...")
    for m, s in SOURCES.items():
        p, f = s["path"], s["filter"]
        od_vol = con.execute(f"""
            SELECT SUM(trip_count) AS tot
            FROM read_parquet('{p}')
            WHERE trip_count > 0 {f}
            GROUP BY PULocationID, DOLocationID
            ORDER BY tot ASC
        """).df()["tot"].values
        cumvol   = np.cumsum(od_vol) / od_vol.sum()
        cumpairs = np.arange(1, len(od_vol)+1) / len(od_vol)
        lorenz_curves[m] = (cumpairs, cumvol)

# ══════════════════════════════════════════════════════════════════════════════
# 6. TEMPORAL DEMAND PATTERNS
# ══════════════════════════════════════════════════════════════════════════════
section("6. TEMPORAL DEMAND PATTERNS")
SHEET6A, SHEET6B = "6a_Demand_by_hour", "6b_Demand_by_dow"
if need_section(SHEET6A):
    hod_rows, dow_rows = [], []
    for m, s in SOURCES.items():
        p, f = s["path"], s["filter"]
        substep(f"[{m}] hourly profile ...")
        r = con.execute(f"""
            SELECT hour,
                   SUM(trip_count) AS total_demand,
                   AVG(trip_count) AS avg_demand_per_cell,
                   COUNT(*)        AS n_cells
            FROM read_parquet('{p}')
            WHERE trip_count > 0 {f}
            GROUP BY hour ORDER BY hour
        """).df()
        r.insert(0, "mode", m); hod_rows.append(r)

        substep(f"[{m}] day-of-week profile ...")
        r = con.execute(f"""
            SELECT DAYOFWEEK(date) AS dow,
                   SUM(trip_count) AS total_demand,
                   AVG(trip_count) AS avg_demand_per_cell,
                   COUNT(*)        AS n_cells
            FROM read_parquet('{p}')
            WHERE trip_count > 0 {f}
            GROUP BY dow ORDER BY dow
        """).df()
        r.insert(0, "mode", m); dow_rows.append(r)
        ok(f"[{m}] done")

    t6a = pd.concat(hod_rows, ignore_index=True)
    t6b = pd.concat(dow_rows, ignore_index=True)
    register(t6a, SHEET6A); register(t6b, SHEET6B)
    flush()
    save_tex(t6a, "demand_by_hour",
        caption=(
            r"Demand by hour of day and mode. \texttt{avg\_demand\_per\_cell} "
            r"removes the composition effect of more cells being active at peak hours. "
            r"Diverging peak profiles provide descriptive evidence of imperfect substitutability."
        ),
        label="tab:demand_by_hour")
    save_tex(t6b, "demand_by_dow",
        caption=(
            r"Demand by day of week (0=Monday) and mode. "
            r"Weekday vs.\ weekend differences motivate a day-of-week fixed effect."
        ),
        label="tab:demand_by_dow")
else:
    t6a = xl_sheets[SHEET6A]
    t6b = xl_sheets.get(SHEET6B, pd.DataFrame())

# ══════════════════════════════════════════════════════════════════════════════
# 7. TOP ZONES
# ══════════════════════════════════════════════════════════════════════════════
section("7. TOP ORIGIN / DESTINATION ZONES")
SHEET7A, SHEET7B = "7a_Top_origin_zones", "7b_Top_dest_zones"
TOP_N = 20
if need_section(SHEET7A):
    top_pu_rows, top_do_rows = [], []
    for m, s in SOURCES.items():
        p, f = s["path"], s["filter"]
        for direction, col, store in [
            ("origin",      "PULocationID", top_pu_rows),
            ("destination", "DOLocationID", top_do_rows),
        ]:
            substep(f"[{m}] top-{TOP_N} {direction} zones ...")
            r = con.execute(f"""
                SELECT {col} AS zone_id,
                       SUM(trip_count) AS total_demand,
                       COUNT(*)        AS n_cells
                FROM read_parquet('{p}')
                WHERE trip_count > 0 {f}
                GROUP BY zone_id
                ORDER BY total_demand DESC
                LIMIT {TOP_N}
            """).df()
            r.insert(0, "direction", direction); r.insert(0, "mode", m)
            store.append(r)
        ok(f"[{m}] done")
    t7a = pd.concat(top_pu_rows, ignore_index=True)
    t7b = pd.concat(top_do_rows, ignore_index=True)
    register(t7a, SHEET7A); register(t7b, SHEET7B)
    flush()
    save_tex(t7a, "top_origin_zones",
        caption=(
            rf"Top {TOP_N} origin zones by total demand, by mode (TLC LocationID). "
            r"High overlap across modes supports zone-level fixed effects."
        ),
        label="tab:top_origin_zones")
else:
    t7a = xl_sheets[SHEET7A]
    t7b = xl_sheets.get(SHEET7B, pd.DataFrame())

# ══════════════════════════════════════════════════════════════════════════════
# 8. MONTHLY TREND
# ══════════════════════════════════════════════════════════════════════════════
section("8. MONTHLY VOLUME TREND")
SHEET8 = "8_Monthly_trend"
if need_section(SHEET8):
    trend_rows = []
    for m, s in SOURCES.items():
        p, f = s["path"], s["filter"]
        substep(f"[{m}] monthly aggregation ...")
        r = con.execute(f"""
            SELECT YEAR(date) AS year, MONTH(date) AS month,
                   SUM(trip_count) AS total_demand, COUNT(*) AS n_cells
            FROM read_parquet('{p}')
            WHERE trip_count > 0 {f}
            GROUP BY year, month ORDER BY year, month
        """).df()
        r["year_month"] = r["year"].astype(str) + "-" + r["month"].astype(str).str.zfill(2)
        r.insert(0, "mode", m); trend_rows.append(r)
        ok(f"[{m}] {len(r)} months")
    t8 = pd.concat(trend_rows, ignore_index=True)
    register(t8, SHEET8)
    flush()
    save_tex(t8, "monthly_trend",
        caption=(
            r"Monthly demand volume by mode (2021--2025). Captures the post-COVID "
            r"recovery, the relative growth of HVFHV vs.\ taxis, and structural breaks "
            r"(Congestion Pricing from Jan 2025). Informs the estimation window."
        ),
        label="tab:monthly_trend")
else:
    t8 = xl_sheets[SHEET8]

# ══════════════════════════════════════════════════════════════════════════════
# 9. PRICE DISTRIBUTION
# ══════════════════════════════════════════════════════════════════════════════
section("9. PRICE DISTRIBUTION BY MODE")
SHEET9 = "9_Price_distribution"
price_quantiles = {}
if need_section(SHEET9):
    price_rows = []
    for m, fare_col in FARE_COL.items():
        if m not in SOURCES:
            warn(f"Mode '{m}' not available; skipping price"); continue
        p = SOURCES[m]["path"]
        substep(f"[{m}] fare summary using '{fare_col}' ...")
        try:
            r = con.execute(f"""
                SELECT
                    ROUND(AVG({fare_col}),2)            AS mean_fare,
                    APPROX_QUANTILE({fare_col},0.05)    AS p5,
                    APPROX_QUANTILE({fare_col},0.25)    AS p25,
                    APPROX_QUANTILE({fare_col},0.50)    AS p50,
                    APPROX_QUANTILE({fare_col},0.75)    AS p75,
                    APPROX_QUANTILE({fare_col},0.90)    AS p90,
                    APPROX_QUANTILE({fare_col},0.95)    AS p95,
                    ROUND(STDDEV({fare_col}),2)         AS std_fare,
                    COUNT(*)                            AS n_cells
                FROM read_parquet('{p}')
                WHERE {fare_col} > 0 AND {fare_col} < 500
            """).df()
            r.insert(0, "fare_column", fare_col); r.insert(0, "mode", m)
            price_rows.append(r)

            substep(f"[{m}] SQL quantiles for box figure ...")
            qrow = con.execute(f"""
                WITH cells AS (
                    SELECT {fare_col} AS fare
                    FROM read_parquet('{p}')
                    WHERE {fare_col} > 0 AND {fare_col} < 500
                )
                SELECT {q_sql('fare')} FROM cells
            """).df()
            price_quantiles[m] = qrow.iloc[0].values.astype(float)
            ok(f"[{m}] mean=${r['mean_fare'].iloc[0]:.2f} p50=${r['p50'].iloc[0]:.2f}")
        except Exception as e:
            warn(f"[{m}] '{fare_col}' error: {e}")

    if price_rows:
        t9 = pd.concat(price_rows, ignore_index=True)
        register(t9, SHEET9)
        flush()
        save_tex(t9, "price_distribution",
            caption=(
                r"Fare distribution by mode from zone-hour aggregated files. "
                r"For taxi, \texttt{avg\_fare\_amount} is the metered base fare; "
                r"for HVFHV, \texttt{avg\_base\_passenger\_fare} includes surge pricing "
                r"and is potentially endogenous ($\xi_{jt}$ correlated). "
                r"Fares $>\$500$ excluded as outliers. "
                r"Higher HVFHV dispersion motivates the IV strategy (OQ-3)."
            ),
            label="tab:price_distribution")
    else:
        t9 = pd.DataFrame()
else:
    t9 = xl_sheets[SHEET9]
    # rebuild price_quantiles for figure even if section was skipped
    substep("Rebuilding price quantiles for figure ...")
    for m, fare_col in FARE_COL.items():
        if m not in SOURCES:
            continue
        p = SOURCES[m]["path"]
        try:
            qrow = con.execute(f"""
                WITH cells AS (
                    SELECT {fare_col} AS fare
                    FROM read_parquet('{p}')
                    WHERE {fare_col} > 0 AND {fare_col} < 500
                )
                SELECT {q_sql('fare')} FROM cells
            """).df()
            price_quantiles[m] = qrow.iloc[0].values.astype(float)
            ok(f"[{m}] quantiles rebuilt")
        except Exception as e:
            warn(f"[{m}] could not rebuild quantiles: {e}")

# ══════════════════════════════════════════════════════════════════════════════
# FIGURES
# ══════════════════════════════════════════════════════════════════════════════
section("FIGURES")

# ── Fig 1 — CDF ───────────────────────────────────────────────────────────────
FIG1 = "fig1_cdf_demand_by_mode.pdf"
if _fig_exists(FIG1):
    warn(f"{FIG1} already exists — skipped")
else:
    substep("Building Fig 1: CDF of demand per cell ...")
    fig, ax = plt.subplots(figsize=(8, 5))
    for m, s in SOURCES.items():
        p, f = s["path"], s["filter"]
        try:
            qrow = con.execute(f"""
                WITH cells AS (
                    SELECT PULocationID, DOLocationID, date, hour,
                           SUM(trip_count) AS tot
                    FROM read_parquet('{p}')
                    WHERE trip_count > 0 {f}
                    GROUP BY ALL
                )
                SELECT {q_sql()} FROM cells
            """).df()
            x_vals = qrow.iloc[0].values.astype(float)
            ax.plot(x_vals, QUANTILE_POINTS, label=m, color=COLORS.get(m), lw=1.8)
        except Exception as e:
            warn(f"[{m}] CDF skipped: {e}")
    ax.set_xscale("log")
    for thresh, ls in zip([5, 30], ["--", ":"]):
        ax.axvline(thresh, color="grey", ls=ls, lw=0.9, alpha=0.7)
        ax.text(thresh, 0.02, f"  {thresh}", fontsize=8, color="grey")
    ax.set_xlabel("Demand units per cell (log scale)")
    ax.set_ylabel("CDF")
    ax.set_title("Demand distribution per market cell\n"
                 r"(origin $\times$ destination $\times$ date $\times$ hour)")
    ax.legend(fontsize=9); ax.grid(True, alpha=0.3)
    fig.text(0.5, -0.06,
        "CDF from 199 SQL quantiles (RAM-safe). Dashed lines: thresholds 5 and 30.\n"
        "Cells below these thresholds produce unstable empirical market shares.",
        ha="center", fontsize=8, color="#444444")
    fig.tight_layout()
    fig.savefig(OUT_FIGURES / FIG1, dpi=150, bbox_inches="tight")
    plt.close(fig)
    ok(f"Saved {FIG1}")

# ── Fig 2 — Heatmap fill rate ─────────────────────────────────────────────────
FIG2 = "fig2_heatmap_fillrate.pdf"
if _fig_exists(FIG2):
    warn(f"{FIG2} already exists — skipped")
else:
    substep("Building Fig 2: heatmap fill rate day x hour ...")
    ref = "yellow" if "yellow" in SOURCES else available[0]
    sr  = SOURCES[ref]
    hm  = con.execute(f"""
        SELECT DAYOFWEEK(date) AS dow, hour, COUNT(*) AS observed
        FROM read_parquet('{sr["path"]}')
        WHERE trip_count > 0 {sr["filter"]}
        GROUP BY ALL
    """).df()
    hm["fill_rate"] = hm["observed"] / (N_ZONES**2) * 100
    hm_piv = hm.pivot(index="dow", columns="hour", values="fill_rate").fillna(0)
    hm_piv = hm_piv.reindex(index=range(7), columns=range(24), fill_value=0)
    fig, ax = plt.subplots(figsize=(14, 4))
    im = ax.imshow(hm_piv.values, aspect="auto", cmap="YlOrRd",
                   vmin=0, vmax=hm_piv.values.max())
    ax.set_xticks(range(24)); ax.set_xticklabels(range(24), fontsize=8)
    ax.set_yticks(range(7)); ax.set_yticklabels(DOW_LABELS)
    ax.set_xlabel("Hour of day"); ax.set_ylabel("Day of week")
    ax.set_title(f"Fill rate (% active OD cells) by day x hour — {ref}\n"
                 f"(base: {N_ZONES}² = {N_ZONES**2:,} theoretical OD pairs)")
    plt.colorbar(im, ax=ax, label="Fill rate (%)")
    fig.text(0.5, -0.08,
        "Fill rate = share of OD pairs with positive demand in that slot.\n"
        "Low values mark rarely-served slots; collapsing them into broader blocks\n"
        "reduces sparsity without losing relevant information.",
        ha="center", fontsize=8, color="#444444")
    fig.tight_layout()
    fig.savefig(OUT_FIGURES / FIG2, dpi=150, bbox_inches="tight")
    plt.close(fig)
    ok(f"Saved {FIG2}")

# ── Fig 3 — Co-presence bar ───────────────────────────────────────────────────
FIG3 = "fig3_copresence_bar.pdf"
if _fig_exists(FIG3):
    warn(f"{FIG3} already exists — skipped")
else:
    substep("Building Fig 3: co-presence bar chart ...")
    fig, ax = plt.subplots(figsize=(6, 4))
    bars = ax.bar(cop["n_modes"].astype(str), cop["share_pct"],
                  color=plt.cm.Blues(np.linspace(0.35, 0.85, len(cop))))
    ax.set_xlabel("Number of modes present in cell")
    ax.set_ylabel("Share of cells (%)")
    ax.set_title("Mode co-presence per market cell\n"
                 r"(origin $\times$ destination $\times$ dow $\times$ hour)")
    for bar, (_, row) in zip(bars, cop.iterrows()):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.3,
                f"{row['share_pct']:.1f}%", ha="center", fontsize=9)
    ax.grid(True, axis="y", alpha=0.3)
    fig.text(0.5, -0.07,
        f"Modes: {', '.join(available)}.\n"
        "Cells with n=1 do not identify substitution parameters.\n"
        "Only n>=2 cells contribute to cross-price elasticity identification.",
        ha="center", fontsize=8, color="#444444")
    fig.tight_layout()
    fig.savefig(OUT_FIGURES / FIG3, dpi=150, bbox_inches="tight")
    plt.close(fig)
    ok(f"Saved {FIG3}")

# ── Fig 4 — Lorenz ────────────────────────────────────────────────────────────
FIG4 = "fig4_lorenz.pdf"
if _fig_exists(FIG4):
    warn(f"{FIG4} already exists — skipped")
else:
    substep("Building Fig 4: Lorenz curves ...")
    fig, ax = plt.subplots(figsize=(7, 5))
    ax.plot([0,1],[0,1], "k--", lw=0.8, label="Perfect equality")
    for m, (x, y) in lorenz_curves.items():
        ax.plot(x, y, label=m, color=COLORS.get(m), lw=1.8)
    ax.set_xlabel("Cumulative share of OD pairs (sorted ascending)")
    ax.set_ylabel("Cumulative share of demand")
    ax.set_title("Lorenz curves — geographic concentration of demand")
    ax.legend(fontsize=9); ax.grid(True, alpha=0.3)
    fig.text(0.5, -0.07,
        "Distance below the diagonal = concentration on few OD pairs.\n"
        "Manhattan--Manhattan and airports dominate road modes;\n"
        "main corridors dominate subway and bus.",
        ha="center", fontsize=8, color="#444444")
    fig.tight_layout()
    fig.savefig(OUT_FIGURES / FIG4, dpi=150, bbox_inches="tight")
    plt.close(fig)
    ok(f"Saved {FIG4}")

# ── Fig 5 — Hourly profile ────────────────────────────────────────────────────
FIG5 = "fig5_hourly_profile.pdf"
if _fig_exists(FIG5):
    warn(f"{FIG5} already exists — skipped")
else:
    substep("Building Fig 5: hourly demand profiles ...")
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    ax = axes[0]
    for m in available:
        df_m = t6a[t6a["mode"] == m]
        vals = df_m.set_index("hour")["total_demand"].reindex(range(24), fill_value=0)
        norm = vals / vals.max() if vals.max() > 0 else vals
        ax.plot(range(24), norm, label=m, color=COLORS.get(m), lw=1.8)
    ax.set_xlabel("Hour of day"); ax.set_ylabel("Normalised total demand (max=1)")
    ax.set_title("Hourly demand profile (normalised)")
    ax.legend(fontsize=8); ax.grid(True, alpha=0.3); ax.set_xticks(range(0, 24, 2))
    ax = axes[1]
    for m in available:
        df_m = t6a[t6a["mode"] == m]
        vals = df_m.set_index("hour")["avg_demand_per_cell"].reindex(range(24), fill_value=0)
        ax.plot(range(24), vals, label=m, color=COLORS.get(m), lw=1.8)
    ax.set_xlabel("Hour of day"); ax.set_ylabel("Avg demand per active cell")
    ax.set_title("Avg demand per cell by hour\n(removes composition effect)")
    ax.legend(fontsize=8); ax.grid(True, alpha=0.3); ax.set_xticks(range(0, 24, 2))
    fig.text(0.5, -0.05,
        "Left: normalised total demand highlights peak timing differences.\n"
        "Right: avg per active cell removes the mechanical peak-hours composition effect,\n"
        "showing intensive-margin variation.",
        ha="center", fontsize=8, color="#444444")
    fig.tight_layout()
    fig.savefig(OUT_FIGURES / FIG5, dpi=150, bbox_inches="tight")
    plt.close(fig)
    ok(f"Saved {FIG5}")

# ── Fig 6 — DoW profile ───────────────────────────────────────────────────────
FIG6 = "fig6_dow_profile.pdf"
if _fig_exists(FIG6):
    warn(f"{FIG6} already exists — skipped")
else:
    substep("Building Fig 6: day-of-week demand profile ...")
    fig, ax = plt.subplots(figsize=(8, 4))
    for m in available:
        df_m = t6b[t6b["mode"] == m]
        vals = df_m.set_index("dow")["total_demand"].reindex(range(7), fill_value=0)
        norm = vals / vals.max() if vals.max() > 0 else vals
        ax.plot(range(7), norm, marker="o", label=m, color=COLORS.get(m), lw=1.8, ms=5)
    ax.set_xticks(range(7)); ax.set_xticklabels(DOW_LABELS)
    ax.set_xlabel("Day of week"); ax.set_ylabel("Normalised total demand (max=1)")
    ax.set_title("Day-of-week demand profile by mode (normalised)")
    ax.legend(fontsize=9); ax.grid(True, alpha=0.3)
    fig.text(0.5, -0.06,
        "Weekday vs. weekend divergence across modes motivates\n"
        "including a day-of-week fixed effect in the utility specification.",
        ha="center", fontsize=8, color="#444444")
    fig.tight_layout()
    fig.savefig(OUT_FIGURES / FIG6, dpi=150, bbox_inches="tight")
    plt.close(fig)
    ok(f"Saved {FIG6}")

# ── Fig 7 — Monthly trend ─────────────────────────────────────────────────────
FIG7 = "fig7_monthly_trend.pdf"
if _fig_exists(FIG7):
    warn(f"{FIG7} already exists — skipped")
else:
    substep("Building Fig 7: monthly demand trend ...")
    fig, ax = plt.subplots(figsize=(12, 5))
    for m in available:
        df_m = t8[t8["mode"] == m].copy().sort_values("year_month")
        if df_m.empty:
            continue
        norm = df_m["total_demand"] / df_m["total_demand"].max()
        ax.plot(range(len(df_m)), norm, label=m, color=COLORS.get(m), lw=1.8)
    ref_m = max(available, key=lambda x: len(t8[t8["mode"] == x]))
    ticks_df = t8[t8["mode"] == ref_m].sort_values("year_month")
    tick_step = max(1, len(ticks_df) // 12)
    ax.set_xticks(range(0, len(ticks_df), tick_step))
    ax.set_xticklabels(ticks_df["year_month"].iloc[::tick_step],
                       rotation=45, ha="right", fontsize=7)
    ax.set_ylabel("Normalised monthly demand (max=1)")
    ax.set_title("Monthly demand trend by mode (normalised, 2021-2025)")
    ax.legend(fontsize=9); ax.grid(True, alpha=0.3)
    fig.text(0.5, -0.08,
        "Each mode normalised to its own maximum. Captures post-COVID recovery,\n"
        "HVFHV growth vs. taxi decline, and structural breaks\n"
        "(Congestion Pricing from Jan 2025). Informs the estimation window.",
        ha="center", fontsize=8, color="#444444")
    fig.tight_layout()
    fig.savefig(OUT_FIGURES / FIG7, dpi=150, bbox_inches="tight")
    plt.close(fig)
    ok(f"Saved {FIG7}")

# ── Fig 8 — Price distribution ────────────────────────────────────────────────
FIG8 = "fig8_price_distribution.pdf"
if _fig_exists(FIG8):
    warn(f"{FIG8} already exists — skipped")
elif price_quantiles:
    substep("Building Fig 8: price distribution box chart ...")
    fig, ax = plt.subplots(figsize=(8, 5))
    modes_with_price = list(price_quantiles.keys())
    for i, m in enumerate(modes_with_price):
        q   = price_quantiles[m]
        q05 = float(np.interp(0.05, QUANTILE_POINTS, q))
        q25 = float(np.interp(0.25, QUANTILE_POINTS, q))
        q50 = float(np.interp(0.50, QUANTILE_POINTS, q))
        q75 = float(np.interp(0.75, QUANTILE_POINTS, q))
        q95 = float(np.interp(0.95, QUANTILE_POINTS, q))
        col = COLORS.get(m, "grey")
        ax.bar(i, q75-q25, bottom=q25, width=0.5, color=col, alpha=0.7, label=m)
        ax.plot([i-0.25, i+0.25], [q50, q50], color="white", lw=2)
        ax.plot([i, i], [q05, q25], color=col, lw=1.5)
        ax.plot([i, i], [q75, q95], color=col, lw=1.5)
        ax.plot([i-0.1, i+0.1], [q05, q05], color=col, lw=1.5)
        ax.plot([i-0.1, i+0.1], [q95, q95], color=col, lw=1.5)
    ax.set_xticks(list(range(len(modes_with_price))))
    ax.set_xticklabels(modes_with_price)
    ax.set_ylabel("Average fare per cell (USD)")
    ax.set_title("Fare distribution by mode\n(box=IQR, whiskers=p5-p95, line=median)")
    ax.grid(True, axis="y", alpha=0.3)
    fig.text(0.5, -0.06,
        "avg_fare_amount for taxi; avg_base_passenger_fare for HVFHV (includes surge).\n"
        "Higher HVFHV dispersion reflects algorithmic surge pricing and motivates\n"
        "the IV strategy to address endogeneity (OQ-3).",
        ha="center", fontsize=8, color="#444444")
    fig.tight_layout()
    fig.savefig(OUT_FIGURES / FIG8, dpi=150, bbox_inches="tight")
    plt.close(fig)
    ok(f"Saved {FIG8}")

# ── Fig 9 — Yellow vs Subway heatmap ─────────────────────────────────────────
FIG9 = "fig9_heatmap_yellow_vs_subway.pdf"
if _fig_exists(FIG9):
    warn(f"{FIG9} already exists — skipped")
elif "subway" in SOURCES and "yellow" in SOURCES:
    substep("Building Fig 9: fill rate Yellow vs Subway ...")
    fig, axes = plt.subplots(2, 1, figsize=(14, 7), sharex=True)
    for ax, mode_ref in zip(axes, ["yellow", "subway"]):
        sr2 = SOURCES[mode_ref]
        hm2 = con.execute(f"""
            SELECT DAYOFWEEK(date) AS dow, hour, COUNT(*) AS observed
            FROM read_parquet('{sr2["path"]}')
            WHERE trip_count > 0 {sr2["filter"]}
            GROUP BY ALL
        """).df()
        hm2["fill_rate"] = hm2["observed"] / (N_ZONES**2) * 100
        piv = hm2.pivot(index="dow", columns="hour", values="fill_rate").fillna(0)
        piv = piv.reindex(index=range(7), columns=range(24), fill_value=0)
        im2 = ax.imshow(piv.values, aspect="auto", cmap="YlOrRd",
                        vmin=0, vmax=piv.values.max())
        ax.set_yticks(range(7)); ax.set_yticklabels(DOW_LABELS)
        ax.set_ylabel("Day"); ax.set_title(f"Fill rate — {mode_ref}")
        plt.colorbar(im2, ax=ax, label="Fill rate (%)")
    axes[-1].set_xticks(range(24)); axes[-1].set_xticklabels(range(24), fontsize=8)
    axes[-1].set_xlabel("Hour of day")
    fig.suptitle("Fill rate comparison: Yellow Taxi vs Subway\n"
                 r"(base: $263^2$ theoretical OD pairs per slot)", y=1.01)
    fig.text(0.5, -0.04,
        "Slots with high fill rate in both modes are best suited\n"
        "for estimating cross-price elasticities between taxi and subway.",
        ha="center", fontsize=8, color="#444444")
    fig.tight_layout()
    fig.savefig(OUT_FIGURES / FIG9, dpi=150, bbox_inches="tight")
    plt.close(fig)
    ok(f"Saved {FIG9}")

# ── final flush ───────────────────────────────────────────────────────────────
section("FINAL EXCEL FLUSH")
flush()

section("DONE")
log(f"Tables  : {OUT_TABLES}")
log(f"Figures : {OUT_FIGURES}")
log(f"Workbook: {XL_PATH}")