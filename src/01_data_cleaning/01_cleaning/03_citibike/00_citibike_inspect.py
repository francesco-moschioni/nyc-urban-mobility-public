"""
00_citibike_inspect.py
Stage: src/01_data_cleaning/01_cleaning/
Purpose: Inspect raw Citi Bike CSV files to map schema changes over time.
         Reads one file per month-year, reports columns, dtypes, and key stats.
         No data is written -- output is printed to stdout for manual review.
         Run before any cleaning script, and again after 01_citibike_unzip.py.
"""

import importlib.util
from pathlib import Path
import pandas as pd
import sys

# ── Config ────────────────────────────────────────────────────────────────────
# parents[0] = 01_cleaning/
# parents[1] = 01_data_cleaning/
# parents[2] = src/
# parents[3] = Tesi/
_cfg_path = Path(__file__).parents[4] / "00_config.py"
_spec = importlib.util.spec_from_file_location("config", _cfg_path)
_mod  = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
cfg = _mod.cfg

RAW_DIR = cfg.raw_citibike / "unzipped"

# ── Column aliases across schema generations ──────────────────────────────────
CANONICAL = {
    "trip_duration_sec":  ["tripduration", "Trip Duration"],
    "started_at":         ["starttime", "Start Time", "started_at"],
    "ended_at":           ["stoptime", "Stop Time", "ended_at"],
    "start_station_id":   ["start station id", "Start Station ID", "start_station_id"],
    "start_station_name": ["start station name", "Start Station Name", "start_station_name"],
    "start_lat":          ["start station latitude", "Start Station Latitude", "start_lat"],
    "start_lng":          ["start station longitude", "Start Station Longitude", "start_lng"],
    "end_station_id":     ["end station id", "End Station ID", "end_station_id"],
    "end_station_name":   ["end station name", "End Station Name", "end_station_name"],
    "end_lat":            ["end station latitude", "End Station Latitude", "end_lat"],
    "end_lng":            ["end station longitude", "End Station Longitude", "end_lng"],
    "member_casual":      ["usertype", "User Type", "member_casual"],
    "rideable_type":      ["rideable_type"],
    "bike_id":            ["bikeid", "Bike ID"],
}


def detect_schema(columns):
    cols_lower = {c.lower() for c in columns}
    if "tripduration" in cols_lower or "start station id" in cols_lower:
        return "old"
    if "started_at" in cols_lower or "start_lat" in cols_lower:
        return "new"
    return "unknown"


def map_columns(df):
    rename_map = {}
    for canonical, aliases in CANONICAL.items():
        for alias in aliases:
            if alias in df.columns:
                rename_map[alias] = canonical
                break
    return df.rename(columns=rename_map)


def inspect_file(path, n_rows=5000):
    try:
        df_raw = pd.read_csv(path, nrows=n_rows, low_memory=False)
    except Exception as e:
        return {"file": path.name, "error": str(e)}

    schema = detect_schema(df_raw.columns.tolist())
    df = map_columns(df_raw)

    member_col = "member_casual" if "member_casual" in df.columns else None
    member_counts = df[member_col].value_counts(dropna=False).to_dict() if member_col else "COLUMN NOT FOUND"

    date_range = None
    if "started_at" in df.columns:
        try:
            dt = pd.to_datetime(df["started_at"], errors="coerce")
            date_range = (str(dt.min()), str(dt.max()))
        except Exception:
            pass

    has_start_coords = ("start_lat" in df.columns and "start_lng" in df.columns)
    has_end_coords   = ("end_lat"   in df.columns and "end_lng"   in df.columns)

    rideable = None
    if "rideable_type" in df.columns:
        rideable = df["rideable_type"].value_counts(dropna=False).to_dict()

    return {
        "file":             path.name,
        "schema":           schema,
        "n_rows_sampled":   len(df_raw),
        "columns_raw":      df_raw.columns.tolist(),
        "date_range":       date_range,
        "member_counts":    member_counts,
        "has_start_coords": has_start_coords,
        "has_end_coords":   has_end_coords,
        "rideable_type":    rideable,
        "null_start_lat":   int(df["start_lat"].isna().sum()) if has_start_coords else None,
        "null_end_lat":     int(df["end_lat"].isna().sum())   if has_end_coords   else None,
    }


def print_summary(info):
    print(f"\n{'-'*70}")
    print(f"FILE:    {info['file']}")
    if "error" in info:
        print(f"  !! ERROR: {info['error']}")
        return
    print(f"SCHEMA:  {info['schema']}  |  rows sampled: {info['n_rows_sampled']}")
    print(f"DATES:   {info['date_range']}")
    print(f"COORDS:  start={info['has_start_coords']}  end={info['has_end_coords']}"
          f"  | null start_lat={info['null_start_lat']}  null end_lat={info['null_end_lat']}")
    print(f"MEMBER:  {info['member_counts']}")
    if info["rideable_type"]:
        print(f"RIDEABLE:{info['rideable_type']}")
    print(f"COLUMNS RAW:\n  {info['columns_raw']}")


def collect_files(root):
    """
    One representative CSV per month:
    - Recurse into year-subdirs (2013-citibike-tripdata ... 2023-citibike-tripdata)
    - Top-level 2024+ chunks: first chunk per month only
    - Exclude JC-* (Jersey City) and __MACOSX
    """
    files = []

    # Year subdirectories
    for entry in sorted(root.iterdir()):
        if not entry.is_dir():
            continue
        if entry.name.startswith("__") or entry.name.startswith("JC"):
            continue
        monthly = {}
        for f in sorted(entry.rglob("*.csv")):
            if f.name.startswith("JC") or "__MACOSX" in str(f):
                continue
            key = f.name[:6] if f.name[:6].isdigit() else f.name
            if key not in monthly:
                monthly[key] = f
        files.extend(sorted(monthly.values()))

    # Top-level CSVs (2024+): first chunk per month
    seen_months = set()
    for f in sorted(root.glob("*.csv")):
        if f.name.startswith("JC") or f.name.startswith("__"):
            continue
        month_key = f.name[:6]
        if month_key not in seen_months:
            seen_months.add(month_key)
            files.append(f)

    return sorted(files, key=lambda p: p.name)


def main():
    if not RAW_DIR.exists():
        print(f"ERROR: RAW_DIR not found: {RAW_DIR}")
        print(f"       (resolved from cfg.raw_citibike = {cfg.raw_citibike})")
        sys.exit(1)

    print(f"RAW_DIR: {RAW_DIR}")
    all_files = collect_files(RAW_DIR)
    if not all_files:
        print("No CSV files found.")
        sys.exit(1)

    print(f"Files to inspect (one per month, JC excluded): {len(all_files)}")
    for f in all_files:
        print(f"  {f.relative_to(RAW_DIR)}")

    print("\nStarting inspection...\n")
    results = []
    for i, f in enumerate(all_files):
        print(f"  [{i+1}/{len(all_files)}] {f.name} ...", end="\r")
        results.append(inspect_file(f))

    for r in results:
        print_summary(r)

    # Schema change summary
    print("\n\n" + "=" * 70)
    print("SCHEMA CHANGE SUMMARY")
    print("=" * 70)
    prev_schema = None
    for r in results:
        schema = r.get("schema", "error")
        marker = " << CHANGE" if schema != prev_schema and prev_schema is not None else ""
        print(f"  {r['file']:<55} {schema}{marker}")
        prev_schema = schema

    # Member column unique value sets
    print("\nMEMBER COLUMN -- unique value sets observed:")
    seen = set()
    for r in results:
        key = str(r.get("member_counts"))
        if key not in seen:
            print(f"  first seen in {r['file']}:")
            print(f"    {r.get('member_counts')}")
            seen.add(key)


if __name__ == "__main__":
    main()
