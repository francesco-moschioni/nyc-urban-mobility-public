"""
01_tlc_clean.py
01_clean.py
---------------
Pipeline stage : 01_data_cleaning / 01_cleaning
Input          : cfg.raw_tlc/{yellow, green, fhvhv, fhv}  — monthly parquet files
Output         : cfg.interim/tlc/{yellow,green,hvfhv,fhv}_clean.parquet
                 cfg.interim/tlc/shards/{mode}/  — temporary shards (auto-deleted)

What it does
------------
For each of the four TLC datasets:
  1. Discovers all monthly parquet files in the raw folder (2021-01 → 2025-12).
  2. Reads each file, renames columns to a canonical schema, computes duration_min,
     applies quality filters, casts types, writes a shard parquet.
  3. Merges all shards into a single clean parquet via ParquetWriter (one shard at
     a time — never loads everything into RAM).
  4. Deletes shards after a successful merge.

Resume support: if a shard already exists it is skipped, so the script is safe
to re-run after a crash.

Canonical schema (all modes)
-----------------------------
  pickup_datetime   timestamp[us]   renamed from mode-specific col
  dropoff_datetime  timestamp[us]
  PULocationID      int32
  DOLocationID      int32
  trip_distance     float32         miles
  duration_min      float32         (dropoff - pickup) in minutes
  passenger_count   int8            NULL for hvfhv / fhv (not recorded)
  dataset           string          "yellow" | "green" | "hvfhv" | "fhv"

Fare columns (mode-specific, appended after canonical cols)
------------------------------------------------------------
  yellow / green:
    fare_amount, extra, mta_tax, tip_amount, tolls_amount,
    improvement_surcharge, congestion_surcharge, airport_fee  (float32 each)

  hvfhv:
    base_passenger_fare, tolls, bcf, sales_tax,
    congestion_surcharge, airport_fee, tips, driver_pay       (float32 each)

  fhv:
    (no fare columns available in source data)

Quality filters applied
------------------------
  - pickup_datetime in [2021-01-01, 2026-01-01)
  - trip_distance > 0
  - For yellow/green/hvfhv: main fare column > 0
    (fare_amount for yellow/green, base_passenger_fare for hvfhv)
  - No zone-ID filter: zones 0 and 264 are kept (filtered at estimation stage)
"""


import importlib.util
import sys
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
import pandas as pd

# ── Config ────────────────────────────────────────────────────────────────────
_cfg_path = Path(__file__).parents[4] / "00_config.py"
_spec     = importlib.util.spec_from_file_location("config", _cfg_path)
_mod      = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
cfg = _mod.cfg

# ── Date range ────────────────────────────────────────────────────────────────
DATE_MIN = pd.Timestamp("2021-01-01")
DATE_MAX = pd.Timestamp("2026-01-01")   # exclusive upper bound

# ── Output directory ──────────────────────────────────────────────────────────
OUT_DIR = cfg.interim / "tlc"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ── Per-mode configuration ────────────────────────────────────────────────────
#
# Each entry defines:
#   raw_dir        : Path — folder with monthly parquet files
#   pickup_col     : str  — source column for pickup datetime
#   dropoff_col    : str  — source column for dropoff datetime
#   pu_col         : str  — source column for PULocationID
#   do_col         : str  — source column for DOLocationID
#   distance_col   : str or None
#   pax_col        : str or None — None → passenger_count filled with NULL
#   fare_filter_col: str or None — column used for fare > 0 filter; None = skip
#   fare_cols      : list[str]   — all fare-related cols to keep (float32)

MODE_CFG = {
    "yellow": dict(
        raw_dir        = cfg.raw_tlc / "yellow",
        pickup_col     = "tpep_pickup_datetime",
        dropoff_col    = "tpep_dropoff_datetime",
        pu_col         = "PULocationID",
        do_col         = "DOLocationID",
        distance_col   = "trip_distance",
        pax_col        = "passenger_count",
        fare_filter_col= "fare_amount",
        extra_cols     = ["payment_type", "RatecodeID"],  # tariff type flags
        fare_cols      = [
            "fare_amount", "extra", "mta_tax", "tip_amount",
            "tolls_amount", "improvement_surcharge",
            "congestion_surcharge", "airport_fee",
            "cbd_congestion_fee",           # Congestion Pricing MTA — from 2025-01
        ],
    ),
    "green": dict(
        raw_dir        = cfg.raw_tlc / "green",
        pickup_col     = "lpep_pickup_datetime",
        dropoff_col    = "lpep_dropoff_datetime",
        pu_col         = "PULocationID",
        do_col         = "DOLocationID",
        distance_col   = "trip_distance",
        pax_col        = "passenger_count",
        fare_filter_col= "fare_amount",
        extra_cols     = ["payment_type", "RatecodeID", "trip_type"],  # trip_type: 1=street-hail, 2=dispatch
        fare_cols      = [
            "fare_amount", "extra", "mta_tax", "tip_amount",
            "tolls_amount", "improvement_surcharge",
            "congestion_surcharge",
            "cbd_congestion_fee",           # Congestion Pricing MTA — from 2025-01
            # airport_fee absent in green — handled gracefully below
        ],
    ),
    "hvfhv": dict(
        raw_dir        = cfg.raw_tlc / "fhvhv",
        pickup_col     = "pickup_datetime",
        dropoff_col    = "dropoff_datetime",
        pu_col         = "PULocationID",
        do_col         = "DOLocationID",
        distance_col   = "trip_miles",
        pax_col        = None,
        fare_filter_col= "base_passenger_fare",
        extra_cols     = ["hvfhs_license_num"],   # used to split Uber/Lyft/Via/Juno
        fare_cols      = [
            "base_passenger_fare", "tolls", "bcf", "sales_tax",
            "congestion_surcharge", "airport_fee", "tips", "driver_pay",
            "cbd_congestion_fee",           # Congestion Pricing MTA — from 2025-01
        ],
    ),
    "fhv": dict(
        raw_dir        = cfg.raw_tlc / "fhv",
        pickup_col     = "pickup_datetime",
        dropoff_col    = "dropoff_datetime",
        pu_col         = "PULocationID",
        do_col         = "DOLocationID",
        distance_col   = None,      # FHV does not record distance
        pax_col        = None,
        fare_filter_col= None,      # FHV has no fare columns
        fare_cols      = [],
    ),
}

# ── Canonical base schema (no fare cols) ─────────────────────────────────────
BASE_FIELDS = [
    pa.field("pickup_datetime",    pa.timestamp("us")),
    pa.field("dropoff_datetime",   pa.timestamp("us")),
    pa.field("PULocationID",       pa.int32()),
    pa.field("DOLocationID",       pa.int32()),
    pa.field("trip_distance",      pa.float32()),
    pa.field("duration_min",       pa.float32()),
    pa.field("passenger_count",    pa.int8()),
    pa.field("dataset",            pa.string()),
    pa.field("hvfhs_license_num",  pa.string()),  # None for non-HVFHV modes
    pa.field("payment_type",       pa.int8()),    # 0=Flex Fare; None for hvfhv/fhv
    pa.field("RatecodeID",         pa.int8()),    # 1=standard meter; None for hvfhv/fhv
    pa.field("trip_type",          pa.int8()),    # 1=street-hail, 2=dispatch (green only)
]

def build_schema(fare_cols: list[str]) -> pa.Schema:
    """Canonical schema + float32 fare columns for this mode."""
    fare_fields = [pa.field(c, pa.float32()) for c in fare_cols]
    return pa.schema(BASE_FIELDS + fare_fields)

# hvfhs_license_num is always the last base field — present for all modes,
# null for yellow/green/fhv, populated for hvfhv.


# ── Single-file processing ────────────────────────────────────────────────────

def process_file(path: Path, mode: str, mcfg: dict) -> pa.Table | None:
    """
    Read one monthly parquet, apply filters, return a PyArrow Table
    in canonical schema.  Returns None if the file yields 0 rows.
    """
    # --- read only needed columns -------------------------------------------
    needed = [
        mcfg["pickup_col"], mcfg["dropoff_col"],
        mcfg["pu_col"], mcfg["do_col"],
    ]
    if mcfg["distance_col"]:
        needed.append(mcfg["distance_col"])
    if mcfg["pax_col"]:
        needed.append(mcfg["pax_col"])
    for fc in mcfg.get("extra_cols", []):
        needed.append(fc)
    for fc in mcfg["fare_cols"]:
        needed.append(fc)

    # read only columns that actually exist in this file
    pf       = pq.ParquetFile(path)
    existing = set(pf.schema_arrow.names)
    needed   = [c for c in needed if c in existing]

    try:
        tbl = pq.read_table(path, columns=needed)
    except Exception as e:
        print(f"    [WARN] could not read {path.name}: {e}")
        return None

    if len(tbl) == 0:
        return None

    # --- rename columns in PyArrow ------------------------------------------
    rename_map = {
        mcfg["pickup_col"]:  "pickup_datetime",
        mcfg["dropoff_col"]: "dropoff_datetime",
        mcfg["pu_col"]:      "PULocationID",
        mcfg["do_col"]:      "DOLocationID",
        # yellow 2024+ changed capitalisation airport_fee → Airport_fee
        "Airport_fee":       "airport_fee",
    }
    if mcfg["distance_col"] and mcfg["distance_col"] != "trip_distance":
        rename_map[mcfg["distance_col"]] = "trip_distance"
    tbl = tbl.rename_columns([rename_map.get(n, n) for n in tbl.schema.names])

    # --- guard: skip files missing datetime cols (old schema) ---------------
    for col in ("pickup_datetime", "dropoff_datetime"):
        if col not in tbl.schema.names:
            print(f"missing column '{col}' (old schema) — skipped")
            return None

    # ── ALL FILTERS IN PYARROW (no pandas allocation yet) ───────────────────
    import pyarrow.compute as pc

    pu  = tbl.column("pickup_datetime")
    do  = tbl.column("dropoff_datetime")

    # 1. valid datetimes
    filt = pc.and_(pc.is_valid(pu), pc.is_valid(do))

    # 2. date range
    filt = pc.and_(filt, pc.greater_equal(pu, pa.scalar(DATE_MIN.to_pydatetime())))
    filt = pc.and_(filt, pc.less(pu, pa.scalar(DATE_MAX.to_pydatetime())))

    # 3. duration > 0  (dropoff > pickup)
    filt = pc.and_(filt, pc.greater(do, pu))

    # 4. trip_distance > 0
    if mcfg["distance_col"] and "trip_distance" in tbl.schema.names:
        dist = tbl.column("trip_distance")
        filt = pc.and_(filt, pc.and_(pc.is_valid(dist), pc.greater(dist, pa.scalar(0.0))))

    # 5. fare > 0
    if mcfg["fare_filter_col"] and mcfg["fare_filter_col"] in tbl.schema.names:
        fare = tbl.column(mcfg["fare_filter_col"])
        filt = pc.and_(filt, pc.and_(pc.is_valid(fare), pc.greater(fare, pa.scalar(0.0))))

    tbl = tbl.filter(filt)
    if len(tbl) == 0:
        return None

    # ── CAST FARE COLS TO FLOAT32 IN PYARROW (before to_pandas) ─────────────
    # Avoids per-column copy allocations in pandas on 20M+ row files.
    cast_map = {}
    for fc in mcfg["fare_cols"]:
        if fc in tbl.schema.names:
            cast_map[fc] = pa.float32()
        else:
            # add missing fare col as null array — zero allocation
            tbl = tbl.append_column(fc, pa.array([None] * len(tbl), type=pa.float32()))
    # cast existing fare cols
    new_cols = {}
    for fc, dtype in cast_map.items():
        idx = tbl.schema.get_field_index(fc)
        new_cols[fc] = tbl.column(idx).cast(dtype, safe=False)
    for fc, arr in new_cols.items():
        idx = tbl.schema.get_field_index(fc)
        tbl = tbl.set_column(idx, fc, arr)

    # cast zone IDs and distance to correct types
    for col, dtype in [("PULocationID", pa.int32()), ("DOLocationID", pa.int32()),
                       ("trip_distance", pa.float32())]:
        if col in tbl.schema.names:
            idx = tbl.schema.get_field_index(col)
            tbl = tbl.set_column(idx, col, tbl.column(col).cast(dtype, safe=False))
        elif col == "trip_distance":
            tbl = tbl.append_column(col, pa.array([None] * len(tbl), type=pa.float32()))

    # ── CONVERT TO PANDAS — already typed, minimal extra allocation ──────────
    df = tbl.to_pandas()
    del tbl

    # --- duration_min -------------------------------------------------------
    df["pickup_datetime"]  = pd.to_datetime(df["pickup_datetime"],  errors="coerce", utc=False)
    df["dropoff_datetime"] = pd.to_datetime(df["dropoff_datetime"], errors="coerce", utc=False)
    df["duration_min"] = (
        (df["dropoff_datetime"] - df["pickup_datetime"])
        .dt.total_seconds()
        .div(60)
        .astype("float32")
    )

    # --- passenger_count ----------------------------------------------------
    if mcfg["pax_col"] and mcfg["pax_col"] in df.columns:
        pax = pd.to_numeric(df["passenger_count"], errors="coerce")
        pax = pax.where((pax >= 1) & (pax <= 9), other=pd.NA)
        df["passenger_count"] = pax.astype("Int8")
    else:
        df["passenger_count"] = pd.array([pd.NA] * len(df), dtype="Int8")

    # --- dataset label ------------------------------------------------------
    df["dataset"] = mode

    # --- hvfhs_license_num: keep for hvfhv, fill None for others ------------
    if "hvfhs_license_num" not in df.columns:
        df["hvfhs_license_num"] = None

    # --- tariff type cols: keep where present, fill None elsewhere ----------
    # payment_type: 0 = Flex Fare (potentially endogenous), others = metered
    # RatecodeID:   1 = standard meter, 2 = JFK flat, 3 = Newark, 5 = negotiated
    # trip_type:    1 = street-hail, 2 = dispatch (green only)
    for tariff_col in ("payment_type", "RatecodeID", "trip_type"):
        if tariff_col in df.columns:
            vals = pd.to_numeric(df[tariff_col], errors="coerce")
            df[tariff_col] = vals.where(vals.notna(), other=pd.NA).astype("Int8")
        else:
            df[tariff_col] = pd.array([pd.NA] * len(df), dtype="Int8")

    # --- select and order final columns -------------------------------------
    final_cols = [
        "pickup_datetime", "dropoff_datetime",
        "PULocationID", "DOLocationID",
        "trip_distance", "duration_min", "passenger_count",
        "dataset", "hvfhs_license_num",
        "payment_type", "RatecodeID", "trip_type",
    ] + mcfg["fare_cols"]
    df = df[[c for c in final_cols if c in df.columns]]

    # --- convert to PyArrow with canonical schema ---------------------------
    schema = build_schema(mcfg["fare_cols"])
    try:
        out = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    except Exception as e:
        print(f"    [WARN] schema cast failed for {path.name}: {e}")
        return None

    return out


# ── Shard → merge pipeline ────────────────────────────────────────────────────

def clean_mode(mode: str, mcfg: dict) -> None:
    raw_dir    = mcfg["raw_dir"]
    shard_dir  = OUT_DIR / "shards" / mode
    out_path   = OUT_DIR / f"{mode}_clean.parquet"
    schema     = build_schema(mcfg["fare_cols"])

    shard_dir.mkdir(parents=True, exist_ok=True)

    # discover raw files — skip anything whose filename year < 2021
    # e.g. "yellow_tripdata_2019-06.parquet" is skipped immediately
    # without opening the file.
    import re
    all_files = sorted(raw_dir.glob("*.parquet"))
    files = []
    skipped_old = 0
    for f in all_files:
        m = re.search(r"(\d{4})", f.stem)
        if m and int(m.group(1)) < 2021:
            skipped_old += 1
        else:
            files.append(f)

    if not all_files:
        print(f"  [SKIP] no parquet files found in {raw_dir}")
        return

    print(f"\n{'='*60}")
    print(f"  MODE: {mode.upper()}  |  {len(all_files)} raw files found, "
          f"{skipped_old} skipped (pre-2021), {len(files)} to process")
    print(f"{'='*60}")

    # ── STEP 1: write shards ─────────────────────────────────────────────────
    total_rows_written = 0
    total_rows_dropped = 0

    for fpath in files:
        shard_path = shard_dir / (fpath.stem + "_shard.parquet")
        if shard_path.exists():
            print(f"  [SKIP] shard exists: {shard_path.name}")
            continue

        print(f"  Processing {fpath.name} ...", end=" ", flush=True)
        tbl = process_file(fpath, mode, mcfg)

        if tbl is None or len(tbl) == 0:
            print("0 rows after filter — skipped")
            continue

        pq.write_table(tbl, shard_path, compression="snappy")
        print(f"{len(tbl):,} rows → {shard_path.name}")
        total_rows_written += len(tbl)
        del tbl

    # ── STEP 2: merge shards ─────────────────────────────────────────────────
    shards = sorted(shard_dir.glob("*_shard.parquet"))
    if not shards:
        print(f"  [WARN] no shards produced for {mode} — output not written")
        return

    print(f"\n  Merging {len(shards)} shards → {out_path.name} ...")
    writer     = pq.ParquetWriter(out_path, schema=schema, compression="snappy")
    total_rows = 0

    for sp in shards:
        t = pq.read_table(sp, schema=schema)
        writer.write_table(t)
        total_rows += len(t)
        del t

    writer.close()
    print(f"  ✓ {mode}_clean.parquet written: {total_rows:,} rows")

    # ── STEP 3: delete shards ────────────────────────────────────────────────
    for sp in shards:
        sp.unlink()
    shard_dir.rmdir()
    print(f"  Shards deleted.")


# ── Obsolete file detection ───────────────────────────────────────────────────
# Sentinel columns: if missing from an existing clean parquet, the file was
# produced by an older version of this script and must be re-generated.
_CLEAN_SENTINEL = {
    "yellow": "cbd_congestion_fee",  # added 2025-01 + disaggregated fare schema
    "green":  "cbd_congestion_fee",
    "hvfhv":  "cbd_congestion_fee",
    "fhv":    None,                  # fhv has no fare cols — never obsolete
}

def purge_obsolete_clean() -> None:
    """Delete clean parquet files that are missing sentinel columns."""
    print("\n── Checking for obsolete clean files ───────────────────────────────")
    any_purged = False
    for mode, sentinel in _CLEAN_SENTINEL.items():
        p = OUT_DIR / f"{mode}_clean.parquet"
        if not p.exists():
            continue
        if sentinel is None:
            print(f"  [OK]       {p.name}")
            continue
        schema_names = pq.read_schema(p).names
        if sentinel not in schema_names:
            print(f"  [OBSOLETE] {p.name} — missing '{sentinel}' → deleting")
            p.unlink()
            # also purge any leftover shards for this mode
            shard_dir = OUT_DIR / "shards" / mode
            if shard_dir.exists():
                for sp in shard_dir.glob("*_shard.parquet"):
                    sp.unlink()
                try:
                    shard_dir.rmdir()
                except OSError:
                    pass
                print(f"             Stale shards in {shard_dir} also deleted.")
            any_purged = True
        else:
            print(f"  [OK]       {p.name}")
    if not any_purged:
        print("  All existing clean files are current.")
    print()


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    print("\nTLC Cleaning Pipeline — 01_tlc_clean.py")
    print(f"Output directory: {OUT_DIR}\n")

    purge_obsolete_clean()

    for mode, mcfg in MODE_CFG.items():
        out_path = OUT_DIR / f"{mode}_clean.parquet"
        if out_path.exists():
            pf  = pq.ParquetFile(out_path)
            row = pf.metadata.num_rows
            print(f"[ALREADY DONE] {mode}_clean.parquet  ({row:,} rows) — skipping")
            continue
        clean_mode(mode, mcfg)

    print("\n\nAll modes complete.")
    print("Output files:")
    for mode in MODE_CFG:
        p = OUT_DIR / f"{mode}_clean.parquet"
        if p.exists():
            pf = pq.ParquetFile(p)
            print(f"  {p.name:35s}  {pf.metadata.num_rows:>15,} rows")


if __name__ == "__main__":
    main()
