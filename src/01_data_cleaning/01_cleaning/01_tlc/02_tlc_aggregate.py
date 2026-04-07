"""
02_tlc_aggregate.py
-------------------
Pipeline stage : 01_data_cleaning / 01_cleaning / tlc
Input          : cfg.interim/tlc/{yellow,green,hvfhv,fhv}_clean.parquet
Output         : cfg.interim/tlc/{yellow,green,uber,lyft,via,juno,hvfhv_other,fhv}_zone_hour.parquet

What it does
------------
Aggregates trip-level clean parquet to:

    (PULocationID, DOLocationID, date, hour, dataset, hail_type)

hail_type classifies yellow/green trips by pricing/recruitment mechanism:
    "flex_fare"      payment_type == 0  (app e-hail, dynamic pricing — endogenous)
    "standard_meter" RatecodeID == 1, payment_type != 0  (metered, exogenous)
    "airport"        RatecodeID in {2, 3}, payment_type != 0  (JFK flat / Newark)
    "negotiated"     RatecodeID == 5, payment_type != 0  (pre-agreed fare)
    "other"          everything else (RatecodeID in {4, 6, 99, null}, etc.)

For HVFHV/FHV hail_type is always null (column present but null — schema uniform).

For HVFHV, splits by hvfhs_license_num:
    HV0002 → "juno"
    HV0003 → "uber"
    HV0004 → "via"
    HV0005 → "lyft"
    other  → "hvfhv_other"

Output columns
--------------
  hail_type           string    pricing/recruitment category (null for hvfhv/fhv)
  trip_count          int32
  avg_fare            float32   mean of main fare col
  avg_total           float32   mean of sum of all fare components
  avg_distance        float32   mean trip_distance
  avg_duration_min    float32   mean duration
  median_duration_min float32   median duration  (exact within each month)
  std_duration_min    float32   std duration     (exact within each month)
  p10_duration_min    float32   10th percentile  (exact within each month)
  p90_duration_min    float32   90th percentile  (exact within each month)
  avg_passengers      float32   mean passenger_count (NaN for hvfhv/fhv)

NOTE: share_flex_fare / share_ratecode_standard / share_street_hail / n_flex_fare /
n_street_hail columns are REMOVED — the information is now encoded in hail_type rows.
green-only: is_street_hail (bool) kept as a column since it is orthogonal to hail_type.

Memory strategy
---------------
  - yellow / green / fhv : month-by-month via PyArrow filter pushdown
  - hvfhv                : row-group split by provider → per-provider monthly shards

Resume support
--------------
  Existing monthly shards are skipped. Safe to re-run after a crash.
"""

import importlib.util
import time
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import numpy as np

# ── Config ────────────────────────────────────────────────────────────────────
_cfg_path = Path(__file__).parents[4] / "00_config.py"
_spec     = importlib.util.spec_from_file_location("config", _cfg_path)
_mod      = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
cfg = _mod.cfg

OUT_DIR = cfg.interim / "tlc"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ── HVFHV provider mapping ────────────────────────────────────────────────────
HVFHV_MAP = {
    "HV0002": "juno",
    "HV0003": "uber",
    "HV0004": "via",
    "HV0005": "lyft",
}

# ── hail_type categories (yellow / green only) ────────────────────────────────
# Priority: flex_fare (payment_type=0) overrides any RatecodeID because the
# meter is legally not running for those trips.
HAIL_TYPE_OTHER    = "other"
HAIL_TYPE_FLEX     = "flex_fare"
HAIL_TYPE_STANDARD = "standard_meter"
HAIL_TYPE_AIRPORT  = "airport"
HAIL_TYPE_NEGOT    = "negotiated"

AIRPORT_RATECODES  = {2, 3}   # JFK flat (2), Newark surcharge (3)


def assign_hail_type(df: pd.DataFrame) -> pd.Series:
    """
    Derive hail_type from payment_type and RatecodeID.
    Works on yellow and green dataframes — both columns must be present.
    Returns a pd.Series of strings with index aligned to df.
    """
    pt  = df["payment_type"].astype("float32")
    rc  = df["RatecodeID"].astype("float32")

    result = pd.Series(HAIL_TYPE_OTHER, index=df.index, dtype="object")

    # apply in reverse priority order so highest-priority overwrites
    # 4. negotiated
    result[rc == 5] = HAIL_TYPE_NEGOT
    # 3. airport (JFK flat / Newark)
    result[rc.isin(AIRPORT_RATECODES)] = HAIL_TYPE_AIRPORT
    # 2. standard metered
    result[rc == 1] = HAIL_TYPE_STANDARD
    # 1. flex fare — highest priority, overwrites everything
    result[pt == 0] = HAIL_TYPE_FLEX

    return result


# ── Fare columns per mode ─────────────────────────────────────────────────────
FARE_COLS = {
    "yellow": ["fare_amount", "extra", "mta_tax", "tip_amount",
               "tolls_amount", "improvement_surcharge",
               "congestion_surcharge", "airport_fee",
               "cbd_congestion_fee"],
    "green":  ["fare_amount", "extra", "mta_tax", "tip_amount",
               "tolls_amount", "improvement_surcharge",
               "congestion_surcharge",
               "cbd_congestion_fee"],
    "hvfhv":  ["base_passenger_fare", "tolls", "bcf", "sales_tax",
               "congestion_surcharge", "airport_fee", "tips", "driver_pay",
               "cbd_congestion_fee"],
    "fhv":    [],
}

# hail_type is part of the group key for yellow/green; null for hvfhv/fhv
GROUP_KEYS_HAIL = ["PULocationID", "DOLocationID", "date", "hour", "dataset", "hail_type"]
GROUP_KEYS_BASE = ["PULocationID", "DOLocationID", "date", "hour", "dataset"]

# ── Base fields (common to all modes) ────────────────────────────────────────
_BASE_FIELDS = [
    pa.field("PULocationID",          pa.int32()),
    pa.field("DOLocationID",          pa.int32()),
    pa.field("date",                  pa.date32()),
    pa.field("hour",                  pa.int8()),
    pa.field("dataset",               pa.string()),
    pa.field("hail_type",             pa.string()),   # null for hvfhv/fhv
    pa.field("trip_count",            pa.int32()),
    pa.field("avg_distance",          pa.float32()),
    pa.field("avg_duration_min",      pa.float32()),
    pa.field("median_duration_min",   pa.float32()),
    pa.field("std_duration_min",      pa.float32()),
    pa.field("p10_duration_min",      pa.float32()),
    pa.field("p90_duration_min",      pa.float32()),
    pa.field("avg_passengers",        pa.float32()),
]

# ── Per-mode fare fields ───────────────────────────────────────────────────────
_FARE_FIELDS = {
    "yellow": [
        pa.field("avg_fare_amount",             pa.float32()),
        pa.field("avg_extra",                   pa.float32()),
        pa.field("avg_mta_tax",                 pa.float32()),
        pa.field("avg_tip_amount",              pa.float32()),
        pa.field("avg_tolls_amount",            pa.float32()),
        pa.field("avg_improvement_surcharge",   pa.float32()),
        pa.field("avg_congestion_surcharge",    pa.float32()),
        pa.field("avg_airport_fee",             pa.float32()),
        pa.field("avg_cbd_congestion_fee",      pa.float32()),
        pa.field("avg_total",                   pa.float32()),
    ],
    "green": [
        pa.field("avg_fare_amount",             pa.float32()),
        pa.field("avg_extra",                   pa.float32()),
        pa.field("avg_mta_tax",                 pa.float32()),
        pa.field("avg_tip_amount",              pa.float32()),
        pa.field("avg_tolls_amount",            pa.float32()),
        pa.field("avg_improvement_surcharge",   pa.float32()),
        pa.field("avg_congestion_surcharge",    pa.float32()),
        pa.field("avg_cbd_congestion_fee",      pa.float32()),
        pa.field("avg_total",                   pa.float32()),
        pa.field("is_street_hail",              pa.bool_()),   # orthogonal to hail_type
    ],
    "hvfhv": [
        pa.field("avg_base_passenger_fare",     pa.float32()),
        pa.field("avg_tolls",                   pa.float32()),
        pa.field("avg_bcf",                     pa.float32()),
        pa.field("avg_sales_tax",               pa.float32()),
        pa.field("avg_congestion_surcharge",    pa.float32()),
        pa.field("avg_airport_fee",             pa.float32()),
        pa.field("avg_tips",                    pa.float32()),
        pa.field("avg_driver_pay",              pa.float32()),
        pa.field("avg_cbd_congestion_fee",      pa.float32()),
        pa.field("avg_total",                   pa.float32()),
    ],
    "fhv": [],
}


def get_schema(mode: str) -> pa.Schema:
    fare_mode = "hvfhv" if mode not in _FARE_FIELDS else mode
    return pa.schema(_BASE_FIELDS + _FARE_FIELDS[fare_mode])


def get_schema_for_label(label: str) -> pa.Schema:
    if label in ("uber", "lyft", "via", "juno", "hvfhv_other"):
        return get_schema("hvfhv")
    if label in _FARE_FIELDS:
        return get_schema(label)
    return get_schema("fhv")


# ── Core aggregation ──────────────────────────────────────────────────────────

def aggregate_df(df: pd.DataFrame, mode: str, dataset_label: str) -> pd.DataFrame:
    """
    Aggregate trip-level df to zone-hour panel.

    For yellow/green: groups by hail_type → one row per
        (PULocationID, DOLocationID, date, hour, dataset, hail_type)

    For hvfhv/fhv: hail_type set to None → one row per
        (PULocationID, DOLocationID, date, hour, dataset)
    """
    df = df.copy()
    df["date"]    = df["pickup_datetime"].dt.date
    df["hour"]    = df["pickup_datetime"].dt.hour.astype("int8")
    df["dataset"] = dataset_label

    # ── assign hail_type ────────────────────────────────────────────────────
    if mode in ("yellow", "green"):
        df["hail_type"] = assign_hail_type(df)
        group_keys = GROUP_KEYS_HAIL
    else:
        df["hail_type"] = None
        group_keys = GROUP_KEYS_BASE + ["hail_type"]   # include null col for schema

    # ── total fare ──────────────────────────────────────────────────────────
    fare_cols    = FARE_COLS[mode]
    fare_present = [c for c in fare_cols if c in df.columns]
    df["_total"] = df[fare_present].sum(axis=1) if fare_present else np.nan

    # ── green: is_street_hail (majority vote within cell) ───────────────────
    if mode == "green" and "trip_type" in df.columns:
        tt = df["trip_type"].astype("float32")
        df["_street"] = (tt == 1).astype("float32")

    grp = df.groupby(group_keys, sort=False, observed=True)
    dur = grp["duration_min"]

    agg = pd.DataFrame({
        "trip_count":          grp["trip_distance"].count(),
        "avg_distance":        grp["trip_distance"].mean(),
        "avg_duration_min":    dur.mean(),
        "median_duration_min": dur.median(),
        "std_duration_min":    dur.std(),
        "p10_duration_min":    dur.quantile(0.10),
        "p90_duration_min":    dur.quantile(0.90),
        "avg_passengers":      grp["passenger_count"].mean(),
        "avg_total":           grp["_total"].mean(),
    }).reset_index()

    # per-component fare means
    for fc in fare_present:
        agg[f"avg_{fc}"] = grp[fc].mean().values

    # green: is_street_hail — True if majority of trips in cell are street-hail
    if mode == "green" and "_street" in df.columns:
        street_mean = grp["_street"].mean().values
        agg["is_street_hail"] = street_mean >= 0.5

    # ── final casts ─────────────────────────────────────────────────────────
    agg["trip_count"] = agg["trip_count"].astype("int32")
    agg["hour"]       = agg["hour"].astype("int8")

    float_cols = [
        "avg_distance", "avg_duration_min", "median_duration_min",
        "std_duration_min", "p10_duration_min", "p90_duration_min",
        "avg_passengers", "avg_total",
    ] + [f"avg_{fc}" for fc in fare_present]
    for c in float_cols:
        if c in agg.columns:
            agg[c] = agg[c].astype("float32")

    return agg


def df_to_table(df: pd.DataFrame, schema: pa.Schema) -> pa.Table:
    for field in schema:
        if field.name not in df.columns:
            df[field.name] = np.nan
    return pa.Table.from_pandas(df[schema.names], schema=schema,
                                preserve_index=False)


# ── Generic month-by-month pipeline ──────────────────────────────────────────

def aggregate_by_month(mode: str, dataset_label: str, in_path: Path, out_path: Path) -> None:
    """
    Aggregate clean parquet to zone-hour panel, one month at a time.
    Uses PyArrow filter pushdown — peak RAM ≈ one month of data.
    Stats (median, std, percentiles) are exact within each month.
    """
    shard_dir = OUT_DIR / "agg_shards" / dataset_label
    shard_dir.mkdir(parents=True, exist_ok=True)
    existing  = {p.stem for p in shard_dir.glob("*_shard.parquet")}

    pf = pq.ParquetFile(in_path)
    print(f"  {pf.metadata.num_rows:,} rows  |  {pf.metadata.num_row_groups} row groups")
    print(f"  Scanning months ...", flush=True)

    months: set = set()
    for rg_idx in range(pf.metadata.num_row_groups):
        col = (pf.read_row_group(rg_idx, columns=["pickup_datetime"])
                 .column("pickup_datetime").to_pandas())
        col = pd.to_datetime(col, errors="coerce").dropna()
        for ym in zip(col.dt.year.astype(int), col.dt.month.astype(int)):
            months.add(ym)
    months = sorted(months)
    print(f"  Found {len(months)} months: {months[0]} to {months[-1]}\n")

    _t0    = time.time()
    schema = get_schema_for_label(dataset_label)

    for i, (year, month) in enumerate(months, 1):
        stem       = f"{year:04d}_{month:02d}_shard"
        shard_path = shard_dir / f"{stem}.parquet"

        if stem in existing:
            print(f"  [{i:>3}/{len(months)}] {year}-{month:02d}  [SKIP — shard exists]")
            continue

        ts_lo = pd.Timestamp(year=year, month=month, day=1)
        ts_hi = ts_lo + pd.offsets.MonthEnd(1) + pd.Timedelta(days=1)

        tbl = pq.read_table(in_path, filters=[
            ("pickup_datetime", ">=", ts_lo),
            ("pickup_datetime", "<",  ts_hi),
        ])
        df = tbl.to_pandas()
        del tbl

        df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"], errors="coerce")

        t_read = time.time() - _t0
        print(f"  [{i:>3}/{len(months)}] {year}-{month:02d}  "
              f"{len(df):>10,} rows  read in {t_read:>5.0f}s", end="  ", flush=True)

        _t_agg = time.time()
        agg    = aggregate_df(df, mode, dataset_label)
        del df
        t_agg = time.time() - _t_agg

        pq.write_table(df_to_table(agg, schema), shard_path, compression="snappy")
        _t0 = time.time()
        print(f"agg {t_agg:>5.0f}s  →  {len(agg):,} zone-hour rows")

    # merge shards
    shards = sorted(shard_dir.glob("*_shard.parquet"))
    if not shards:
        print(f"  [WARN] no shards produced — output not written")
        return

    print(f"  Writing {len(shards)} shards → {out_path.name} ...")
    writer     = pq.ParquetWriter(out_path, schema=schema, compression="snappy")
    total_rows = 0
    for i, sp in enumerate(shards, 1):
        tbl = pq.read_table(sp, schema=schema)
        writer.write_table(tbl)
        total_rows += len(tbl)
        print(f"    shard {i}/{len(shards)}: {sp.name} ({len(tbl):,} rows)", flush=True)
        del tbl
    writer.close()
    print(f"  ✓ {out_path.name}: {total_rows:,} rows")

    for sp in shards:
        sp.unlink()
    try:
        shard_dir.rmdir()
    except OSError:
        pass
    print("  Shards deleted.")


def aggregate_small(mode: str) -> None:
    in_path  = OUT_DIR / f"{mode}_clean.parquet"
    out_path = OUT_DIR / f"{mode}_zone_hour.parquet"

    if not in_path.exists():
        print(f"  [SKIP] {in_path.name} not found")
        return

    print(f"\n{'='*60}\n  MODE: {mode.upper()}\n{'='*60}")
    aggregate_by_month(mode, mode, in_path, out_path)


# ── HVFHV pipeline ────────────────────────────────────────────────────────────

def aggregate_hvfhv() -> None:
    in_path = OUT_DIR / "hvfhv_clean.parquet"

    if not in_path.exists():
        print(f"  [SKIP] hvfhv_clean.parquet not found")
        return

    print(f"\n{'='*60}\n  MODE: HVFHV (split by provider)\n{'='*60}")

    pf   = pq.ParquetFile(in_path)
    n_rg = pf.metadata.num_row_groups
    print(f"  {pf.metadata.num_rows:,} rows  |  {n_rg} row groups")

    split_dir = OUT_DIR / "agg_shards" / "_hvfhv_split"
    split_dir.mkdir(parents=True, exist_ok=True)
    existing_splits = {p.stem for p in split_dir.glob("*.parquet")}

    providers_seen: set = set()

    _t0_hvfhv = time.time()
    for rg_idx in range(n_rg):
        stem = f"rg{rg_idx:05d}"
        if stem in existing_splits:
            t = pq.read_table(split_dir / f"{stem}.parquet", columns=["dataset"])
            for p in t.column("dataset").to_pylist():
                if p:
                    providers_seen.add(p)
            continue

        tbl = pf.read_row_group(rg_idx)
        df  = tbl.to_pandas()
        del tbl

        df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"], errors="coerce")
        df["dataset"] = df["hvfhs_license_num"].map(HVFHV_MAP).fillna("hvfhv_other")
        providers_seen.update(df["dataset"].unique())

        t_out = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(t_out, split_dir / f"{stem}.parquet", compression="snappy")
        del df, t_out

        if (rg_idx + 1) % 10 == 0 or rg_idx == n_rg - 1:
            elapsed_h = time.time() - _t0_hvfhv
            rate_h    = (rg_idx + 1) / elapsed_h if elapsed_h > 0 else 0
            eta_h     = (n_rg - rg_idx - 1) / rate_h if rate_h > 0 else 0
            print(f"  split rg {rg_idx+1:>5}/{n_rg}  "
                  f"| {elapsed_h:>6.0f}s  ETA {eta_h:>5.0f}s  "
                  f"| providers: {sorted(providers_seen)}", flush=True)

    for provider in sorted(providers_seen):
        out_path = OUT_DIR / f"{provider}_zone_hour.parquet"
        if out_path.exists():
            pf2 = pq.ParquetFile(out_path)
            print(f"  [SKIP] {provider}_zone_hour.parquet ({pf2.metadata.num_rows:,} rows)")
            continue

        print(f"\n  Aggregating provider: {provider} ...")
        shard_dir = OUT_DIR / "agg_shards" / provider
        shard_dir.mkdir(parents=True, exist_ok=True)
        existing_shards = {p.stem for p in shard_dir.glob("*_shard.parquet")}

        month_buf: dict[tuple, list] = {}
        seen_months: set = set()

        def flush_month_p(ym: tuple) -> None:
            if ym not in month_buf or not month_buf[ym]:
                return
            stem = f"{ym[0]:04d}_{ym[1]:02d}_shard"
            if stem in existing_shards:
                month_buf.pop(ym)
                return
            df_month = pd.concat(month_buf.pop(ym), ignore_index=True)
            print(f"      → flushing {provider} {ym[0]}-{ym[1]:02d}  "
                  f"({len(df_month):,} rows) ...", end=" ", flush=True)
            agg    = aggregate_df(df_month, "hvfhv", provider)
            del df_month
            schema = get_schema_for_label(provider)
            pq.write_table(df_to_table(agg, schema),
                           shard_dir / f"{stem}.parquet", compression="snappy")
            print(f"{len(agg):,} zone-hour rows")

        split_files = sorted(split_dir.glob("rg*.parquet"))
        for sp in split_files:
            tbl = pq.read_table(sp, filters=[("dataset", "=", provider)])
            if len(tbl) == 0:
                continue
            df = tbl.to_pandas()
            del tbl

            df["_ym"] = list(zip(
                df["pickup_datetime"].dt.year.fillna(0).astype(int),
                df["pickup_datetime"].dt.month.fillna(0).astype(int),
            ))
            for ym, grp in df.groupby("_ym", sort=False):
                if ym == (0, 0):
                    continue
                if ym not in month_buf:
                    month_buf[ym] = []
                    for old_ym in list(seen_months):
                        if old_ym != ym and old_ym in month_buf:
                            flush_month_p(old_ym)
                month_buf[ym].append(grp.drop(columns=["_ym"]))
                seen_months.add(ym)
            del df

        for ym in list(month_buf.keys()):
            flush_month_p(ym)

        shards = sorted(shard_dir.glob("*_shard.parquet"))
        if not shards:
            print(f"  [WARN] no shards for {provider}")
            continue

        prov_schema = get_schema_for_label(provider)
        print(f"  Writing {len(shards)} shards → {out_path.name} ...")
        writer     = pq.ParquetWriter(out_path, schema=prov_schema, compression="snappy")
        total_rows = 0
        for i, sp in enumerate(shards, 1):
            tbl = pq.read_table(sp, schema=prov_schema)
            writer.write_table(tbl)
            total_rows += len(tbl)
            print(f"    shard {i}/{len(shards)}: {sp.name} ({len(tbl):,} rows)", flush=True)
            del tbl
        writer.close()
        print(f"  ✓ {out_path.name}: {total_rows:,} rows")

        for sp in shards:
            sp.unlink()
        try:
            shard_dir.rmdir()
        except OSError:
            pass

    for sp in split_dir.glob("*.parquet"):
        sp.unlink()
    try:
        split_dir.rmdir()
    except OSError:
        pass
    print("  Split shards deleted.")


# ── Obsolete file detection ───────────────────────────────────────────────────

# A file is obsolete if it was produced before hail_type was added to the schema.
_REQUIRED_COLS = {
    "yellow":      "hail_type",
    "green":       "hail_type",
    "uber":        "avg_cbd_congestion_fee",
    "lyft":        "avg_cbd_congestion_fee",
    "via":         "avg_cbd_congestion_fee",
    "juno":        "avg_cbd_congestion_fee",
    "hvfhv_other": "avg_cbd_congestion_fee",
    "fhv":         None,
}


def is_obsolete(label: str, path: Path) -> bool:
    if not path.exists():
        return False
    required = _REQUIRED_COLS.get(label)
    if required is None:
        return False
    return required not in pq.read_schema(path).names


def purge_obsolete(labels: list[str]) -> None:
    print("\n── Checking for obsolete output files ──────────────────────────")
    any_purged = False
    for label in labels:
        p = OUT_DIR / f"{label}_zone_hour.parquet"
        if is_obsolete(label, p):
            print(f"  [OBSOLETE] {p.name} — missing required col → deleting")
            p.unlink()
            any_purged = True
        elif p.exists():
            print(f"  [OK]       {p.name}")
    if not any_purged:
        print("  All existing files are current.")
    print()


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    print("\nTLC Aggregation Pipeline — 02_tlc_aggregate.py")
    print(f"Output directory: {OUT_DIR}\n")

    all_labels = ["yellow", "green", "fhv", "uber", "lyft", "via", "juno", "hvfhv_other"]

    purge_obsolete(all_labels)

    for mode in ["yellow", "green", "fhv"]:
        out_path = OUT_DIR / f"{mode}_zone_hour.parquet"
        if out_path.exists():
            pf = pq.ParquetFile(out_path)
            print(f"[ALREADY DONE] {mode}_zone_hour.parquet "
                  f"({pf.metadata.num_rows:,} rows) — skipping")
            continue
        aggregate_small(mode)

    providers      = ["uber", "lyft", "via", "juno", "hvfhv_other"]
    shard_dir      = OUT_DIR / "agg_shards" / "hvfhv"
    any_missing    = any(
        not (OUT_DIR / f"{p}_zone_hour.parquet").exists() for p in providers
    )
    shards_present = shard_dir.exists() and any(shard_dir.glob("*_shard.parquet"))

    if not any_missing and not shards_present:
        for p in providers:
            pf = pq.ParquetFile(OUT_DIR / f"{p}_zone_hour.parquet")
            print(f"[ALREADY DONE] {p}_zone_hour.parquet "
                  f"({pf.metadata.num_rows:,} rows) — skipping")
    else:
        aggregate_hvfhv()

    print("\n\nAll modes complete.")
    print("Output files:")
    for mode in all_labels:
        p = OUT_DIR / f"{mode}_zone_hour.parquet"
        if p.exists():
            pf = pq.ParquetFile(p)
            print(f"  {p.name:45s}  {pf.metadata.num_rows:>12,} rows")


if __name__ == "__main__":
    main()