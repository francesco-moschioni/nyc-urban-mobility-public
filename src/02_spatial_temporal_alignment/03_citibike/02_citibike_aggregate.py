"""
03_citibike_aggregate.py
Stage: src/02_spatial_temporal_alignment/

Input:
    cfg.interim / citibike / citibike_tlc.parquet

Output:
    cfg.interim / citibike / citibike_agg.parquet

Aggregation key:
    (start_tlc_zone, end_tlc_zone, date, hour, member_casual, rideable_type)

Memory strategy:
    STEP 1 — Each row group aggregated independently -> shard parquet.
             Never accumulate raw trips in RAM.
    STEP 2 — All shards concatenated via ParquetWriter (one shard at a time).
    STEP 3 — Final groupby on the merged file (aggregated rows << 221M trips,
             safe to load).
    Resume support: existing shards are skipped.
"""

import importlib.util
from pathlib import Path
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

# ── Config ────────────────────────────────────────────────────────────────────
_cfg_path = Path(__file__).parents[3] / "00_config.py"
_spec = importlib.util.spec_from_file_location("config", _cfg_path)
_mod  = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
cfg = _mod.cfg

IN_PATH   = cfg.interim / "citibike" / "citibike_tlc.parquet"
OUT_PATH  = cfg.interim / "citibike" / "citibike_agg.parquet"
SHARD_DIR = cfg.interim / "citibike" / "_agg_shards"

YEAR_MIN    = 2021
DUR_MAX_MIN = 300

SEP = "=" * 60

KEY_COLS = ["start_tlc_zone", "end_tlc_zone", "date", "hour",
            "member_casual", "rideable_type"]

OUT_SCHEMA = pa.schema([
    pa.field("start_tlc_zone",      pa.int32()),
    pa.field("end_tlc_zone",        pa.int32()),
    pa.field("date",                pa.date32()),
    pa.field("hour",                pa.int8()),
    pa.field("member_casual",       pa.string()),
    pa.field("rideable_type",       pa.string()),
    pa.field("n_trips",             pa.int32()),
    pa.field("avg_distance_km",     pa.float32()),
    pa.field("mean_duration_min",   pa.float32()),
    pa.field("median_duration_min", pa.float32()),
    pa.field("std_duration_min",    pa.float32()),
    pa.field("p10_duration_min",    pa.float32()),
    pa.field("p90_duration_min",    pa.float32()),
])


def haversine_km(lat1: pd.Series, lng1: pd.Series,
                 lat2: pd.Series, lng2: pd.Series) -> pd.Series:
    """Vectorised Haversine distance in km between two sets of GPS coordinates."""
    R = 6_371.0
    lat1, lng1, lat2, lng2 = map(np.radians, [lat1, lng1, lat2, lng2])
    dlat = lat2 - lat1
    dlng = lng2 - lng1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlng / 2) ** 2
    return (2 * R * np.arcsin(np.sqrt(a))).astype("float32")


def aggregate_rg(df: pd.DataFrame) -> pd.DataFrame:
    """Filter and aggregate one row group to key-level stats."""
    df["duration_min"] = (
        (df["ended_at"] - df["started_at"]).dt.total_seconds() / 60
    ).astype("float32")

    df["distance_km"] = haversine_km(
        df["start_lat"], df["start_lng"],
        df["end_lat"],   df["end_lng"],
    )

    df = df[
        (df["started_at"].dt.year >= YEAR_MIN) &
        (df["rideable_type"] != "unknown") &
        (df["duration_min"] > 0) &
        (df["duration_min"] <= DUR_MAX_MIN)
    ]

    if df.empty:
        return pd.DataFrame()

    df = df.copy()
    df["date"] = df["started_at"].dt.date
    df["hour"] = df["started_at"].dt.hour.astype("int8")

    grp_dur  = df.groupby(KEY_COLS, sort=False)["duration_min"]
    grp_dist = df.groupby(KEY_COLS, sort=False)["distance_km"]
    agg = pd.DataFrame({
        "n_trips":             grp_dur.count().astype("int32"),
        "avg_distance_km":     grp_dist.mean().astype("float32"),
        "mean_duration_min":   grp_dur.mean().astype("float32"),
        "median_duration_min": grp_dur.median().astype("float32"),
        "std_duration_min":    grp_dur.std().astype("float32"),
        "p10_duration_min":    grp_dur.quantile(0.10).astype("float32"),
        "p90_duration_min":    grp_dur.quantile(0.90).astype("float32"),
    }).reset_index()

    return agg


def final_merge(df: pd.DataFrame) -> pd.DataFrame:
    """Collapse duplicate keys from different shards. Fully vectorised."""
    df["_w_dur"]  = df["mean_duration_min"] * df["n_trips"]
    df["_w_dist"] = df["avg_distance_km"]   * df["n_trips"]
    grp = df.groupby(KEY_COLS, sort=False)
    n   = grp["n_trips"].sum()
    result = pd.DataFrame({
        "n_trips":             n.astype("int32"),
        "avg_distance_km":     (grp["_w_dist"].sum() / n).astype("float32"),
        "mean_duration_min":   (grp["_w_dur"].sum()  / n).astype("float32"),
        "median_duration_min": grp["median_duration_min"].mean().astype("float32"),
        "std_duration_min":    grp["std_duration_min"].mean().astype("float32"),
        "p10_duration_min":    grp["p10_duration_min"].min().astype("float32"),
        "p90_duration_min":    grp["p90_duration_min"].max().astype("float32"),
    }).reset_index()
    return result


# ── STEP 1: one shard per row group ──────────────────────────────────────────
print(f"\n{SEP}")
print("03_citibike_aggregate.py")
print(SEP)

pf   = pq.ParquetFile(IN_PATH)
meta = pf.metadata
print(f"  Input      : {IN_PATH}")
print(f"  Rows       : {meta.num_rows:,}  |  Row groups: {meta.num_row_groups}")
print(f"  Filter     : year >= {YEAR_MIN}, rideable_type != 'unknown', "
      f"0 < duration <= {DUR_MAX_MIN} min")
print(f"  Output     : {OUT_PATH}\n")

SHARD_DIR.mkdir(parents=True, exist_ok=True)
existing = {p.stem for p in SHARD_DIR.glob("shard_*.parquet")}
print(f"  Existing shards: {len(existing)} — skipping already-processed row groups")

total_trips = 0

print(f"\n{SEP}")
print("STEP 1 — Aggregate each row group -> shard")

for rg_idx in range(meta.num_row_groups):
    shard_stem = f"shard_{rg_idx:04d}"
    shard_path = SHARD_DIR / f"{shard_stem}.parquet"

    if shard_stem in existing:
        # count trips from existing shard for progress reporting
        total_trips += pq.read_metadata(shard_path).row_group(0).num_rows \
                       if shard_path.exists() else 0
        continue

    rg = pf.read_row_group(
        rg_idx,
        columns=["started_at", "ended_at", "start_tlc_zone", "end_tlc_zone",
                 "member_casual", "rideable_type",
                 "start_lat", "start_lng", "end_lat", "end_lng"]
    ).to_pandas()

    agg = aggregate_rg(rg)
    del rg

    if not agg.empty:
        total_trips += int(agg["n_trips"].sum())
        table = pa.Table.from_pandas(agg, schema=OUT_SCHEMA, preserve_index=False)
        pq.write_table(table, shard_path, compression="snappy")
        del table, agg

    if (rg_idx + 1) % 50 == 0:
        print(f"  ... {rg_idx+1}/{meta.num_row_groups} RGs | "
              f"trips in scope: {total_trips:,}")

print(f"  Step 1 complete. Total trips in scope: {total_trips:,}")

# ── STEP 2: concatenate shards into one file ──────────────────────────────────
print(f"\n{SEP}")
print("STEP 2 — Concatenate shards")

shards = sorted(SHARD_DIR.glob("shard_*.parquet"))
print(f"  Shards found: {len(shards)}")

MERGED_PATH = SHARD_DIR / "_merged.parquet"
writer = None
for sp in shards:
    t = pq.read_table(sp)
    if writer is None:
        writer = pq.ParquetWriter(MERGED_PATH, OUT_SCHEMA, compression="snappy")
    writer.write_table(t)
    del t
if writer:
    writer.close()

merged_meta = pq.ParquetFile(MERGED_PATH).metadata
print(f"  Merged: {merged_meta.num_rows:,} rows | "
      f"{MERGED_PATH.stat().st_size / 1e6:.1f} MB")

# ── STEP 3: final groupby to collapse cross-shard duplicate keys ──────────────
print(f"\n{SEP}")
print("STEP 3 — Final merge (row-group by row-group, vectorised)")

# 120M aggregated rows still too large for a single to_pandas().
# Read merged file row-group by row-group, do a pandas groupby on each chunk,
# collect results in a list, then do one final groupby on the collected
# (already-small) list of DataFrames.

pf_merged = pq.ParquetFile(MERGED_PATH)
n_rg = pf_merged.metadata.num_row_groups
print(f"  Merged file: {pf_merged.metadata.num_rows:,} rows | {n_rg} row groups")

partials = []
FLUSH3 = 500   # merge partials every N row groups of the merged file
for rg_idx in range(n_rg):
    rg = pf_merged.read_row_group(rg_idx).to_pandas()
    chunk = final_merge(rg)
    del rg
    partials.append(chunk)

    if len(partials) >= FLUSH3:
        combined = pd.concat(partials, ignore_index=True)
        partials = [final_merge(combined)]
        del combined

    if (rg_idx + 1) % 200 == 0:
        print(f"  ... {rg_idx+1}/{n_rg} RGs | partials in buffer: {len(partials)}")

print(f"  All row groups scanned. Running final collapse ...")
# Now partials is a list of small DataFrames — safe to concat + groupby
df_all = pd.concat(partials, ignore_index=True)
del partials
df_final = final_merge(df_all)
del df_all
print(f"  Rows after final merge  : {len(df_final):,}")

# ── Write final output ────────────────────────────────────────────────────────
df_final["date"]          = pd.to_datetime(df_final["date"]).dt.date
df_final["hour"]          = df_final["hour"].astype("int8")
df_final["start_tlc_zone"] = df_final["start_tlc_zone"].astype("int32")
df_final["end_tlc_zone"]   = df_final["end_tlc_zone"].astype("int32")

table = pa.Table.from_pandas(df_final, schema=OUT_SCHEMA, preserve_index=False)
pq.write_table(table, OUT_PATH, compression="snappy")

print(f"\n{SEP}")
print("DONE")
print(f"  Output rows : {len(df_final):,}  (unique keys)")
print(f"  Total trips : {df_final['n_trips'].sum():,}")
print(f"  File size   : {OUT_PATH.stat().st_size / 1e6:.1f} MB")

# ── Sanity check ──────────────────────────────────────────────────────────────
print(f"\n{SEP}")
print("SANITY CHECK")
print(f"\nmember_casual:")
print(df_final.groupby("member_casual")["n_trips"].sum().to_string())
print(f"\nrideable_type:")
print(df_final.groupby("rideable_type")["n_trips"].sum().to_string())
print(f"\nyear:")
df_final["year"] = pd.to_datetime(df_final["date"]).dt.year
print(df_final.groupby("year")["n_trips"].sum().to_string())
wmean_dur  = (df_final["mean_duration_min"] * df_final["n_trips"]).sum() / df_final["n_trips"].sum()
wmean_dist = (df_final["avg_distance_km"]   * df_final["n_trips"]).sum() / df_final["n_trips"].sum()
print(f"\nduration weighted mean : {wmean_dur:.2f} min")
print(f"p10 min : {df_final['p10_duration_min'].min():.2f}")
print(f"p90 max : {df_final['p90_duration_min'].max():.2f}")
print(f"distance weighted mean : {wmean_dist:.3f} km")

# ── Cleanup shards ────────────────────────────────────────────────────────────
print(f"\n{SEP}")
print("Cleaning up shards ...")
for sp in SHARD_DIR.glob("*.parquet"):
    sp.unlink()
SHARD_DIR.rmdir()
print(f"  Done.\n{SEP}\n")