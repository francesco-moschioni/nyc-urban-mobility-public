"""
03_mta_aggregate.py
Stage   : 02_spatial_temporal_alignment/02_mta/
Input   : cfg.interim_spatial / "mta_flows_tlc.parquet"
Output  : cfg.interim / "mta" / "mta_zone_hour.parquet"
Logic   : Aggregate estimated subway flows to (origin_zone, dest_zone, date, hour),
          summing estimated_flow across all fare_class_category values and all
          station-to-station pairs that map to the same TLC zone pair.
          Row-group-by-row-group to avoid OOM on the 9.2 GB file.
          Resume-safe: existing output deleted at startup if schema sentinel missing.
"""

import importlib.util
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
import pandas as pd

# ── config ────────────────────────────────────────────────────────────────────
_cfg_path = Path(__file__).parents[3] / "00_config.py"
_spec = importlib.util.spec_from_file_location("config", _cfg_path)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
cfg = _mod.cfg

# ── paths ─────────────────────────────────────────────────────────────────────
IN_FILE  = Path(cfg.interim_spatial) / "mta_flows_tlc.parquet"
OUT_DIR  = Path(cfg.interim) / "mta"
OUT_FILE = OUT_DIR / "mta_zone_hour.parquet"
SHARD_DIR = Path(cfg.interim) / "mta" / "shards" / "mta"

OUT_DIR.mkdir(parents=True, exist_ok=True)
SHARD_DIR.mkdir(parents=True, exist_ok=True)

# ── output schema ─────────────────────────────────────────────────────────────
OUT_SCHEMA = pa.schema([
    ("PULocationID",         pa.int32()),
    ("DOLocationID",         pa.int32()),
    ("date",                 pa.date32()),
    ("hour",                 pa.int8()),
    ("dataset",              pa.string()),
    ("fare_class_category",  pa.string()),
    ("trip_count",           pa.int32()),
])

SENTINEL_COL = "trip_count"

# ── purge stale output ────────────────────────────────────────────────────────
if OUT_FILE.exists():
    try:
        existing_schema = pq.read_schema(OUT_FILE)
        if SENTINEL_COL not in existing_schema.names:
            print(f"[purge] {OUT_FILE.name} missing sentinel column '{SENTINEL_COL}' — deleting.")
            OUT_FILE.unlink()
        else:
            print(f"[skip] {OUT_FILE.name} already exists with correct schema. Delete manually to re-run.")
            raise SystemExit(0)
    except Exception as e:
        if "SystemExit" in type(e).__name__:
            raise
        print(f"[purge] Could not read schema of {OUT_FILE.name} ({e}) — deleting.")
        OUT_FILE.unlink()

# ── columns to read from source ───────────────────────────────────────────────
READ_COLS = ["origin_zone", "dest_zone", "date", "hour", "fare_class_category", "estimated_flow"]

# ── process row-group by row-group ────────────────────────────────────────────
pf = pq.ParquetFile(IN_FILE)
n_rg = pf.metadata.num_row_groups
print(f"[info] {IN_FILE.name}: {n_rg} row groups, {pf.metadata.num_rows:,} rows total")

existing_shards = set(int(p.stem) for p in SHARD_DIR.glob("*.parquet") if p.stem.isdigit())
print(f"[info] {len(existing_shards)} shards already on disk — skipping those row groups")

for rg_idx in range(n_rg):
    if rg_idx in existing_shards:
        continue

    shard_path = SHARD_DIR / f"{rg_idx}.parquet"

    # read one row group, only needed columns
    tbl = pf.read_row_group(rg_idx, columns=READ_COLS)

    # drop sentinel rows (dest_id == -1 mapped to dest_zone == -1 or missing)
    # origin_zone and dest_zone should be valid TLC zones [1, 263]
    mask = pc.and_(
        pc.greater(tbl.column("origin_zone"), 0),
        pc.greater(tbl.column("dest_zone"),   0),
    )
    tbl = tbl.filter(mask)

    if tbl.num_rows == 0:
        print(f"  [rg {rg_idx:>4}] all rows dropped — writing empty shard")
        empty = pa.table({
            "PULocationID": pa.array([], type=pa.int32()),
            "DOLocationID": pa.array([], type=pa.int32()),
            "date":         pa.array([], type=pa.date32()),
            "hour":         pa.array([], type=pa.int8()),
            "dataset":      pa.array([], type=pa.string()),
            "fare_class_category": pa.array([], type=pa.string()),
            "trip_count":   pa.array([], type=pa.int32()),
        })
        pq.write_table(empty, shard_path)
        continue

    # to pandas for groupby (small after filter — one RG ~few hundred MB)
    df = tbl.to_pandas()
    del tbl

    # rename to canonical names
    df.rename(columns={"origin_zone": "PULocationID",
                        "dest_zone":   "DOLocationID"}, inplace=True)

    # coerce date to date (may come as timestamp)
    if pd.api.types.is_datetime64_any_dtype(df["date"]):
        df["date"] = df["date"].dt.date

    # aggregate: sum flows across station pairs for same zone-zone-date-hour-fare_class
    agg = (
        df.groupby(["PULocationID", "DOLocationID", "date", "hour", "fare_class_category"], as_index=False)
          ["estimated_flow"]
          .sum()
    )
    agg.rename(columns={"estimated_flow": "trip_count"}, inplace=True)
    agg["dataset"] = "subway"

    # cast types
    agg["PULocationID"] = agg["PULocationID"].astype("int32")
    agg["DOLocationID"] = agg["DOLocationID"].astype("int32")
    agg["hour"]         = agg["hour"].astype("int8")
    agg["trip_count"]   = agg["trip_count"].astype("int32")

    # write shard
    shard_tbl = pa.Table.from_pandas(agg, schema=OUT_SCHEMA, preserve_index=False)
    pq.write_table(shard_tbl, shard_path)

    print(f"  [rg {rg_idx:>4}] {len(df):>12,} rows → {len(agg):>10,} cells  →  {shard_path.name}")
    del df, agg, shard_tbl

# ── merge shards ──────────────────────────────────────────────────────────────
shard_files = sorted(SHARD_DIR.glob("*.parquet"), key=lambda p: int(p.stem))
print(f"\n[merge] {len(shard_files)} shards → {OUT_FILE.name}")

writer = None
total_rows = 0
try:
    for sp in shard_files:
        t = pq.read_table(sp, schema=OUT_SCHEMA)
        if writer is None:
            writer = pq.ParquetWriter(OUT_FILE, schema=OUT_SCHEMA)
        writer.write_table(t)
        total_rows += t.num_rows
        del t
finally:
    if writer is not None:
        writer.close()

print(f"[done] {OUT_FILE.name}: {total_rows:,} rows written")

# ── delete shards ─────────────────────────────────────────────────────────────
for sp in shard_files:
    sp.unlink()
print(f"[cleanup] {len(shard_files)} shards deleted")
