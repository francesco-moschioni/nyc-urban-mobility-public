"""
04_mta_to_tlc_schema.py
Stage: 02_spatial_temporal_alignment/02_mta/
Input: cfg.interim/mta/mta_zone_hour.parquet
Output: cfg.interim/mta/mta_zone_hour_tlc.parquet

Converts MTA aggregated output to TLC-compatible schema.
Renames columns, fixes types, adds missing TLC columns as NULL.
This enables downstream modeling with consistent schema across all modes.
"""

import importlib.util
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc

# ── Config ────────────────────────────────────────────────────────────────────
_cfg_path = Path(__file__).parents[3] / "00_config.py"
_spec = importlib.util.spec_from_file_location("config", _cfg_path)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
cfg = _mod.cfg

IN_PATH = Path(cfg.interim) / "mta" / "mta_zone_hour.parquet"
OUT_PATH = Path(cfg.interim) / "mta" / "mta_zone_hour_tlc.parquet"

# ── Output schema (matching TLC) ───────────────────────────────────────────────
OUT_SCHEMA = pa.schema([
    pa.field("PULocationID",         pa.int32()),
    pa.field("DOLocationID",         pa.int32()),
    pa.field("date",                 pa.date32()),
    pa.field("hour",                 pa.int8()),
    pa.field("dataset",              pa.string()),
    pa.field("hail_type",            pa.string()),      # from fare_class_category
    pa.field("trip_count",           pa.int32()),
    pa.field("avg_distance",         pa.float32()),     # null for MTA
    pa.field("avg_duration_min",     pa.float32()),     # null for MTA
    pa.field("median_duration_min",  pa.float32()),     # null for MTA
    pa.field("std_duration_min",     pa.float32()),     # null for MTA
    pa.field("p10_duration_min",     pa.float32()),     # null for MTA
    pa.field("p90_duration_min",     pa.float32()),     # null for MTA
    pa.field("avg_passengers",       pa.float32()),     # null for MTA
    pa.field("avg_fare",             pa.float32()),     # null for MTA
    pa.field("avg_total",            pa.float32()),     # null for MTA
])

print(f"\n{'='*70}")
print("04_mta_to_tlc_schema.py")
print('='*70)
print(f"  Input  : {IN_PATH}")
print(f"  Output : {OUT_PATH}\n")

# ── Read & transform row-group by row-group ───────────────────────────────────
pf = pq.ParquetFile(IN_PATH)
n_rg = pf.metadata.num_row_groups
print(f"  Row groups : {n_rg}")
print(f"  Total rows : {pf.metadata.num_rows:,}\n")

NULL_COLS = [
    "avg_distance", "avg_duration_min", "median_duration_min",
    "std_duration_min", "p10_duration_min", "p90_duration_min",
    "avg_passengers", "avg_fare", "avg_total",
]

writer = None
total_rows = 0
try:
    for rg_idx in range(n_rg):
        tbl = pf.read_row_group(rg_idx)

        # rename fare_class_category → hail_type
        if "fare_class_category" in tbl.schema.names:
            tbl = tbl.rename_columns(
                ["hail_type" if n == "fare_class_category" else n
                 for n in tbl.schema.names]
            )

        # dataset "subway" → "mta"
        if "dataset" in tbl.schema.names:
            col = pc.if_else(pc.equal(tbl.column("dataset"), "subway"), "mta",
                             tbl.column("dataset"))
            tbl = tbl.set_column(tbl.schema.get_field_index("dataset"), "dataset", col)

        # cast trip_count to int32 if needed
        if tbl.schema.field("trip_count").type != pa.int32():
            idx = tbl.schema.get_field_index("trip_count")
            tbl = tbl.set_column(idx, "trip_count",
                                 tbl.column("trip_count").cast(pa.int32()))

        # add NULL float32 columns for TLC fields absent in MTA
        for col_name in NULL_COLS:
            tbl = tbl.append_column(
                pa.field(col_name, pa.float32()),
                pa.nulls(tbl.num_rows, type=pa.float32()),
            )

        # reorder + cast to final schema
        tbl = tbl.select(OUT_SCHEMA.names).cast(OUT_SCHEMA)

        if writer is None:
            writer = pq.ParquetWriter(OUT_PATH, schema=OUT_SCHEMA)
        writer.write_table(tbl)
        total_rows += tbl.num_rows
        del tbl

        print(f"  [rg {rg_idx:>4}] written")
finally:
    if writer is not None:
        writer.close()

print(f"  [done] Written {OUT_PATH.name} with {total_rows:,} rows")
print(f"  Schema : {OUT_SCHEMA.names}\n")
print('='*70)
