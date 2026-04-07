"""
02_mta_flows_to_tlc.py
Stage: 02_spatial_temporal_alignment

Input:
    cfg.interim / spatial_alignment / mta_flows_estimated.parquet
    cfg.external / nyc_zones / taxi_zones.shp

Output:
    cfg.interim / spatial_alignment / mta_flows_tlc.parquet

Logic:
    1. Extract unique station points (origin + destination) from the flows file
    2. Reproject station coordinates WGS84 → EPSG:2263, spatial join → TLC LocationID
    3. Scan flows row-group by row-group, add origin_zone / dest_zone columns,
       drop rows with dest_id == -1 or unmatched stations, write incrementally.

No aggregation: output has the same granularity as input
(origin_id, dest_id, date, hour, fare_class_category, estimated_flow)
with two extra columns (origin_zone, dest_zone).

Output schema:
    origin_id            int32
    dest_id              int32
    origin_zone          int32   TLC LocationID of origin station
    dest_zone            int32   TLC LocationID of destination station
    date                 date32
    hour                 int8
    fare_class_category  string
    estimated_flow       float32
    origin_lat           float32
    origin_lon           float32
    dest_lat             float32
    dest_lon             float32
"""

import importlib.util
from pathlib import Path
import pandas as pd
import geopandas as gpd
import pyarrow as pa
import pyarrow.parquet as pq

# ── config ────────────────────────────────────────────────────────────────────
_cfg_path = Path(__file__).parents[4] / "00_config.py"
_spec = importlib.util.spec_from_file_location("config", _cfg_path)
_mod  = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
cfg = _mod.cfg

IN_FLOWS = cfg.interim / "spatial_alignment" / "mta_flows_estimated.parquet"
IN_SHP   = cfg.external / "nyc_zones" / "taxi_zones.shp"
OUT_PATH = cfg.interim / "spatial_alignment" / "mta_flows_tlc.parquet"

SEP = "=" * 60

# ── 1. build station → TLC zone lookup ───────────────────────────────────────
print(f"\n{SEP}")
print("STEP 1 — Building station → TLC zone lookup")

pf   = pq.ParquetFile(IN_FLOWS)
meta = pf.metadata
print(f"  Flows file: {meta.num_rows:,} rows, {meta.num_row_groups} row groups")

print("  Scanning for unique station coordinates...")
station_coords = {}  # station_id -> (lat, lon)

for rg_idx in range(meta.num_row_groups):
    rg = pf.read_row_group(
        rg_idx,
        columns=["origin_id", "dest_id",
                 "origin_lat", "origin_lon",
                 "dest_lat",   "dest_lon"]
    ).to_pandas()

    for _, row in (rg[["origin_id", "origin_lat", "origin_lon"]]
                   .drop_duplicates("origin_id").iterrows()):
        sid = int(row["origin_id"])
        if sid not in station_coords:
            station_coords[sid] = (float(row["origin_lat"]), float(row["origin_lon"]))

    valid = rg[rg["dest_id"] != -1]
    for _, row in (valid[["dest_id", "dest_lat", "dest_lon"]]
                   .drop_duplicates("dest_id").iterrows()):
        sid = int(row["dest_id"])
        if sid not in station_coords:
            station_coords[sid] = (float(row["dest_lat"]), float(row["dest_lon"]))

    if (rg_idx + 1) % 200 == 0:
        print(f"  ... {rg_idx+1}/{meta.num_row_groups} row groups scanned")

print(f"  Unique station IDs found: {len(station_coords):,}")

# Reproject WGS84 → EPSG:2263 and spatial join to TLC zones
ids  = list(station_coords)
lats = [station_coords[s][0] for s in ids]
lons = [station_coords[s][1] for s in ids]

stations_gdf = gpd.GeoDataFrame(
    {"station_id": ids},
    geometry=gpd.points_from_xy(lons, lats),
    crs="EPSG:4326"
).to_crs("EPSG:2263")

zones_gdf = gpd.read_file(IN_SHP)[["LocationID", "geometry"]]

joined = gpd.sjoin(stations_gdf, zones_gdf, how="left", predicate="within")

# Stations on zone boundaries → retry with nearest
outside_mask = joined["LocationID"].isna()
n_outside = outside_mask.sum()
if n_outside > 0:
    print(f"  {n_outside} stations outside all zones — retrying with nearest join")
    outside_stations = stations_gdf[
        stations_gdf["station_id"].isin(joined.loc[outside_mask, "station_id"])
    ]
    nearest = gpd.sjoin_nearest(outside_stations, zones_gdf, how="left")
    fix_map = dict(zip(nearest["station_id"], nearest["LocationID"]))
    for sid, loc in fix_map.items():
        joined.loc[joined["station_id"] == sid, "LocationID"] = loc

still_outside = joined[joined["LocationID"].isna()]["station_id"].tolist()
if still_outside:
    print(f"  WARNING: {len(still_outside)} stations still unmatched → will be dropped")

station_to_zone = {
    int(row["station_id"]): (int(row["LocationID"]) if pd.notna(row["LocationID"]) else -1)
    for _, row in joined.iterrows()
}
station_to_zone[-1] = -1  # sentinel for no-OD-match rows

print(f"  Lookup complete: {len(station_to_zone):,} entries")

# ── 2. enrich flows row-group by row-group ────────────────────────────────────
print(f"\n{SEP}")
print("STEP 2 — Adding origin_zone / dest_zone to flows")

OUT_COLS = [
    "origin_id", "dest_id", "origin_zone", "dest_zone",
    "date", "hour", "fare_class_category", "estimated_flow",
    "origin_lat", "origin_lon", "dest_lat", "dest_lon",
]

OUT_SCHEMA = pa.schema([
    pa.field("origin_id",            pa.int32()),
    pa.field("dest_id",              pa.int32()),
    pa.field("origin_zone",          pa.int32()),
    pa.field("dest_zone",            pa.int32()),
    pa.field("date",                 pa.timestamp("ns")),
    pa.field("hour",                 pa.int8()),
    pa.field("fare_class_category",  pa.string()),
    pa.field("estimated_flow",       pa.float32()),
    pa.field("origin_lat",           pa.float32()),
    pa.field("origin_lon",           pa.float32()),
    pa.field("dest_lat",             pa.float32()),
    pa.field("dest_lon",             pa.float32()),
])

writer    = None
total_in  = 0
total_out = 0

for rg_idx in range(meta.num_row_groups):
    rg = pf.read_row_group(rg_idx).to_pandas()
    total_in += len(rg)

    # Drop no-OD-match rows
    rg = rg[rg["dest_id"] != -1]

    # Map station → TLC zone
    rg["origin_zone"] = rg["origin_id"].map(station_to_zone).fillna(-1).astype("int32")
    rg["dest_zone"]   = rg["dest_id"].map(station_to_zone).fillna(-1).astype("int32")

    # Drop rows where station could not be matched to any zone
    rg = rg[(rg["origin_zone"] != -1) & (rg["dest_zone"] != -1)]

    total_out += len(rg)

    # Cast types
    rg["origin_id"]   = rg["origin_id"].astype("int32")
    rg["dest_id"]     = rg["dest_id"].astype("int32")
    rg["hour"]        = rg["hour"].astype("int8")
    rg["estimated_flow"] = rg["estimated_flow"].astype("float32")
    rg["origin_lat"]  = rg["origin_lat"].astype("float32")
    rg["origin_lon"]  = rg["origin_lon"].astype("float32")
    rg["dest_lat"]    = rg["dest_lat"].astype("float32")
    rg["dest_lon"]    = rg["dest_lon"].astype("float32")

    table = pa.Table.from_pandas(rg[OUT_COLS], schema=OUT_SCHEMA, preserve_index=False)

    if writer is None:
        writer = pq.ParquetWriter(OUT_PATH, OUT_SCHEMA)
    writer.write_table(table)
    del table, rg

    if (rg_idx + 1) % 100 == 0:
        print(f"  ... {rg_idx+1}/{meta.num_row_groups} RGs | "
              f"rows in: {total_in:,} | rows out: {total_out:,}")

if writer:
    writer.close()

print(f"\n{SEP}")
print("DONE")
print(f"  Total rows read    : {total_in:,}")
print(f"  Total rows written : {total_out:,}")
print(f"  Drop rate          : {100*(total_in - total_out)/total_in:.2f}%")
if OUT_PATH.exists():
    print(f"  File size          : {OUT_PATH.stat().st_size / 1_048_576:.1f} MB")

# ── 3. sanity check ───────────────────────────────────────────────────────────
print(f"\n{SEP}")
print("SANITY CHECK (first row group)")
out_pf = pq.ParquetFile(OUT_PATH)
sample = out_pf.read_row_group(0).to_pandas()
print(f"  Output rows total      : {out_pf.metadata.num_rows:,}")
print(f"  Output row groups      : {out_pf.metadata.num_row_groups}")
print(f"  Sample rows            : {len(sample):,}")
print(f"  Unique origin stations : {sample['origin_id'].nunique()}")
print(f"  Unique origin zones    : {sample['origin_zone'].nunique()}")
print(f"  Unique dest zones      : {sample['dest_zone'].nunique()}")
print(f"  Flow range             : {sample['estimated_flow'].min():.4f} – {sample['estimated_flow'].max():.2f}")
print(f"\nPrime 5 righe:")
print(sample[["origin_id","dest_id","origin_zone","dest_zone",
              "date","hour","fare_class_category","estimated_flow"]].head().to_string(index=False))