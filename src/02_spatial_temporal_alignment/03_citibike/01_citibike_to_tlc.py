"""
02_citibike_to_tlc.py
Stage: src/02_spatial_temporal_alignment/

Input:
    cfg.interim / citibike / citibike_clean.parquet
    cfg.external / nyc_zones / taxi_zones.shp

Output:
    cfg.interim / citibike / citibike_tlc.parquet

Logic:
    1. Extract unique (start_lat, start_lng) and (end_lat, end_lng) coordinate
       pairs from the parquet, reproject WGS84 -> EPSG:2263, spatial join
       -> TLC LocationID. Boundary points retried with nearest join.
    2. Scan citibike_clean.parquet row-group by row-group, map each trip's
       start/end coordinates to TLC zones, drop unmatched rows, write
       incrementally via PyArrow ParquetWriter (low memory).

No aggregation: output is still trip-level, same granularity as input,
with two extra columns (start_tlc_zone, end_tlc_zone).

Output schema (all columns from citibike_clean.parquet plus):
    start_tlc_zone   int32   TLC LocationID of trip origin
    end_tlc_zone     int32   TLC LocationID of trip destination
"""

import importlib.util
from pathlib import Path
import pandas as pd
import geopandas as gpd
import pyarrow as pa
import pyarrow.parquet as pq

# ── Config ────────────────────────────────────────────────────────────────────
# parents[0]=02_spatial_temporal_alignment  [1]=src  [2]=Tesi
_cfg_path = Path(__file__).parents[4] / "00_config.py"
_spec = importlib.util.spec_from_file_location("config", _cfg_path)
_mod  = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
cfg = _mod.cfg

IN_CLEAN = cfg.interim / "citibike" / "citibike_clean.parquet"
IN_SHP   = cfg.external / "nyc_zones" / "taxi_zones.shp"
OUT_PATH = cfg.interim / "citibike" / "citibike_tlc.parquet"

SEP = "=" * 60

# ── STEP 1: build coordinate -> TLC zone lookups ──────────────────────────────
print(f"\n{SEP}")
print("STEP 1 — Building coordinate -> TLC zone lookups")

pf   = pq.ParquetFile(IN_CLEAN)
meta = pf.metadata
print(f"  Input file : {IN_CLEAN}")
print(f"  Rows       : {meta.num_rows:,}  |  Row groups: {meta.num_row_groups}")

print("  Scanning for unique coordinate pairs ...")
start_coords = set()
end_coords   = set()

for rg_idx in range(meta.num_row_groups):
    rg = pf.read_row_group(
        rg_idx,
        columns=["start_lat", "start_lng", "end_lat", "end_lng"]
    ).to_pandas()
    start_coords.update(
        zip(rg["start_lat"].round(6), rg["start_lng"].round(6))
    )
    end_coords.update(
        zip(rg["end_lat"].round(6), rg["end_lng"].round(6))
    )
    if (rg_idx + 1) % 100 == 0:
        print(f"  ... {rg_idx+1}/{meta.num_row_groups} row groups scanned")

all_coords = start_coords | end_coords
print(f"  Unique start coord pairs : {len(start_coords):,}")
print(f"  Unique end coord pairs   : {len(end_coords):,}")
print(f"  Unique combined          : {len(all_coords):,}")


def coords_to_zone_lookup(coord_set: set, zones_gdf: gpd.GeoDataFrame) -> dict:
    """
    Spatial join a set of (lat, lng) tuples to TLC LocationIDs.
    Returns dict: (lat_rounded, lng_rounded) -> LocationID (int) or -1.
    """
    lats = [c[0] for c in coord_set]
    lngs = [c[1] for c in coord_set]

    pts_gdf = gpd.GeoDataFrame(
        {"lat": lats, "lng": lngs},
        geometry=gpd.points_from_xy(lngs, lats),
        crs="EPSG:4326"
    ).to_crs("EPSG:2263")

    joined = gpd.sjoin(pts_gdf, zones_gdf, how="left", predicate="within")

    # Retry boundary points with nearest join
    outside_mask = joined["LocationID"].isna()
    n_outside = outside_mask.sum()
    if n_outside > 0:
        print(f"  {n_outside} points outside all zones — retrying with nearest join")
        outside_pts = pts_gdf[outside_mask].copy()
        nearest = gpd.sjoin_nearest(outside_pts, zones_gdf, how="left")
        joined.loc[outside_mask, "LocationID"] = nearest["LocationID"].values

    still_outside = joined["LocationID"].isna().sum()
    if still_outside:
        print(f"  WARNING: {still_outside} points still unmatched -> will be dropped")

    lookup = {}
    for i, row in joined.iterrows():
        key = (round(row["lat"], 6), round(row["lng"], 6))
        lookup[key] = int(row["LocationID"]) if pd.notna(row["LocationID"]) else -1

    return lookup


print("\n  Loading TLC zones shapefile ...")
zones_gdf = gpd.read_file(IN_SHP)[["LocationID", "geometry"]].to_crs("EPSG:2263")

print("  Running spatial join on all unique coordinate pairs ...")
coord_to_zone = coords_to_zone_lookup(all_coords, zones_gdf)
print(f"  Lookup built: {len(coord_to_zone):,} entries")

# ── STEP 2: enrich trips row-group by row-group ───────────────────────────────
print(f"\n{SEP}")
print("STEP 2 — Mapping trips to TLC zones (row-group by row-group)")

# Build output schema by appending new fields to input schema
in_schema  = pq.read_schema(IN_CLEAN)
out_schema = pa.schema(
    list(in_schema) + [
        pa.field("start_tlc_zone", pa.int32()),
        pa.field("end_tlc_zone",   pa.int32()),
    ]
)

writer    = None
total_in  = 0
total_out = 0

for rg_idx in range(meta.num_row_groups):
    rg = pf.read_row_group(rg_idx).to_pandas()
    total_in += len(rg)

    # Round coords to match lookup keys
    rg["_slat"] = rg["start_lat"].round(6)
    rg["_slng"] = rg["start_lng"].round(6)
    rg["_elat"] = rg["end_lat"].round(6)
    rg["_elng"] = rg["end_lng"].round(6)

    rg["start_tlc_zone"] = rg.apply(
        lambda r: coord_to_zone.get((r["_slat"], r["_slng"]), -1), axis=1
    ).astype("int32")
    rg["end_tlc_zone"] = rg.apply(
        lambda r: coord_to_zone.get((r["_elat"], r["_elng"]), -1), axis=1
    ).astype("int32")

    rg = rg.drop(columns=["_slat", "_slng", "_elat", "_elng"])
    rg = rg[(rg["start_tlc_zone"] != -1) & (rg["end_tlc_zone"] != -1)]
    total_out += len(rg)

    if not rg.empty:
        for col in rg.select_dtypes("category").columns:
            rg[col] = rg[col].astype(str)
        table = pa.Table.from_pandas(rg, schema=out_schema, preserve_index=False)
        if writer is None:
            writer = pq.ParquetWriter(OUT_PATH, out_schema, compression="snappy")
        writer.write_table(table)
        del table

    del rg

    if (rg_idx + 1) % 100 == 0:
        print(f"  ... {rg_idx+1}/{meta.num_row_groups} RGs | "
              f"in: {total_in:,} | out: {total_out:,}")

if writer:
    writer.close()

print(f"\n{SEP}")
print("DONE")
print(f"  Total rows read    : {total_in:,}")
print(f"  Total rows written : {total_out:,}")
print(f"  Drop rate          : {100*(total_in - total_out)/total_in:.2f}%")
if OUT_PATH.exists():
    print(f"  File size          : {OUT_PATH.stat().st_size / 1_048_576:.1f} MB")

# ── STEP 3: sanity check ──────────────────────────────────────────────────────
print(f"\n{SEP}")
print("SANITY CHECK (first row group)")
out_pf = pq.ParquetFile(OUT_PATH)
sample = out_pf.read_row_group(0).to_pandas()
print(f"  Output rows total      : {out_pf.metadata.num_rows:,}")
print(f"  Output row groups      : {out_pf.metadata.num_row_groups}")
print(f"  Unique start TLC zones : {sample['start_tlc_zone'].nunique()}")
print(f"  Unique end TLC zones   : {sample['end_tlc_zone'].nunique()}")
print(f"\nmember_casual distribution (sample):")
print(sample["member_casual"].value_counts().to_string())
print(f"\nrideable_type distribution (sample):")
print(sample["rideable_type"].value_counts().to_string())
print(f"\nPrime 5 righe:")
print(sample[["started_at", "start_tlc_zone", "end_tlc_zone",
              "member_casual", "rideable_type"]].head().to_string(index=False))
