"""
Census Tract to TLC Zone Crosswalk
Spatial join Census Tracts to TLC taxi zones with area-weighted aggregation.

Strategy:
1. Load Census Tracts shapefile (EPSG:4269) + ACS data
2. Load TLC zones shapefile (EPSG:2263)
3. Reproject Census Tracts to EPSG:2263 (NY State Plane)
4. Spatial intersection: compute overlap area between each tract-zone pair
5. Aggregate demographics to TLC zone level using area weights
6. Output: one row per TLC zone with population-weighted demographics

Input:
- cfg.raw_census / "shapefiles" / "tl_2022_36_tract" / "tl_2022_36_tract.shp"
- cfg.interim / "census" / "acs_2022_5yr_clean.parquet"
- cfg.nyc_zones / "taxi_zones.shp"

Output:
- cfg.interim / "census" / "census_to_tlc_crosswalk.parquet" (tract-zone pairs + weights)
- cfg.interim / "census" / "tlc_zone_demographics.parquet" (aggregated demographics per zone)

Goes in: src/02_spatial_temporal_alignment/
parents[N]: 2 (from 02_spatial_temporal_alignment/ to Tesi/)
"""

import importlib.util
from pathlib import Path
import pandas as pd
import geopandas as gpd
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np

# Load config
_cfg_path = Path(__file__).parents[3] / "00_config.py"
_spec = importlib.util.spec_from_file_location("config", _cfg_path)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
cfg = _mod.cfg

# Input paths
tiger_shp = cfg.raw_census / "shapefiles" / "tl_2022_36_tract" / "tl_2022_36_tract.shp"
acs_data = cfg.interim / "census" / "acs_2022_5yr_clean.parquet"
tlc_shp = cfg.nyc_zones / "taxi_zones.shp"

# Output paths
output_dir = cfg.interim / "census"
crosswalk_file = output_dir / "census_to_tlc_crosswalk.parquet"
demographics_file = output_dir / "tlc_zone_demographics.parquet"

print("="*70)
print("CENSUS TRACT → TLC ZONE CROSSWALK")
print("="*70)

# 1. Load Census Tracts shapefile
print("\n1. Loading Census Tracts shapefile...")
print(f"   {tiger_shp}")
tracts = gpd.read_file(tiger_shp)
print(f"   Total NY State tracts: {len(tracts)}")
print(f"   CRS: {tracts.crs}")

# Filter to NYC counties only
nyc_counties = ['005', '047', '061', '081', '085']
tracts_nyc = tracts[tracts['COUNTYFP'].isin(nyc_counties)].copy()
print(f"   NYC tracts: {len(tracts_nyc)}")

# 2. Load ACS data
print("\n2. Loading ACS 2022 data...")
print(f"   {acs_data}")
df_acs = pd.read_parquet(acs_data)
print(f"   Rows: {len(df_acs)}")

# Merge ACS data into tracts (left join on GEOID)
tracts_nyc = tracts_nyc.merge(df_acs, on='GEOID', how='left')
print(f"   Merged tracts with demographics: {len(tracts_nyc)}")

# Drop tracts with zero population (no demographic data to contribute)
tracts_nyc = tracts_nyc[~tracts_nyc['zero_population']].copy()
print(f"   Valid tracts (population > 0): {len(tracts_nyc)}")

# 3. Load TLC zones
print("\n3. Loading TLC taxi zones...")
print(f"   {tlc_shp}")
tlc_zones = gpd.read_file(tlc_shp)
print(f"   TLC zones: {len(tlc_zones)}")
print(f"   CRS: {tlc_zones.crs}")

# 4. Reproject Census Tracts to match TLC zones (EPSG:2263)
print("\n4. Reprojecting Census Tracts to EPSG:2263...")
tracts_nyc = tracts_nyc.to_crs(epsg=2263)
print(f"   ✓ Reprojected to {tracts_nyc.crs}")

# 5. Spatial intersection to compute overlap areas
print("\n5. Computing spatial intersections (tract × zone)...")
print("   This may take a few minutes...")

# Perform overlay (intersection)
overlay = gpd.overlay(tracts_nyc, tlc_zones, how='intersection')
print(f"   ✓ Intersection complete: {len(overlay)} tract-zone pairs")

# Calculate overlap area (in square feet, since EPSG:2263 is in feet)
overlay['overlap_area_sqft'] = overlay.geometry.area

# Calculate area weight for each tract-zone pair
# Weight = overlap_area / total_tract_area
overlay['tract_area_sqft'] = overlay.groupby('GEOID')['overlap_area_sqft'].transform('sum')
overlay['area_weight'] = overlay['overlap_area_sqft'] / overlay['tract_area_sqft']

# Sanity check: weights should sum to ~1.0 per tract
weight_sums = overlay.groupby('GEOID')['area_weight'].sum()
max_deviation = (weight_sums - 1.0).abs().max()
print(f"   Weight sum check (max deviation from 1.0): {max_deviation:.2e}")

# 6. Save crosswalk (tract-zone pairs with weights)
print("\n6. Saving crosswalk table...")

crosswalk = overlay[[
    'GEOID', 'LocationID', 'zone', 
    'overlap_area_sqft', 'tract_area_sqft', 'area_weight',
    'total_population'
]].copy()

crosswalk = crosswalk.rename(columns={
    'GEOID': 'tract_geoid',
    'LocationID': 'tlc_zone_id',
    'zone': 'tlc_zone_name'
})

# Convert to parquet
crosswalk_schema = pa.schema([
    ('tract_geoid', pa.string()),
    ('tlc_zone_id', pa.int32()),
    ('tlc_zone_name', pa.string()),
    ('overlap_area_sqft', pa.float64()),
    ('tract_area_sqft', pa.float64()),
    ('area_weight', pa.float64()),
    ('total_population', pa.int64()),
])

table = pa.Table.from_pandas(crosswalk, schema=crosswalk_schema)
pq.write_table(table, crosswalk_file)
print(f"   ✓ Saved: {crosswalk_file}")
print(f"     Rows: {len(crosswalk)}")

# 7. Aggregate demographics to TLC zone level
print("\n7. Aggregating demographics to TLC zones...")

# Variables to aggregate (population-weighted means for ratios, sums for counts)
sum_vars = [
    'total_population', 'workers_total', 'workers_car', 'workers_public_transit',
    'workers_bicycle', 'workers_walked', 'tenure_vehicles_universe',
    'owner_no_vehicle', 'renter_no_vehicle', 'total_no_vehicle'
]

weighted_mean_vars = [
    'median_hh_income', 'share_workers_car', 'share_workers_pt',
    'share_workers_bike', 'share_workers_walk', 'share_no_vehicle'
]

# For sum variables: multiply by area_weight, then sum by zone
agg_dict = {}
for var in sum_vars:
    if var in overlay.columns:
        overlay[f'{var}_weighted'] = overlay[var] * overlay['area_weight']
        agg_dict[f'{var}_weighted'] = 'sum'

# For weighted means: weight by population × area_weight
for var in weighted_mean_vars:
    if var in overlay.columns:
        overlay[f'{var}_weighted'] = (
            overlay[var] * overlay['total_population'] * overlay['area_weight']
        )
        agg_dict[f'{var}_weighted'] = 'sum'

# Also track total population weight for averaging
agg_dict['total_population_weighted'] = 'sum'

# Group by TLC zone
zone_agg = overlay.groupby('LocationID').agg(agg_dict).reset_index()

# Rename LocationID
zone_agg = zone_agg.rename(columns={'LocationID': 'tlc_zone_id'})

# Compute final weighted means (divide by total population)
for var in weighted_mean_vars:
    zone_agg[var] = (
        zone_agg[f'{var}_weighted'] / zone_agg['total_population_weighted']
    )
    zone_agg = zone_agg.drop(columns=[f'{var}_weighted'])

# Rename sum variables (remove _weighted suffix)
for var in sum_vars:
    if f'{var}_weighted' in zone_agg.columns:
        zone_agg = zone_agg.rename(columns={f'{var}_weighted': var})

# Add zone names
zone_lookup = tlc_zones[['LocationID', 'zone', 'borough']].rename(
    columns={'LocationID': 'tlc_zone_id', 'zone': 'tlc_zone_name'}
)
zone_agg = zone_agg.merge(zone_lookup, on='tlc_zone_id', how='left')

# Reorder columns
id_cols = ['tlc_zone_id', 'tlc_zone_name', 'borough']
demo_cols = [c for c in zone_agg.columns if c not in id_cols]
zone_agg = zone_agg[id_cols + demo_cols]

# Round and convert count variables to int64 (they're float due to weighting)
int_cols = [
    'total_population', 'workers_total', 'workers_car', 'workers_public_transit',
    'workers_bicycle', 'workers_walked', 'tenure_vehicles_universe',
    'owner_no_vehicle', 'renter_no_vehicle', 'total_no_vehicle'
]
for col in int_cols:
    if col in zone_agg.columns:
        zone_agg[col] = zone_agg[col].round().astype('Int64')  # nullable int

# 8. Summary stats
print("\n" + "="*70)
print("AGGREGATED DEMOGRAPHICS BY TLC ZONE")
print("="*70)

print(f"\nTotal TLC zones with demographics: {len(zone_agg)}")
print(f"\nMedian household income by zone:")
print(f"  Min:    ${zone_agg['median_hh_income'].min():>12,.0f}")
print(f"  Median: ${zone_agg['median_hh_income'].median():>12,.0f}")
print(f"  Mean:   ${zone_agg['median_hh_income'].mean():>12,.0f}")
print(f"  Max:    ${zone_agg['median_hh_income'].max():>12,.0f}")

print(f"\nCommute mode shares (mean across zones):")
print(f"  Car:              {zone_agg['share_workers_car'].mean():>6.1%}")
print(f"  Public transit:   {zone_agg['share_workers_pt'].mean():>6.1%}")
print(f"  Bicycle:          {zone_agg['share_workers_bike'].mean():>6.1%}")
print(f"  Walked:           {zone_agg['share_workers_walk'].mean():>6.1%}")

print(f"\nNo-vehicle households:")
print(f"  Mean share: {zone_agg['share_no_vehicle'].mean():>6.1%}")
print(f"  Median:     {zone_agg['share_no_vehicle'].median():>6.1%}")

# Top 5 zones by median income
print(f"\nTop 5 zones by median household income:")
top5 = zone_agg.nlargest(5, 'median_hh_income')[
    ['tlc_zone_name', 'borough', 'median_hh_income', 'total_population']
]
print(top5.to_string(index=False))

# 9. Save aggregated demographics
print(f"\n9. Saving aggregated demographics...")

demographics_schema = pa.schema([
    ('tlc_zone_id', pa.int32()),
    ('tlc_zone_name', pa.string()),
    ('borough', pa.string()),
    ('total_population', pa.int64()),
    ('workers_total', pa.int64()),
    ('workers_car', pa.int64()),
    ('workers_public_transit', pa.int64()),
    ('workers_bicycle', pa.int64()),
    ('workers_walked', pa.int64()),
    ('tenure_vehicles_universe', pa.int64()),
    ('owner_no_vehicle', pa.int64()),
    ('renter_no_vehicle', pa.int64()),
    ('total_no_vehicle', pa.int64()),
    ('median_hh_income', pa.float64()),
    ('share_workers_car', pa.float64()),
    ('share_workers_pt', pa.float64()),
    ('share_workers_bike', pa.float64()),
    ('share_workers_walk', pa.float64()),
    ('share_no_vehicle', pa.float64()),
])

table = pa.Table.from_pandas(zone_agg, schema=demographics_schema)
pq.write_table(table, demographics_file)

print(f"   ✓ Saved: {demographics_file}")
print(f"     Rows: {len(zone_agg)}")
print(f"     Size: {demographics_file.stat().st_size / (1024**2):.1f} MB")

print("\n" + "="*70)
print("✓ CROSSWALK COMPLETE")
print("="*70)