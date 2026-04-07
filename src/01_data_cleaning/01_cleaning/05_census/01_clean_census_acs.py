"""
Clean ACS 5-Year 2022 data for NYC Census Tracts
Handles Census Bureau sentinel values and zero-population tracts.

Census sentinel values (indicate missing/unavailable data):
- -666666666: Data not available
- -888888888: Not applicable
- -222222222: No sample observations
- -333333333: Median falls in lowest or highest interval
- -555555555: Estimate controlled to be equal to zero

Input:  cfg.raw_census / "acs" / "acs_2022_5yr_nyc.csv"
Output: cfg.interim / "census" / "acs_2022_5yr_clean.parquet"

Goes in: src/01_data_cleaning/01_cleaning/
parents[N]: 3 (from 01_cleaning/ to Tesi/)
"""

import importlib.util
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Load config
_cfg_path = Path(__file__).parents[4] / "00_config.py"
_spec = importlib.util.spec_from_file_location("config", _cfg_path)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
cfg = _mod.cfg

# Input/output paths
input_file = cfg.raw_census / "acs" / "acs_2022_5yr_nyc.csv"
output_dir = cfg.interim / "census"
output_dir.mkdir(parents=True, exist_ok=True)
output_file = output_dir / "acs_2022_5yr_clean.parquet"

print("="*60)
print("CLEANING ACS 2022 5-YEAR DATA")
print("="*60)

# Load raw data
print(f"\nLoading: {input_file}")
df = pd.read_csv(input_file)
print(f"  Raw rows: {len(df)}")

# Census sentinel values to replace with NaN
SENTINEL_VALUES = [-666666666, -888888888, -222222222, -333333333, -555555555]

# Variable name mapping (for readability)
VAR_MAP = {
    'B19013_001E': 'median_hh_income',
    'B01003_001E': 'total_population',
    'B08301_001E': 'workers_total',
    'B08301_002E': 'workers_car',
    'B08301_010E': 'workers_public_transit',
    'B08301_018E': 'workers_bicycle',
    'B08301_019E': 'workers_walked',
    'B25044_001E': 'tenure_vehicles_universe',
    'B25044_003E': 'owner_no_vehicle',
    'B25044_010E': 'renter_no_vehicle',
    'B01001_001E': 'pop_age_total',
    'B01001_003E': 'male_under_5',
    'B01001_007E': 'male_18_19',
    'B01001_020E': 'male_60_61',
    'B01001_025E': 'male_85_plus',
}

# Convert all numeric columns
numeric_cols = list(VAR_MAP.keys())

print("\nReplacing Census sentinel values with NaN...")
for col in numeric_cols:
    df[col] = pd.to_numeric(df[col], errors='coerce')
    df.loc[df[col].isin(SENTINEL_VALUES), col] = pd.NA

# Rename columns
df = df.rename(columns=VAR_MAP)

# Create derived variables
print("\nCreating derived variables...")

# Share of workers by commute mode (avoid division by zero)
df['share_workers_car'] = df['workers_car'] / df['workers_total'].replace(0, pd.NA)
df['share_workers_pt'] = df['workers_public_transit'] / df['workers_total'].replace(0, pd.NA)
df['share_workers_bike'] = df['workers_bicycle'] / df['workers_total'].replace(0, pd.NA)
df['share_workers_walk'] = df['workers_walked'] / df['workers_total'].replace(0, pd.NA)

# Share of households with no vehicle
df['total_no_vehicle'] = df['owner_no_vehicle'] + df['renter_no_vehicle']
df['share_no_vehicle'] = df['total_no_vehicle'] / df['tenure_vehicles_universe'].replace(0, pd.NA)

# Flag zero-population tracts (parks, industrial zones, etc.)
df['zero_population'] = (df['total_population'] == 0) | (df['total_population'].isna())

print(f"  Zero-population tracts: {df['zero_population'].sum()}")

# Reorder columns
id_cols = ['GEOID', 'NAME', 'state', 'county', 'tract']
derived_cols = [
    'share_workers_car', 'share_workers_pt', 'share_workers_bike', 'share_workers_walk',
    'total_no_vehicle', 'share_no_vehicle', 'zero_population'
]
base_cols = list(VAR_MAP.values())

df = df[id_cols + base_cols + derived_cols]

# Summary stats
print("\n" + "="*60)
print("SUMMARY STATISTICS (excluding zero-pop tracts)")
print("="*60)

df_valid = df[~df['zero_population']].copy()

print(f"\nValid tracts (population > 0): {len(df_valid)}")
print(f"Zero-population tracts: {df['zero_population'].sum()}")

print("\nMedian household income:")
print(f"  Min:    ${df_valid['median_hh_income'].min():>12,.0f}")
print(f"  Median: ${df_valid['median_hh_income'].median():>12,.0f}")
print(f"  Mean:   ${df_valid['median_hh_income'].mean():>12,.0f}")
print(f"  Max:    ${df_valid['median_hh_income'].max():>12,.0f}")
print(f"  NaN:    {df_valid['median_hh_income'].isna().sum():>12}")

print("\nCommute mode shares (mean across tracts):")
print(f"  Car:              {df_valid['share_workers_car'].mean():>6.1%}")
print(f"  Public transit:   {df_valid['share_workers_pt'].mean():>6.1%}")
print(f"  Bicycle:          {df_valid['share_workers_bike'].mean():>6.1%}")
print(f"  Walked:           {df_valid['share_workers_walk'].mean():>6.1%}")

print("\nHouseholds with no vehicle:")
print(f"  Mean share: {df_valid['share_no_vehicle'].mean():>6.1%}")
print(f"  Median:     {df_valid['share_no_vehicle'].median():>6.1%}")

# Save to parquet
print(f"\nSaving to: {output_file}")

# Force string types for ID columns (they come as int/mixed from API)
df['GEOID'] = df['GEOID'].astype(str)
df['state'] = df['state'].astype(str)
df['county'] = df['county'].astype(str)
df['tract'] = df['tract'].astype(str)

# Define schema for parquet (force string types for IDs)
schema = pa.schema([
    ('GEOID', pa.string()),
    ('NAME', pa.string()),
    ('state', pa.string()),
    ('county', pa.string()),
    ('tract', pa.string()),
    ('median_hh_income', pa.float64()),
    ('total_population', pa.int64()),
    ('workers_total', pa.int64()),
    ('workers_car', pa.int64()),
    ('workers_public_transit', pa.int64()),
    ('workers_bicycle', pa.int64()),
    ('workers_walked', pa.int64()),
    ('tenure_vehicles_universe', pa.int64()),
    ('owner_no_vehicle', pa.int64()),
    ('renter_no_vehicle', pa.int64()),
    ('pop_age_total', pa.int64()),
    ('male_under_5', pa.int64()),
    ('male_18_19', pa.int64()),
    ('male_60_61', pa.int64()),
    ('male_85_plus', pa.int64()),
    ('share_workers_car', pa.float64()),
    ('share_workers_pt', pa.float64()),
    ('share_workers_bike', pa.float64()),
    ('share_workers_walk', pa.float64()),
    ('total_no_vehicle', pa.int64()),
    ('share_no_vehicle', pa.float64()),
    ('zero_population', pa.bool_()),
])

# Convert to PyArrow Table and write
table = pa.Table.from_pandas(df, schema=schema)
pq.write_table(table, output_file)

print(f"✓ Saved {len(df)} rows")
print(f"  Size: {output_file.stat().st_size / (1024**2):.1f} MB")

print("\n" + "="*60)
print("✓ ACS CLEANING COMPLETE")
print("="*60)