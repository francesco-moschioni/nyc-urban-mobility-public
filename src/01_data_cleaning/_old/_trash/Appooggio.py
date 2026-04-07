"""
Sanity check Census downloads - hardcoded paths
Run from anywhere after downloads complete
"""

from pathlib import Path
import pandas as pd
import geopandas as gpd

# Report file (same folder as script)
report_path = Path(__file__).parent / "sanity_check_report.txt"

# reset file
with open(report_path, "w", encoding="utf-8") as f:
    f.write("=== CENSUS DATA SANITY CHECK REPORT ===\n\n")

def log(msg):
    print(msg)
    with open(report_path, "a", encoding="utf-8") as f:
        f.write(msg + "\n")

# Hardcoded paths
TIGER_DIR = Path(r"D:\tesi\data\raw\census\shapefiles\tl_2022_36_tract")
ACS_FILE = Path(r"D:\tesi\data\raw\census\acs\acs_2022_5yr_nyc.csv")

log("="*60)
log("CENSUS DATA SANITY CHECK")
log("="*60)

# 1. Check TIGER shapefile
log("\n1. TIGER/Line Shapefile Check")
log("-" * 40)

if not TIGER_DIR.exists():
    log(f"✗ Directory not found: {TIGER_DIR}")
else:
    shp_file = TIGER_DIR / "tl_2022_36_tract.shp"
    if not shp_file.exists():
        log(f"✗ Shapefile not found: {shp_file}")
    else:
        log(f"✓ Shapefile exists: {shp_file}")
        
        # Load and inspect
        gdf = gpd.read_file(shp_file)
        log(f"  Total tracts in NY State: {len(gdf)}")
        log(f"  CRS: {gdf.crs}")
        log(f"  Columns: {list(gdf.columns)}")
        
        # Filter to NYC counties
        nyc_counties = ['005', '047', '061', '081', '085']
        gdf_nyc = gdf[gdf['COUNTYFP'].isin(nyc_counties)]
        log(f"\n  NYC tracts only: {len(gdf_nyc)}")
        log(f"  Breakdown by county:")
        for county in nyc_counties:
            county_name = {
                '005': 'Bronx',
                '047': 'Kings (Brooklyn)',
                '061': 'New York (Manhattan)',
                '081': 'Queens',
                '085': 'Richmond (Staten Island)'
            }[county]
            count = len(gdf_nyc[gdf_nyc['COUNTYFP'] == county])
            log(f"    {county_name}: {count} tracts")

# 2. Check ACS data
log("\n2. ACS 5-Year 2022 Data Check")
log("-" * 40)

if not ACS_FILE.exists():
    log(f"✗ File not found: {ACS_FILE}")
else:
    log(f"✓ File exists: {ACS_FILE}")
    
    df = pd.read_csv(ACS_FILE)
    log(f"  Total rows: {len(df)}")
    log(f"  Columns: {len(df.columns)}")
    log(f"\n  Column names:")
    for col in df.columns:
        log(f"    - {col}")
    
    log(f"\n  Sample (first 3 rows):")
    log(df.head(3).to_string())
    
    # Check for nulls in key variables
    log(f"\n  Null counts in key variables:")
    key_vars = ['B19013_001E', 'B01003_001E', 'B08301_001E']
    for var in key_vars:
        if var in df.columns:
            nulls = df[var].isna().sum()
            log(f"    {var}: {nulls} nulls")
    
    # Median income stats
    if 'B19013_001E' in df.columns:
        df['income'] = pd.to_numeric(df['B19013_001E'], errors='coerce')
        log(f"\n  Median household income (B19013_001E):")
        log(f"    Min: ${df['income'].min():,.0f}")
        log(f"    Median: ${df['income'].median():,.0f}")
        log(f"    Max: ${df['income'].max():,.0f}")
        log(f"    Mean: ${df['income'].mean():,.0f}")

log("\n" + "="*60)
log("✓ Sanity check complete")
log("="*60)