"""
Download ACS 5-Year 2022 data for NYC Census Tracts
Fetches demographic variables from Census Bureau API.
Requires CENSUS_API_KEY in .env

Variables:
- B19013_001E: Median household income
- B01003_001E: Total population
- B08301_001E: Total workers (commute mode universe)
- B08301_002E: Workers - Car, truck, or van
- B08301_010E: Workers - Public transportation
- B08301_018E: Workers - Bicycle
- B08301_019E: Workers - Walked
- B25044_001E: Tenure by vehicles available (universe)
- B25044_003E: Owner occupied - No vehicle available
- B25044_010E: Renter occupied - No vehicle available
- B01001_001E: Total population (for age breakdown)
- B01001_003E: Male - Under 5 years
- B01001_007E: Male - 18 and 19 years (start working age)
- B01001_020E: Male - 60 and 61 years
- B01001_025E: Male - 85 years and over

Output: cfg.raw_census / "acs" / "acs_2022_5yr_nyc.csv"
"""

import importlib.util
from pathlib import Path
import requests
import pandas as pd
from dotenv import load_dotenv
import os

# Load config
_cfg_path = Path(__file__).parents[3] / "00_config.py"
_spec = importlib.util.spec_from_file_location("config", _cfg_path)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
cfg = _mod.cfg

# Load API key from .env
load_dotenv()
api_key = os.getenv("CENSUS_API_KEY")
if not api_key:
    raise ValueError("CENSUS_API_KEY not found in .env file")

# Output directory
out_dir = cfg.raw_census / "acs"
out_dir.mkdir(parents=True, exist_ok=True)

# NYC counties (FIPS state 36 + county codes)
nyc_counties = {
    "061": "New York (Manhattan)",
    "047": "Kings (Brooklyn)",
    "081": "Queens",
    "005": "Bronx",
    "085": "Richmond (Staten Island)"
}

# ACS variables to fetch
variables = [
    "B19013_001E",  # Median household income
    "B01003_001E",  # Total population
    "B08301_001E",  # Total workers (commute mode)
    "B08301_002E",  # Workers - Car/truck/van
    "B08301_010E",  # Workers - Public transportation
    "B08301_018E",  # Workers - Bicycle
    "B08301_019E",  # Workers - Walked
    "B25044_001E",  # Vehicles available universe
    "B25044_003E",  # Owner - No vehicle
    "B25044_010E",  # Renter - No vehicle
    "B01001_001E",  # Total population (age)
    "B01001_003E",  # Male - Under 5
    "B01001_007E",  # Male - 18-19 years
    "B01001_020E",  # Male - 60-61 years
    "B01001_025E",  # Male - 85+ years
]

# Base URL for ACS 5-Year 2022
base_url = "https://api.census.gov/data/2022/acs/acs5"

# Build query
var_string = ",".join(variables)

all_data = []

for county_code, county_name in nyc_counties.items():
    print(f"Fetching data for {county_name} (county {county_code})...")
    
    params = {
        "get": f"NAME,{var_string}",
        "for": "tract:*",
        "in": f"state:36 county:{county_code}",
        "key": api_key
    }
    
    response = requests.get(base_url, params=params)
    response.raise_for_status()
    
    data = response.json()
    
    # First row is headers
    headers = data[0]
    rows = data[1:]
    
    df = pd.DataFrame(rows, columns=headers)
    all_data.append(df)
    
    print(f"  ✓ Fetched {len(rows)} tracts")

# Combine all counties
df_combined = pd.concat(all_data, ignore_index=True)

# Create GEOID (state + county + tract)
df_combined['GEOID'] = (
    df_combined['state'] + 
    df_combined['county'] + 
    df_combined['tract']
)

# Reorder columns
cols = ['GEOID', 'NAME', 'state', 'county', 'tract'] + variables
df_combined = df_combined[cols]

# Save
out_path = out_dir / "acs_2022_5yr_nyc.csv"
df_combined.to_csv(out_path, index=False)

print(f"\n✓ Saved to: {out_path}")
print(f"  Total tracts: {len(df_combined)}")
print(f"  Columns: {len(df_combined.columns)}")
print(f"\nSample:")
print(df_combined.head(3))
