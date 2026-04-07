"""
Download TIGER/Line shapefiles for NYC Census Tracts (2022)
Downloads shapefiles for 5 NYC counties from Census Bureau FTP.
No API key required.

Output: cfg.raw_census / "shapefiles" / "tl_2022_36_tract" / ...
"""

import importlib.util
from pathlib import Path
import requests
import zipfile
import io

# Load config
_cfg_path = Path(__file__).parents[3] / "00_config.py"
_spec = importlib.util.spec_from_file_location("config", _cfg_path)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
cfg = _mod.cfg

# Output directory
out_dir = cfg.raw_census / "shapefiles"
out_dir.mkdir(parents=True, exist_ok=True)

# TIGER/Line 2022 Census Tracts for New York State (FIPS 36)
# URL pattern: https://www2.census.gov/geo/tiger/TIGER2022/TRACT/tl_2022_{state_fips}_tract.zip
# New York State FIPS: 36
# NYC counties (FIPS): New York 061, Kings 047, Queens 081, Bronx 005, Richmond 085

base_url = "https://www2.census.gov/geo/tiger/TIGER2022/TRACT/"
state_fips = "36"
filename = f"tl_2022_{state_fips}_tract.zip"
url = base_url + filename

print(f"Downloading TIGER/Line 2022 Census Tracts for New York State...")
print(f"URL: {url}")

# Download
response = requests.get(url, stream=True)
response.raise_for_status()

# Extract directly to output directory
zip_content = io.BytesIO(response.content)
with zipfile.ZipFile(zip_content, 'r') as zip_ref:
    extract_to = out_dir / f"tl_2022_{state_fips}_tract"
    extract_to.mkdir(exist_ok=True)
    zip_ref.extractall(extract_to)
    
print(f"✓ Extracted to: {extract_to}")
print(f"  Files: {list(extract_to.glob('*'))}")

# Note: shapefile covers entire NY State
# Filter to NYC counties (061, 047, 081, 005, 085) happens during cleaning/spatial join
print("\nNote: Shapefile contains all NY State tracts.")
print("NYC counties (to filter later):")
print("  - New York (Manhattan): 061")
print("  - Kings (Brooklyn): 047")
print("  - Queens: 081")
print("  - Bronx: 005")
print("  - Richmond (Staten Island): 085")
