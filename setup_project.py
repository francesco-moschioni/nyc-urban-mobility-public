"""
setup_project.py
================
Run this script ONCE to initialise the full project folder structure.
Creates data directories on D:\\tesi and code directories next to this script (OneDrive).

Usage:
    python setup_project.py
"""

from pathlib import Path

# ─────────────────────────────────────────────────────────────────────────────
# EDIT THESE TWO PATHS IF NECESSARY
# ─────────────────────────────────────────────────────────────────────────────
CODE_ROOT = Path(__file__).parent.resolve()   # Where this script lives (OneDrive)
DATA_ROOT = Path(r"D:\tesi")                  # Heavy data disk
# ─────────────────────────────────────────────────────────────────────────────

# ── Data directories (disk D) ─────────────────────────────────────────────────
DATA_DIRS = [
    # TLC trip records
    "data/raw/tlc/yellow",
    "data/raw/tlc/fhvhv",
    "data/raw/tlc/fhv",
    "data/raw/tlc/green",
    # MTA ridership and alerts
    "data/raw/mta",
    # GTFS static feeds
    "data/raw/gtfs/current",
    "data/raw/gtfs/archive",
    # Other raw sources
    "data/raw/citibike",
    "data/raw/weather",
    "data/raw/census",
    "data/raw/traffic",
    # Interim processing
    "data/interim/spatial_alignment",
    "data/interim/temporal_panels",
    # Final datasets and auxiliary
    "data/processed",
    "data/external/nyc_zones",
    # Models and outputs
    "models",
    "outputs/tables",
    "outputs/figures",
    "outputs/reports",
]

# ── Code directories (OneDrive) ───────────────────────────────────────────────
CODE_DIRS = [
    "src/01_data_cleaning",
    "src/02_spatial_temporal_alignment",
    "src/03_outside_option",
    "src/04_instruments",
    "src/05_models",
    "src/06_nlp_disruption_index",
    "src/07_results",
    "notebooks",
    "docs",
    "thesis/chapters",
    "thesis/bibliography",
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def create_dirs(root: Path, dirs: list[str], label: str) -> None:
    print(f"\n  Creating {label} directories in: {root}")
    for d in dirs:
        path = root / d
        path.mkdir(parents=True, exist_ok=True)
        print(f"    [ok]  {path}")


def write_env(code_root: Path, data_root: Path) -> None:
    env_path = code_root / ".env"
    content = f"""\
# ─────────────────────────────────────────────────────────────
# .env  —  Local project paths  (DO NOT share / commit to Git)
# Each collaborator fills in their own local paths.
# ─────────────────────────────────────────────────────────────

# Root of the code folder (OneDrive)
CODE_ROOT={code_root}

# Root of the heavy data folder (local disk)
DATA_ROOT={data_root}

# Socrata app token — for MTA open-data downloads
SOCRATA_APP_TOKEN=

# Mobility Database API token — for GTFS archive downloads
# Register free at: https://mobilitydatabase.org
MOBILITY_DB_TOKEN=
"""
    env_path.write_text(content, encoding="utf-8")
    print(f"\n    [ok]  {env_path}")


def write_env_example(code_root: Path) -> None:
    path = code_root / ".env.example"
    content = """\
# ─────────────────────────────────────────────────────────────
# .env.example  —  Template for collaborators
# Copy this file to .env and fill in your local paths.
# ─────────────────────────────────────────────────────────────

# Root of the code folder (e.g. OneDrive clone or local repo)
CODE_ROOT=C:\\Users\\YourName\\OneDrive\\thesis_nyc_mobility

# Root of the heavy data folder (local disk with enough space)
DATA_ROOT=D:\\tesi

# Socrata app token — for MTA open-data downloads
SOCRATA_APP_TOKEN=your_token_here

# Mobility Database API token — for GTFS archive downloads
# Register free at: https://mobilitydatabase.org
MOBILITY_DB_TOKEN=your_token_here
"""
    path.write_text(content, encoding="utf-8")
    print(f"    [ok]  {path}")


def write_gitignore(code_root: Path) -> None:
    path = code_root / ".gitignore"
    content = """\
# Environment variables — contain local paths and secrets
.env

# Python
__pycache__/
*.py[cod]
.venv/
*.egg-info/

# Jupyter checkpoints
.ipynb_checkpoints/

# OS
.DS_Store
Thumbs.db

# Heavy data files (live on disk D, not in OneDrive)
*.parquet
*.pkl
*.csv
*.log
"""
    path.write_text(content, encoding="utf-8")
    print(f"    [ok]  {path}")


def write_readme_raw(data_root: Path) -> None:
    path = data_root / "data" / "raw" / "README.md"
    content = """\
# data/raw — Original raw data

> **Warning**: This folder contains raw data that must never be overwritten by scripts.
> All processing must write results to `interim/` or `processed/`.

## Sources

| Folder        | Dataset                        | Source                                                                 |
|---------------|--------------------------------|------------------------------------------------------------------------|
| `tlc/`        | TLC Trip Records (Taxi, HVFHV) | https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page          |
| `mta/`        | MTA Ridership + Service Alerts | https://data.ny.gov / MTA Developer Portal                             |
| `gtfs/current`| MTA Subway GTFS (latest)       | https://rrgtfsfeeds.s3.amazonaws.com/gtfs_subway.zip                   |
| `gtfs/archive`| MTA Subway GTFS (2019–2024)    | https://mobilitydatabase.org/feeds/gtfs/mdb-79                         |
| `citibike/`   | Citi Bike Trip Data            | https://citibikenyc.com/system-data                                    |
| `weather/`    | NOAA Local Climatological Data | https://www.ncei.noaa.gov/cdo-web/                                     |
| `census/`     | ACS + ZCTA Shapefiles          | https://www.census.gov/geo/maps-data/                                  |
| `traffic/`    | NYC DOT Traffic Counts         | https://data.cityofnewyork.us                                          |
"""
    path.write_text(content, encoding="utf-8")
    print(f"    [ok]  {path}")


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("  Setup: NYC Urban Mobility Demand Estimation")
    print("=" * 60)

    create_dirs(DATA_ROOT, DATA_DIRS, "DATA (disk D)")
    create_dirs(CODE_ROOT, CODE_DIRS, "CODE (OneDrive)")

    print("\n  Writing project files:")
    write_env(CODE_ROOT, DATA_ROOT)
    write_env_example(CODE_ROOT)
    write_gitignore(CODE_ROOT)
    write_readme_raw(DATA_ROOT)

    print("\n" + "=" * 60)
    print("  Setup complete.")
    print(f"  Code  ->  {CODE_ROOT}")
    print(f"  Data  ->  {DATA_ROOT}")
    print("=" * 60)
    print("\nNext steps:")
    print("  1. pip install python-dotenv requests")
    print("  2. Check that .env has the correct paths")
    print("  3. Add your API tokens to .env (Socrata, Mobility Database)")
    print("  4. Run: python src/01_data_cleaning/01_download_gtfs.py")
