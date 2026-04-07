"""
00_config.py
────────────
Single entry point for all project paths.
Reads CODE_ROOT and DATA_ROOT from the .env file in the project root.

Usage in any script:
    import importlib.util
    from pathlib import Path

    _cfg_path = Path(__file__).parents[N] / "00_config.py"  # adjust N to reach Tesi/
    _spec = importlib.util.spec_from_file_location("config", _cfg_path)
    _mod  = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(_mod)
    cfg = _mod.cfg

    df = pd.read_parquet(cfg.processed / "choice_set.parquet")

parents[N] depth rule:
    parents[1] → script in src/ or project root
    parents[2] → script in src/01_data_cleaning/00_download/ or src/02_spatial.../
    parents[3] → script in src/01_data_cleaning/01_cleaning/
    parents[4] → script in src/07_results/00_descriptives/
"""

from pathlib import Path
from dotenv import load_dotenv
import os

# .env is in the same folder as this file (Tesi/)
_ENV_FILE = Path(__file__).parent / ".env"
load_dotenv(_ENV_FILE)

_CODE_ROOT = Path(os.environ["CODE_ROOT"])
_DATA_ROOT = Path(os.environ["DATA_ROOT"])


class _Paths:
    # ── Roots ─────────────────────────────────────────────────
    code    : Path = _CODE_ROOT
    data    : Path = _DATA_ROOT / "data"

    # ── Raw data (disk D) ─────────────────────────────────────
    raw              : Path = _DATA_ROOT / "data" / "raw"
    raw_tlc          : Path = _DATA_ROOT / "data" / "raw" / "tlc"
    raw_mta          : Path = _DATA_ROOT / "data" / "raw" / "mta"
    raw_gtfs         : Path = _DATA_ROOT / "data" / "raw" / "gtfs"
    raw_gtfs_current : Path = _DATA_ROOT / "data" / "raw" / "gtfs" / "current"
    raw_gtfs_archive : Path = _DATA_ROOT / "data" / "raw" / "gtfs" / "archive"
    raw_citibike     : Path = _DATA_ROOT / "data" / "raw" / "citibike"
    raw_weather      : Path = _DATA_ROOT / "data" / "raw" / "weather"
    raw_census       : Path = _DATA_ROOT / "data" / "raw" / "census"
    raw_traffic      : Path = _DATA_ROOT / "data" / "raw" / "traffic"

    # ── Interim (disk D) ──────────────────────────────────────
    interim          : Path = _DATA_ROOT / "data" / "interim"
    interim_citibike : Path = _DATA_ROOT / "data" / "interim" / "citibike"
    interim_spatial  : Path = _DATA_ROOT / "data" / "interim" / "spatial_alignment"
    interim_temporal : Path = _DATA_ROOT / "data" / "interim" / "temporal_panels"

    # ── Processed / external (disk D) ─────────────────────────
    processed        : Path = _DATA_ROOT / "data" / "processed"
    external         : Path = _DATA_ROOT / "data" / "external"
    nyc_zones        : Path = _DATA_ROOT / "data" / "external" / "nyc_zones"

    # ── Outputs (disk D) ──────────────────────────────────────
    models           : Path = _DATA_ROOT / "models"
    outputs          : Path = _DATA_ROOT / "outputs"
    tables           : Path = _DATA_ROOT / "outputs" / "tables"
    tables_desc      : Path = _DATA_ROOT / "outputs" / "tables" / "00_descriptives"
    figures          : Path = _DATA_ROOT / "outputs" / "figures"
    figures_desc     : Path = _DATA_ROOT / "outputs" / "figures" / "00_descriptives"
    reports          : Path = _DATA_ROOT / "outputs" / "reports"

    # ── Code (OneDrive) ───────────────────────────────────────
    src              : Path = _CODE_ROOT / "src"
    notebooks        : Path = _CODE_ROOT / "notebooks"
    docs             : Path = _CODE_ROOT / "docs"
    docs_dicts       : Path = _CODE_ROOT / "docs" / "data_dictionaries"
    thesis           : Path = _CODE_ROOT / "thesis"


cfg = _Paths()