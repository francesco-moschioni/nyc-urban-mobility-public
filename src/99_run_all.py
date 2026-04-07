"""
Master orchestrator script for NYC Urban Mobility demand estimation pipeline.

Runs the complete data processing pipeline in phases:
  - Phase 1: Download data from all sources (parallel)
  - Phase 2: Clean and normalize by dataset type (sequential per type)
  - Phase 3: Spatial-temporal alignment across datasets (sequential)

Usage:
  python src/99_run_all.py                 # Run all phases
  python src/99_run_all.py --phase 1       # Run Phase 1 only
  python src/99_run_all.py --phase 2       # Run Phase 2 only
  python src/99_run_all.py --phase 3       # Run Phase 3 only
  python src/99_run_all.py --dry-run       # Print commands without executing
  python src/99_run_all.py --phase 2 --dataset mta  # Run Phase 2 for MTA only

Dependencies:
  - All scripts in 00_download/, 01_cleaning/, 02_spatial_temporal_alignment/
  - 00_config.py at project root
  - .env file with API credentials
"""

import argparse
import sys
import logging
from pathlib import Path
from subprocess import run, CalledProcessError
from datetime import datetime
import importlib.util

# Load config
_cfg_path = Path(__file__).parents[1] / "00_config.py"  # src/99_run_all.py -> parents[1] = Tesi/
_spec = importlib.util.spec_from_file_location("config", _cfg_path)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
cfg = _mod.cfg

# Setup logging
log_dir = Path(__file__).parent / "logs"
log_dir.mkdir(exist_ok=True, parents=True)
log_file = log_dir / f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Paths to scripts
PHASE1_DOWNLOADS = [
    "src/01_data_cleaning/00_download/download_gtfs.py",
    "src/01_data_cleaning/00_download/download_mta.py",
    "src/01_data_cleaning/00_download/download_tlc.py",
    "src/01_data_cleaning/00_download/download_citibike.py",
    "src/01_data_cleaning/00_download/download_weather.py",
    "src/01_data_cleaning/00_download/download_census_acs.py",
    "src/01_data_cleaning/00_download/download_census_tiger.py",
    "src/01_data_cleaning/00_download/download_nycgov.py",
]

PHASE2_CLEANING = {
    "tlc": [
        "src/01_data_cleaning/01_cleaning/01_tlc/01_tlc_clean.py",
        "src/01_data_cleaning/01_cleaning/01_tlc/02_tlc_aggregate.py",
    ],
    "mta": [
        "src/01_data_cleaning/01_cleaning/02_mta/01_clean_mta_entries.py",
        "src/01_data_cleaning/01_cleaning/02_mta/02_clean_mta_od.py",
    ],
    "citibike": [
        "src/01_data_cleaning/01_cleaning/03_citibike/01_citibike_unzip.py",
        "src/01_data_cleaning/01_cleaning/03_citibike/02_citibike_clean.py",
    ],
    "weather": [
        "src/01_data_cleaning/01_cleaning/04_weather/01_clean_weather_isd.py",
    ],
    "census": [
        "src/01_data_cleaning/01_cleaning/05_census/01_clean_census_acs.py",
    ],
}

PHASE3_ALIGNMENT = [
    "src/02_spatial_temporal_alignment/02_mta/01_mta_flows.py",
    "src/02_spatial_temporal_alignment/02_mta/02_mta_flows_to_tlc.py",
    "src/02_spatial_temporal_alignment/02_mta/03_mta_aggregate.py",
    "src/02_spatial_temporal_alignment/02_mta/04_mta_to_tlc_schema.py",
    "src/02_spatial_temporal_alignment/03_citibike/01_citibike_to_tlc.py",
    "src/02_spatial_temporal_alignment/03_citibike/02_citibike_aggregate.py",
    "src/02_spatial_temporal_alignment/05_census/01_census_to_tlc_crosswalk.py",
]


def run_script(script_path: str, dry_run: bool = False) -> bool:
    """Execute a single script. Return True if successful."""
    try:
        if dry_run:
            logger.info(f"[DRY-RUN] python {script_path}")
            return True
        else:
            logger.info(f"Running: {script_path}")
            result = run(
                [sys.executable, script_path],
                cwd=Path(__file__).parents[1],  # Tesi/
                check=True,
                capture_output=False
            )
            logger.info(f"✓ {script_path}")
            return True
    except CalledProcessError as e:
        logger.error(f"✗ {script_path} failed with exit code {e.returncode}")
        return False


def run_phase_1(dry_run: bool = False, skip: list = None) -> bool:
    """
    Phase 1: Download data from all sources (parallel conceptually, but run sequentially for now).
    """
    logger.info("=" * 80)
    logger.info("PHASE 1: DOWNLOAD DATA")
    logger.info("=" * 80)

    skip = skip or []
    success = True

    for script in PHASE1_DOWNLOADS:
        script_name = Path(script).name
        if script_name in skip:
            logger.info(f"⊘ Skipping {script_name}")
            continue

        if not run_script(script, dry_run=dry_run):
            success = False
            logger.warning(f"Phase 1 failed at {script}. Continuing with remaining downloads...")

    return success


def run_phase_2(dry_run: bool = False, dataset: str = None, skip: list = None) -> bool:
    """
    Phase 2: Clean and normalize data by dataset type (sequential per type).

    Args:
        dataset: If specified, run only this dataset (e.g., 'tlc', 'mta', 'citibike')
    """
    logger.info("=" * 80)
    logger.info("PHASE 2: CLEANING & NORMALIZATION")
    logger.info("=" * 80)

    skip = skip or []
    success = True

    datasets_to_run = [dataset] if dataset else PHASE2_CLEANING.keys()

    for ds in datasets_to_run:
        if ds not in PHASE2_CLEANING:
            logger.error(f"Unknown dataset: {ds}")
            continue

        logger.info(f"\n--- {ds.upper()} cleaning ---")
        for script in PHASE2_CLEANING[ds]:
            script_name = Path(script).name
            if script_name in skip:
                logger.info(f"⊘ Skipping {script_name}")
                continue

            if not run_script(script, dry_run=dry_run):
                success = False
                logger.error(f"Phase 2 ({ds}) failed at {script}. Aborting phase.")
                break  # Stop this dataset, continue with next

    return success


def run_phase_3(dry_run: bool = False, skip: list = None) -> bool:
    """
    Phase 3: Spatial-temporal alignment (sequential per dataset type).
    """
    logger.info("=" * 80)
    logger.info("PHASE 3: SPATIAL-TEMPORAL ALIGNMENT")
    logger.info("=" * 80)

    skip = skip or []
    success = True

    for script in PHASE3_ALIGNMENT:
        script_name = Path(script).name
        if script_name in skip:
            logger.info(f"⊘ Skipping {script_name}")
            continue

        if not run_script(script, dry_run=dry_run):
            success = False
            logger.error(f"Phase 3 failed at {script}. Aborting.")
            break  # Critical phase — stop if any script fails

    return success


def main():
    parser = argparse.ArgumentParser(
        description="Master orchestrator for NYC Urban Mobility demand estimation pipeline"
    )
    parser.add_argument(
        "--phase",
        choices=["all", "1", "2", "3"],
        default="all",
        help="Which phase(s) to run"
    )
    parser.add_argument(
        "--dataset",
        choices=["tlc", "mta", "citibike", "weather", "census"],
        default=None,
        help="For Phase 2 only: limit to specific dataset"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print commands without executing"
    )
    parser.add_argument(
        "--skip",
        nargs="+",
        default=[],
        help="Skip specific scripts by name (e.g., download_mta.py download_tlc.py)"
    )

    args = parser.parse_args()

    logger.info(f"Starting pipeline orchestration. Phase: {args.phase}, Dry-run: {args.dry_run}")

    all_success = True

    if args.phase in ["all", "1"]:
        if not run_phase_1(dry_run=args.dry_run, skip=args.skip):
            all_success = False
            if args.phase == "1":
                logger.error("Phase 1 completed with errors")

    if args.phase in ["all", "2"]:
        if not run_phase_2(dry_run=args.dry_run, dataset=args.dataset, skip=args.skip):
            all_success = False
            if args.phase == "2":
                logger.error("Phase 2 completed with errors")

    if args.phase in ["all", "3"]:
        if not run_phase_3(dry_run=args.dry_run, skip=args.skip):
            all_success = False
            if args.phase == "3":
                logger.error("Phase 3 completed with errors")

    # Summary
    logger.info("=" * 80)
    if all_success:
        logger.info("✓ All phases completed successfully!")
    else:
        logger.warning("⚠ Pipeline completed with errors. Check log above.")
    logger.info(f"Log saved to: {log_file}")

    return 0 if all_success else 1


if __name__ == "__main__":
    sys.exit(main())
