"""
01_citibike_unzip.py
Stage: src/01_data_cleaning/01_cleaning/
Purpose: Extract nested .zip files inside the annual subdirectories
         (2020-citibike-tripdata, 2021-citibike-tripdata, etc.).
         Safe to re-run: skips extraction if the target CSV already exists.
"""

import importlib.util
import zipfile
from pathlib import Path
import sys

# ── Config ────────────────────────────────────────────────────────────────────
# parents[0]=01_cleaning  [1]=01_data_cleaning  [2]=src  [3]=Tesi
_cfg_path = Path(__file__).parents[4] / "00_config.py"
_spec = importlib.util.spec_from_file_location("config", _cfg_path)
_mod  = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
cfg = _mod.cfg

RAW_DIR = cfg.raw_citibike / "unzipped"

YEAR_DIRS = [
    "2020-citibike-tripdata",
    "2021-citibike-tripdata",
    "2022-citibike-tripdata",
    "2023-citibike-tripdata",
]


def extract_zip(zip_path: Path) -> tuple[int, int]:
    """
    Extract a zip into the same directory.
    Skips members whose target file already exists.
    Returns (n_extracted, n_skipped).
    """
    extracted = 0
    skipped   = 0
    dest_dir  = zip_path.parent

    with zipfile.ZipFile(zip_path, "r") as zf:
        for member in zf.infolist():
            if "__MACOSX" in member.filename or member.filename.startswith("."):
                continue
            target = dest_dir / member.filename
            if target.exists():
                skipped += 1
                continue
            zf.extract(member, dest_dir)
            extracted += 1

    return extracted, skipped


def main():
    if not RAW_DIR.exists():
        print(f"ERROR: RAW_DIR not found: {RAW_DIR}")
        print(f"       (resolved from cfg.raw_citibike = {cfg.raw_citibike})")
        sys.exit(1)

    print(f"RAW_DIR: {RAW_DIR}\n")

    total_extracted = 0
    total_skipped   = 0

    for year_dir_name in YEAR_DIRS:
        year_dir = RAW_DIR / year_dir_name
        if not year_dir.exists():
            print(f"[warn] Directory not found, skipping: {year_dir}")
            continue

        zip_files = sorted(year_dir.rglob("*.zip"))
        if not zip_files:
            print(f"[info] No zip files in {year_dir_name} — already extracted.")
            continue

        print(f"{'='*60}")
        print(f"Processing {year_dir_name}: {len(zip_files)} zip file(s)")
        print(f"{'='*60}")

        for zp in zip_files:
            try:
                n_ext, n_skip = extract_zip(zp)
                total_extracted += n_ext
                total_skipped   += n_skip
                if n_ext == 0:
                    print(f"  [skip] {zp.name} — all files already exist")
                else:
                    print(f"  [ok]   {zp.name} — extracted {n_ext}, skipped {n_skip}")
            except zipfile.BadZipFile as e:
                print(f"  [ERROR] Bad zip: {zp.name} — {e}")
            except Exception as e:
                print(f"  [ERROR] {zp.name} — {e}")

    print(f"\n{'='*60}")
    print(f"Done.  Extracted: {total_extracted}  |  Already existed: {total_skipped}")
    print(f"{'='*60}")

    print("\nContents after extraction:")
    for year_dir_name in YEAR_DIRS:
        year_dir = RAW_DIR / year_dir_name
        if not year_dir.exists():
            continue
        csvs = list(year_dir.rglob("*.csv"))
        zips = list(year_dir.rglob("*.zip"))
        print(f"  {year_dir_name}: {len(csvs)} CSV(s), {len(zips)} zip(s) still present")


if __name__ == "__main__":
    main()