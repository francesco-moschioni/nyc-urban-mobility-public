"""
01_download_weather_noaa.py
Pipeline stage : 01_data_cleaning / 00_download
Input          : NCEI Global Hourly (ISD) — direct file download, NO token required
Output         : cfg.raw_weather / isd_<station_name>_<YEAR>.csv  (raw, one per station per year)

Direct download URL pattern:
  https://www.ncei.noaa.gov/data/global-hourly/access/{YEAR}/{USAF}{WBAN}.csv

Stations (USAF + WBAN concatenated = 11-char filename):
  central_park  72505394728   (USAF 725053, WBAN 94728)
  laguardia     72503014732   (USAF 725030, WBAN 14732)
  jfk           74486094789   (USAF 744860, WBAN 94789)

Usage:
  python src/01_data_cleaning/00_download/01_download_weather_noaa.py

Requirements:
  pip install requests tqdm
  (no token, no dotenv needed)
"""

import importlib.util
import time
from pathlib import Path

import requests
from tqdm import tqdm

# ── 0. Load config ────────────────────────────────────────────────────────────
_cfg_path = Path(__file__).parents[3] / "00_config.py"
_spec = importlib.util.spec_from_file_location("config", _cfg_path)
_mod  = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
cfg = _mod.cfg

# ── 1. Parameters ─────────────────────────────────────────────────────────────
BASE_URL = "https://www.ncei.noaa.gov/data/global-hourly/access"

STATIONS = {
    "central_park": "72505394728",
    "laguardia":    "72503014732",
    "jfk":          "74486094789",
}

START_YEAR = 2021
END_YEAR   = 2025   # inclusive

# ── 2. Download ───────────────────────────────────────────────────────────────

def fetch_year(station_name: str, station_id: str, year: int, out_dir: Path) -> bool:
    out_path = out_dir / f"isd_{station_name}_{year}.csv"

    if out_path.exists() and out_path.stat().st_size > 0:
        print(f"    [skip] {out_path.name} already exists")
        return True

    url = f"{BASE_URL}/{year}/{station_id}.csv"

    for attempt in range(1, 4):
        try:
            resp = requests.get(url, timeout=120, stream=True)
            resp.raise_for_status()

            with open(out_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=1024 * 256):
                    f.write(chunk)

            size_mb = out_path.stat().st_size / 1_048_576
            n_lines = sum(1 for _ in open(out_path, encoding="utf-8")) - 1
            print(f"    [ok]   {out_path.name}  ({n_lines:,} rows, {size_mb:.1f} MB)")
            return True

        except requests.HTTPError as e:
            print(f"    [err]  attempt {attempt}/3 -- {e}  url={url}")
            time.sleep(3 * attempt)
        except requests.RequestException as e:
            print(f"    [err]  attempt {attempt}/3 -- {e}")
            time.sleep(3 * attempt)

    # remove partial file if exists
    if out_path.exists():
        out_path.unlink()
    print(f"    [FAIL] {station_name} {year} -- skipped after 3 attempts")
    return False


# ── 3. Main ───────────────────────────────────────────────────────────────────

def main():
    out_dir = Path(cfg.raw_weather)
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"Output directory: {out_dir}\n")

    results = {}

    for name, station_id in STATIONS.items():
        print(f"\n{'='*55}")
        print(f"Station: {name}  (id: {station_id})")
        print(f"{'='*55}")

        station_results = {}
        for year in tqdm(range(START_YEAR, END_YEAR + 1), desc=name, unit="yr"):
            ok = fetch_year(name, station_id, year, out_dir)
            station_results[year] = ok
            time.sleep(0.3)

        results[name] = station_results

    print(f"\n{'='*55}")
    print("Download summary")
    print(f"{'='*55}")
    for name, years in results.items():
        ok_years   = [y for y, v in years.items() if v]
        fail_years = [y for y, v in years.items() if not v]
        print(f"  {name:15s}  ok={ok_years}  failed={fail_years if fail_years else 'none'}")

    print(f"\nRaw CSVs saved to: {out_dir}")


if __name__ == "__main__":
    main()