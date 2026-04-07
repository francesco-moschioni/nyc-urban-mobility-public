"""
01_download_citibike.py
=======================
Pipeline stage : 01_data_cleaning (pre-step — raw download)
Input          : S3 bucket https://s3.amazonaws.com/tripdata/
Output         : cfg.raw_citibike / zipped/    ← original .zip files
                 cfg.raw_citibike / unzipped/  ← extracted CSV/Parquet files

Logic
-----
1. Fetch the S3 bucket index and parse ALL available .zip filenames dynamically.
2. Skip files already present in the zipped/ folder (resume-safe).
3. Download all missing zips first.
4. Unzip everything at the end (also skips already-extracted files).

Run from any working directory — config loaded via importlib.
"""

import importlib.util
import sys
import time
import zipfile
from pathlib import Path

import requests
from tqdm import tqdm
from xml.etree import ElementTree as ET

# ── 0. Load project config ────────────────────────────────────────────────────

def load_cfg(script_path: Path, levels_up: int = 3):
    """Walk up `levels_up` directories from script_path to find 00_config.py."""
    cfg_path = script_path.parents[levels_up - 1] / "00_config.py"
    if not cfg_path.exists():
        raise FileNotFoundError(
            f"Could not find 00_config.py at {cfg_path}. "
            "Adjust levels_up or run from the correct directory."
        )
    spec = importlib.util.spec_from_file_location("config", cfg_path)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.cfg

cfg = load_cfg(Path(__file__), levels_up=3)  # src/01_data_cleaning/ → Tesi/

# ── 1. Destination folders ────────────────────────────────────────────────────

DIR_ZIPPED   = Path(cfg.raw_citibike) / "zipped"
DIR_UNZIPPED = Path(cfg.raw_citibike) / "unzipped"
DIR_ZIPPED.mkdir(parents=True, exist_ok=True)
DIR_UNZIPPED.mkdir(parents=True, exist_ok=True)

# ── 2. Constants ──────────────────────────────────────────────────────────────

BUCKET_URL   = "https://s3.amazonaws.com/tripdata/"
INDEX_URL    = f"{BUCKET_URL}?list-type=2"   # S3 ListObjectsV2 XML API
CHUNK_SIZE   = 1024 * 1024                   # 1 MB chunks for streaming download
MAX_RETRIES  = 3
RETRY_DELAY  = 5                             # seconds between retries

# ── 3. Discover available zip files from S3 ───────────────────────────────────

def fetch_all_zip_keys(index_url: str) -> list[str]:
    """
    Query the S3 bucket listing API (XML) and return all keys ending in .zip.
    Handles S3 pagination via ContinuationToken.
    """
    keys = []
    continuation_token = None
    page = 1

    while True:
        url = index_url
        if continuation_token:
            url += f"&continuation-token={requests.utils.quote(continuation_token)}"

        print(f"  Fetching bucket index (page {page})…")
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()

        # Parse XML — S3 uses namespace
        ns  = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
        root = ET.fromstring(resp.text)

        for content in root.findall("s3:Contents", ns):
            key = content.find("s3:Key", ns).text
            if key.endswith(".zip"):
                keys.append(key)

        # Check for more pages
        is_truncated = root.findtext("s3:IsTruncated", namespaces=ns)
        if is_truncated and is_truncated.lower() == "true":
            continuation_token = root.findtext(
                "s3:NextContinuationToken", namespaces=ns
            )
            page += 1
        else:
            break

    return sorted(keys)


# ── 4. Download helpers ───────────────────────────────────────────────────────

def download_file(url: str, dest: Path, retries: int = MAX_RETRIES) -> bool:
    """
    Stream-download `url` to `dest` with a progress bar.
    Writes to a .tmp file first; renames on success (atomic).
    Returns True on success, False on permanent failure.
    """
    tmp = dest.with_suffix(dest.suffix + ".tmp")

    for attempt in range(1, retries + 1):
        try:
            with requests.get(url, stream=True, timeout=60) as r:
                r.raise_for_status()
                total = int(r.headers.get("content-length", 0))
                with open(tmp, "wb") as f, tqdm(
                    total=total,
                    unit="B",
                    unit_scale=True,
                    unit_divisor=1024,
                    desc=dest.name,
                    leave=False,
                ) as bar:
                    for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                        f.write(chunk)
                        bar.update(len(chunk))

            tmp.rename(dest)
            return True

        except Exception as exc:
            print(f"    ⚠ Attempt {attempt}/{retries} failed for {dest.name}: {exc}")
            if tmp.exists():
                tmp.unlink()
            if attempt < retries:
                time.sleep(RETRY_DELAY)

    print(f"    ✗ Permanently failed: {dest.name}")
    return False


# ── 5. Unzip helpers ──────────────────────────────────────────────────────────

def unzip_file(zip_path: Path, dest_dir: Path) -> bool:
    """
    Extract all members of `zip_path` into `dest_dir`.
    Skips members that already exist (resume-safe at file level).
    Returns True on success.
    """
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            members = zf.namelist()
            to_extract = [
                m for m in members
                if not (dest_dir / m).exists()
            ]
            if not to_extract:
                return True  # already fully extracted

            for member in tqdm(to_extract, desc=zip_path.stem, leave=False):
                zf.extract(member, dest_dir)
        return True

    except zipfile.BadZipFile:
        print(f"    ✗ Bad zip file, skipping: {zip_path.name}")
        return False
    except Exception as exc:
        print(f"    ✗ Error unzipping {zip_path.name}: {exc}")
        return False


# ── 6. Main pipeline ──────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  Citi Bike — Raw Data Download")
    print("=" * 60)

    # 6a. Discover all zip keys in the bucket
    print("\n[1/3] Discovering files in S3 bucket…")
    all_keys = fetch_all_zip_keys(INDEX_URL)
    print(f"  Found {len(all_keys)} zip file(s) in bucket.")

    # 6b. Determine which files still need to be downloaded
    print("\n[2/3] Checking already-downloaded files…")
    to_download = []
    already_have = []

    for key in all_keys:
        fname = Path(key).name          # strip any S3 prefix folders
        dest  = DIR_ZIPPED / fname
        if dest.exists() and dest.stat().st_size > 0:
            already_have.append(fname)
        else:
            to_download.append((key, dest))

    print(f"  Already present : {len(already_have)} file(s)  [skipped]")
    print(f"  To download     : {len(to_download)} file(s)")

    # 6c. Download missing files
    failed_downloads = []
    if to_download:
        print(f"\n  Downloading {len(to_download)} file(s)…\n")
        for i, (key, dest) in enumerate(to_download, 1):
            url = BUCKET_URL + key
            print(f"  [{i}/{len(to_download)}] {dest.name}")
            ok = download_file(url, dest)
            if not ok:
                failed_downloads.append(dest.name)
    else:
        print("  Nothing to download.")

    # 6d. Summary of downloads
    if failed_downloads:
        print(f"\n  ⚠ {len(failed_downloads)} download(s) failed:")
        for f in failed_downloads:
            print(f"    – {f}")

    # 6e. Unzip all successfully downloaded files
    print(f"\n[3/3] Unzipping files to {DIR_UNZIPPED}…\n")
    zip_files = sorted(DIR_ZIPPED.glob("*.zip"))
    failed_unzips = []

    for i, zp in enumerate(zip_files, 1):
        print(f"  [{i}/{len(zip_files)}] {zp.name}")
        ok = unzip_file(zp, DIR_UNZIPPED)
        if not ok:
            failed_unzips.append(zp.name)

    # 6f. Final report
    print("\n" + "=" * 60)
    print("  Done.")
    print(f"  Zipped files   → {DIR_ZIPPED}")
    print(f"  Unzipped files → {DIR_UNZIPPED}")
    print(f"  Downloads OK   : {len(all_keys) - len(failed_downloads)}/{len(all_keys)}")
    if failed_downloads:
        print(f"  Downloads FAIL : {len(failed_downloads)}")
    if failed_unzips:
        print(f"  Unzip FAIL     : {len(failed_unzips)}")
    print("=" * 60)

    # Exit with error code if anything failed
    if failed_downloads or failed_unzips:
        sys.exit(1)


if __name__ == "__main__":
    main()
