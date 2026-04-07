"""
01_data_cleaning/download_traffic.py
======================================
Downloads NYC Traffic datasets from the Socrata bulk endpoint:
    https://data.cityofnewyork.us/api/views/{id}/rows.csv?accessType=DOWNLOAD

Modeled after download_mta.py — same resume support, backoff, and meta logic.

Dependencies:
    pip install requests tqdm python-dotenv

Token expected in the project .env (project root):
    SOCRATA_APP_TOKEN=xxxx
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

import requests
from dotenv import load_dotenv
from requests.exceptions import RequestException
from tqdm import tqdm

# =========================
# PROJECT CONFIG
# =========================
import importlib.util

PROJECT_ROOT    = Path(__file__).parents[2].resolve()   # → D:\Tesi
SCRIPT_DIR      = Path(__file__).parent.resolve()        # → D:\Tesi\src\01_data_cleaning

_cfg_path = PROJECT_ROOT / "00_config.py"
_spec     = importlib.util.spec_from_file_location("config", _cfg_path)
_mod      = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
cfg = _mod.cfg

# =========================
# LOAD CREDENTIALS
# =========================
load_dotenv(SCRIPT_DIR / ".env.txt")

APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN")
if not APP_TOKEN:
    raise EnvironmentError(
        "SOCRATA_APP_TOKEN not found.\n"
        f"Add to {PROJECT_ROOT / '.env'}:\n"
        "    SOCRATA_APP_TOKEN=<your_token>"
    )

# =========================
# CONFIG
# =========================
DOMAIN   = "https://data.cityofnewyork.us"
OUT_DIR  = cfg.raw_traffic      # → D:\Tesi\data\raw\traffic
OUT_DIR.mkdir(parents=True, exist_ok=True)

CHUNK_SIZE       = 1024 * 1024   # 1 MB per stream chunk
TIMEOUT_S        = 60            # initial connection timeout
RETRY_MAX_WAIT_S = 120           # exponential backoff cap

DATASETS = {
    "NYC Automated Traffic Volume Counts": "7ym2-wayt",
}

# =========================
# HELPERS — FILE / META
# =========================
def safe_filename(name: str) -> str:
    for ch in '<>:"/\\|?*':
        name = name.replace(ch, "_")
    return " ".join(name.split()).strip()


def meta_path_for(out_file: Path) -> Path:
    return out_file.with_suffix(out_file.suffix + ".meta.json")


def load_meta(meta_file: Path) -> dict | None:
    if not meta_file.exists():
        return None
    try:
        return json.loads(meta_file.read_text(encoding="utf-8"))
    except Exception:
        return None


def save_meta(meta_file: Path, meta: dict) -> None:
    meta_file.write_text(
        json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def delete_meta(meta_file: Path) -> None:
    """Remove the meta file once the dataset is fully downloaded."""
    try:
        if meta_file.exists():
            meta_file.unlink()
    except Exception as e:
        print(f"    [warning] could not delete meta file {meta_file.name}: {e}")


def bulk_url(dataset_id: str) -> str:
    return f"{DOMAIN}/api/views/{dataset_id}/rows.csv?accessType=DOWNLOAD"


# =========================
# PRE-SCAN
# =========================
def scan_existing(datasets: dict[str, str]) -> None:
    """
    Print a summary of what is already present in OUT_DIR before downloading.
    Three states per dataset:
      ✔ complete   — CSV present, no meta file (already cleaned up)
      ⟳ partial    — CSV present with meta file (interrupted download)
      ✗ missing    — no CSV found
    """
    print(f"\n{'─'*80}")
    print(f"{'DATASET':<60} {'STATUS':>18}")
    print(f"{'─'*80}")

    for name, dsid in datasets.items():
        fname     = f"{safe_filename(name)}__{dsid}.csv"
        out_file  = OUT_DIR / fname
        meta_file = meta_path_for(out_file)

        if out_file.exists() and not meta_file.exists():
            size   = out_file.stat().st_size / 1024**2
            status = f"✔ complete ({size:.0f} MB)"
        elif out_file.exists() and meta_file.exists():
            size   = out_file.stat().st_size / 1024**2
            status = f"⟳ partial  ({size:.0f} MB)"
        else:
            status = "✗ missing"

        label = name if len(name) <= 58 else name[:55] + "..."
        print(f"  {label:<58} {status:>20}")

    print(f"{'─'*80}\n")


# =========================
# HELPERS — NETWORK
# =========================
def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"X-App-Token": APP_TOKEN})
    return s


def get_remote_size(session: requests.Session, url: str) -> int | None:
    """HEAD request to retrieve Content-Length (total size in bytes)."""
    try:
        r = session.head(url, timeout=TIMEOUT_S, allow_redirects=True)
        cl = r.headers.get("Content-Length")
        return int(cl) if cl else None
    except Exception:
        return None


def supports_range(session: requests.Session, url: str) -> bool:
    """Check whether the server accepts Range requests (for resume support)."""
    try:
        r = session.head(url, timeout=TIMEOUT_S, allow_redirects=True)
        return r.headers.get("Accept-Ranges", "none").lower() != "none"
    except Exception:
        return False


def stream_request_with_backoff(
    session: requests.Session,
    url: str,
    resume_from: int = 0,
) -> requests.Response:
    """
    Streaming GET with exponential backoff on network errors / 5xx / 429.
    If resume_from > 0, adds the Range header to resume the download.
    """
    wait    = 1
    attempt = 0
    headers = {}
    if resume_from > 0:
        headers["Range"] = f"bytes={resume_from}-"

    while True:
        attempt += 1
        try:
            r = session.get(
                url,
                headers=headers,
                timeout=TIMEOUT_S,
                stream=True,
                allow_redirects=True,
            )
        except RequestException as e:
            print(f"\n    [network] attempt {attempt}: {e}. Retrying in {wait}s...")
            time.sleep(wait)
            wait = min(wait * 2, RETRY_MAX_WAIT_S)
            continue

        if r.status_code == 429:
            retry_after = int(r.headers.get("Retry-After", wait))
            print(f"\n    [429] Waiting {retry_after}s...")
            time.sleep(retry_after)
            wait = min(wait * 2, RETRY_MAX_WAIT_S)
            continue

        if r.status_code in (500, 502, 503, 504):
            print(f"\n    [HTTP {r.status_code}] attempt {attempt}. Retrying in {wait}s...")
            time.sleep(wait)
            wait = min(wait * 2, RETRY_MAX_WAIT_S)
            continue

        # 206 Partial Content = resume OK, 200 = normal download
        if r.status_code not in (200, 206):
            raise RuntimeError(
                f"HTTP {r.status_code} — {url}\n"
                f"Body: {r.text[:500]}"
            )

        return r


# =========================
# MAIN DOWNLOAD
# =========================
def download_dataset(dataset_id: str, out_file: Path) -> int:
    """
    Downloads a full dataset via bulk download URL.

    - If CSV exists with no meta  → already complete, skip
    - If CSV exists with meta     → partial download, resume
    - If nothing exists           → fresh download
    - On completion: CSV kept, meta file deleted
    """
    meta_file = meta_path_for(out_file)
    url       = bulk_url(dataset_id)

    # --- skip if CSV present and no meta (clean completed state) ---
    if out_file.exists() and not meta_file.exists():
        size = out_file.stat().st_size
        print(f"    ✔ already complete ({size / 1024**2:.1f} MB) → skip")
        return size

    session    = make_session()
    total_size = get_remote_size(session, url)   # may be None
    range_ok   = supports_range(session, url)

    # --- determine resume offset ---
    resume_from = 0
    if out_file.exists() and range_ok:
        resume_from = out_file.stat().st_size
        if resume_from > 0:
            print(f"    Resuming from {resume_from / 1024**2:.1f} MB")
    elif out_file.exists() and not range_ok:
        print("    Server does not support Range → restarting from scratch")
        out_file.unlink()
        resume_from = 0

    # --- initialise meta ---
    meta = {
        "dataset_id":       dataset_id,
        "url":              url,
        "total_bytes":      total_size,
        "bytes_downloaded": resume_from,
        "completed":        False,
        "updated_at_utc":   None,
    }
    save_meta(meta_file, meta)

    # --- stream download ---
    r         = stream_request_with_backoff(session, url, resume_from=resume_from)
    file_mode = "ab" if resume_from > 0 else "wb"
    bytes_done = resume_from

    pbar = tqdm(
        total=total_size,
        initial=resume_from,
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
        desc=dataset_id,
        dynamic_ncols=True,
        smoothing=0.1,
    )

    with out_file.open(file_mode) as f:
        for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
            if not chunk:
                continue
            f.write(chunk)
            bytes_done += len(chunk)
            pbar.update(len(chunk))

            # update meta on every chunk (reliable resume)
            meta["bytes_downloaded"] = bytes_done
            meta["updated_at_utc"]   = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            save_meta(meta_file, meta)

    pbar.close()

    # --- cleanup: delete meta now that download is complete ---
    delete_meta(meta_file)

    return bytes_done


# =========================
# UI — CONSOLE MENU
# =========================
def print_menu(items: list[tuple[str, str]]) -> None:
    print("\nWhat do you want to download?")
    print("  0) Exit")
    print("  A) Download ALL")
    print("  M) Multiple selection (e.g.: 1,3,5-7)")
    print("-" * 80)
    for i, (name, dsid) in enumerate(items, start=1):
        print(f"  {i:2d}) {name}  [{dsid}]")


def parse_multi_selection(s: str, max_n: int) -> list[int]:
    chosen: set[int] = set()
    for part in s.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            a, b = (int(x.strip()) for x in part.split("-", 1))
            if a > b:
                a, b = b, a
            chosen.update(k for k in range(a, b + 1) if 1 <= k <= max_n)
        else:
            k = int(part)
            if 1 <= k <= max_n:
                chosen.add(k)
    return sorted(chosen)


def main() -> None:
    print(f"Output  →  {OUT_DIR}")
    items = list(DATASETS.items())

    # always show what's already on disk before asking what to download
    scan_existing(DATASETS)

    while True:
        print_menu(items)
        choice = input("\nChoice: ").strip().lower()

        if not choice:
            continue
        if choice == "0":
            print("Bye.")
            return

        if choice == "a":
            selected = list(range(1, len(items) + 1))
        elif choice == "m":
            s = input("Enter selection (e.g.: 1,3,5-7): ").strip()
            selected = parse_multi_selection(s, len(items))
            if not selected:
                print("Empty or invalid selection.")
                continue
        else:
            try:
                k = int(choice)
            except ValueError:
                print("Invalid input. Use 0, A, M or a number.")
                continue
            if not (1 <= k <= len(items)):
                print("Number out of range.")
                continue
            selected = [k]

        for idx in selected:
            name, dsid = items[idx - 1]
            fname    = f"{safe_filename(name)}__{dsid}.csv"
            out_file = OUT_DIR / fname

            print(f"\n{'='*80}")
            print(f"Dataset : {name}")
            print(f"ID      : {dsid}")
            print(f"URL     : {bulk_url(dsid)}")
            print(f"Output  : {out_file}")
            print(f"{'='*80}")

            try:
                n = download_dataset(dsid, out_file)
                print(f"✓ Completed: {n / 1024**2:.1f} MB downloaded")
            except Exception as e:
                print(f"✗ Error on {dsid}: {e}")

        print("\nOperation completed.")


if __name__ == "__main__":
    main()