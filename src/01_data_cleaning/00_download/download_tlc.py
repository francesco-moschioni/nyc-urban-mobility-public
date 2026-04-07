from __future__ import annotations

"""
01_data_cleaning/download_tlc.py
=================================
Downloads NYC TLC trip data from CloudFront (monthly parquet files).
Anti-ban strategy: long pauses, no HEAD requests, aggressive backoff
at the first sign of throttling.

Output: <cfg.raw_tlc>/<type>/<type>_tripdata_<year>-<mm>.parquet
"""

import datetime
import io
import json
import os
import random
import sys
import time
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# =========================
# PROJECT CONFIG
# =========================
import importlib.util

PROJECT_ROOT = Path(__file__).parents[3].resolve()

_cfg_path = PROJECT_ROOT / "00_config.py"
_spec     = importlib.util.spec_from_file_location("config", _cfg_path)
_mod      = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
cfg = _mod.cfg

# ============================================================
# CONFIG
# ============================================================
OUT_ROOT      = cfg.raw_tlc
PROGRESS_PATH = cfg.raw_tlc / "_progress_v5.json"
LOG_PATH      = cfg.raw_tlc / "_download.log"
REPORT_PATH   = cfg.raw_tlc / "_data_description.txt"

CF_BASE         = "https://d37ci6vzurychx.cloudfront.net/trip-data"
CF_MISC         = "https://d37ci6vzurychx.cloudfront.net/misc"
ZONE_LOOKUP_URL = f"{CF_MISC}/taxi_zone_lookup.csv"
ZONE_LOOKUP_OUT = cfg.external / "nyc_zones" / "taxi_zone_lookup.csv"
ZONE_SHP_URL    = f"{CF_MISC}/taxi_zones.zip"
ZONE_SHP_DIR    = cfg.external / "nyc_zones"   # i 5 file .shp/.dbf/... vanno qui
CANARY_URL      = f"{CF_BASE}/yellow_tripdata_2023-01.parquet"

TYPES = ("yellow", "green", "fhv", "fhvhv")

SLEEP_MIN = 30
SLEEP_MAX = 60

PAUSE_EVERY_N   = 5
PAUSE_EXTRA_MIN = 3 * 60
PAUSE_EXTRA_MAX = 5 * 60

THROTTLE_WAIT_MIN = 20 * 60
THROTTLE_WAIT_MAX = 40 * 60

FIRST_YEAR: Dict[str, int] = {
    "yellow": 2009,
    "green":  2013,
    "fhv":    2015,
    "fhvhv":  2019,
}
LAST_YEAR = 2025

# File che compongono lo shapefile (usati per check "già presente")
SHAPEFILE_PARTS = (
    "taxi_zones.shp",
    "taxi_zones.dbf",
    "taxi_zones.shx",
    "taxi_zones.prj",
    "taxi_zones.cpg",
)


# ============================================================
# Helpers
# ============================================================
def human_bytes(n: Optional[int]) -> str:
    if n is None:
        return "?"
    units = ["B", "KB", "MB", "GB", "TB"]
    x = float(n)
    for u in units[:-1]:
        if x < 1024:
            return f"{x:.2f} {u}"
        x /= 1024
    return f"{x:.2f} TB"


def print_progress(prefix: str, done: int, total: Optional[int]) -> None:
    bar_len = 30
    if total and total > 0:
        frac = min(max(done / total, 0.0), 1.0)
        filled = int(bar_len * frac)
        bar = "#" * filled + "-" * (bar_len - filled)
        pct = int(frac * 100)
        msg = f"{prefix} [{bar}] {pct:3d}%  {human_bytes(done)} / {human_bytes(total)}"
    else:
        spinner = "-\\|/"[(done // (512 * 1024)) % 4]
        msg = f"{prefix} {spinner}  {human_bytes(done)} / ?"
    sys.stdout.write("\r" + msg + " " * 8)
    sys.stdout.flush()


def sleep_log(seconds: float, reason: str) -> None:
    print(f"[WAIT] {reason} -- waiting {seconds:.0f}s ...")
    time.sleep(seconds)


# ============================================================
# Job
# ============================================================
@dataclass(frozen=True)
class Job:
    t: str
    year: int
    month: int

    @property
    def fname(self) -> str:
        return f"{self.t}_tripdata_{self.year}-{self.month:02d}.parquet"

    @property
    def url(self) -> str:
        return f"{CF_BASE}/{self.fname}"

    def out_path(self, root: Path) -> Path:
        return root / self.t / self.fname

    @property
    def key(self) -> str:
        return f"{self.t}:{self.year:04d}-{self.month:02d}"


def iter_jobs(years: Iterable[int], months: Iterable[int]) -> List[Job]:
    jobs = []
    for t in TYPES:
        first = FIRST_YEAR[t]
        for y in years:
            if y < first:
                continue
            for m in months:
                jobs.append(Job(t=t, year=y, month=m))
    return jobs


# ============================================================
# Progress
# ============================================================
class Progress:
    def __init__(self, path: Path):
        self.path = path
        self.data: Dict[str, str] = self._load()

    def _load(self) -> Dict[str, str]:
        if self.path.exists():
            try:
                return json.loads(self.path.read_text(encoding="utf-8"))
            except Exception:
                return {}
        return {}

    def get(self, key: str) -> Optional[str]:
        return self.data.get(key)

    def set(self, key: str, status: str) -> None:
        self.data[key] = status
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.data, indent=2, sort_keys=True), encoding="utf-8")
        os.replace(tmp, self.path)

    def _parse_attempts(self, status: str) -> int:
        parts = status.split(":attempts=")
        if len(parts) == 2:
            try:
                return int(parts[1])
            except Exception:
                pass
        return 0

    def should_attempt(self, key: str, max_attempts: int) -> bool:
        prev = self.get(key)
        if prev is None:
            return True
        if prev in ("ok", "missing"):
            return False
        if prev.startswith("error:"):
            return self._parse_attempts(prev) < max_attempts
        return True

    def inc_error(self, key: str, code: str) -> int:
        prev = self.get(key)
        attempts = self._parse_attempts(prev) if prev else 0
        attempts += 1
        self.set(key, f"error:{code}:attempts={attempts}")
        return attempts

    def summary(self) -> Dict[str, int]:
        out: Dict[str, int] = {
            "ok": 0, "missing": 0, "error": 0, "recorded": len(self.data)
        }
        for v in self.data.values():
            if v == "ok":                    out["ok"] += 1
            elif v == "missing":             out["missing"] += 1
            elif v.startswith("error:"):     out["error"] += 1
        return out


# ============================================================
# Logger
# ============================================================
class Logger:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._f = open(path, "a", encoding="utf-8", buffering=1)
        self._w(f"\n{'='*70}")
        self._w(f"SESSION START  {datetime.datetime.now():%Y-%m-%d %H:%M:%S}")
        self._w(f"{'='*70}")

    def _w(self, msg: str) -> None:
        ts = datetime.datetime.now().strftime("%H:%M:%S")
        self._f.write(f"[{ts}] {msg}\n")

    def ok(self, job: Job, size: Optional[int]) -> None:
        self._w(f"OK       {job.fname:<52} {human_bytes(size):>10}")

    def missing(self, job: Job) -> None:
        self._w(f"MISSING  {job.fname}")

    def error(self, job: Job, code: str, attempt: int, max_a: int) -> None:
        self._w(f"ERROR    {job.fname:<52} code={code}  attempt={attempt}/{max_a}")

    def throttle(self, wait_s: float) -> None:
        self._w(f"THROTTLE IP blocked -- waiting {wait_s:.0f}s")

    def info(self, msg: str) -> None:
        self._w(f"INFO     {msg}")

    def close(self, summary: Dict[str, int]) -> None:
        self._w(f"SESSION END  OK={summary['ok']}  MISSING={summary['missing']}  "
                f"ERRORS={summary['error']}  TOTAL={summary['recorded']}")
        self._w(f"{'='*70}\n")
        self._f.close()


# ============================================================
# HTTP
# ============================================================
def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=2.0,
        status_forcelist=(500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=1, pool_maxsize=1)
    s.mount("https://", adapter)
    s.headers.update({
        "User-Agent":                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36 Edg/145.0",
        "Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language":           "it,it-IT;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6,fr;q=0.5",
        "Accept-Encoding":           "gzip, deflate, br, zstd",
        "Referer":                   "https://www.nyc.gov/",
        "DNT":                       "1",
        "Priority":                  "u=0, i",
        "sec-ch-ua":                 '"Not:A-Brand";v="99", "Microsoft Edge";v="145", "Chromium";v="145"',
        "sec-ch-ua-mobile":          "?0",
        "sec-ch-ua-platform":        '"Windows"',
        "sec-fetch-dest":            "document",
        "sec-fetch-mode":            "navigate",
        "sec-fetch-site":            "cross-site",
        "sec-fetch-user":            "?1",
        "Upgrade-Insecure-Requests": "1",
    })
    return s


def is_throttled(session: requests.Session) -> bool:
    try:
        r = session.get(CANARY_URL, headers={"Range": "bytes=0-0"},
                        timeout=(10, 15), stream=True)
        r.close()
        return r.status_code == 403
    except Exception:
        return True


# ============================================================
# Support files: zone lookup CSV + shapefile
# ============================================================
def download_zone_lookup(session: requests.Session) -> None:
    """Scarica taxi_zone_lookup.csv. Salta se già presente."""
    out = ZONE_LOOKUP_OUT
    out.parent.mkdir(parents=True, exist_ok=True)

    if out.exists() and out.stat().st_size > 0:
        print(f"[ZONES] lookup CSV already present → {out}")
        return

    print(f"[ZONES] Downloading zone lookup CSV ...")
    print(f"        URL : {ZONE_LOOKUP_URL}")
    print(f"        OUT : {out}")

    try:
        r = session.get(ZONE_LOOKUP_URL, timeout=(20, 60))
        if r.status_code != 200:
            print(f"[ZONES] ERROR: HTTP {r.status_code}")
            return
        out.write_bytes(r.content)
        print(f"[ZONES] Done ({human_bytes(out.stat().st_size)})")
    except Exception as e:
        print(f"[ZONES] ERROR: {e}")


def download_zone_shapefile(session: requests.Session) -> None:
    """
    Scarica taxi_zones.zip da CloudFront ed estrae i 5 file
    (.shp, .dbf, .shx, .prj, .cpg) direttamente in ZONE_SHP_DIR.
    Salta se tutti i file sono già presenti.

    I file estratti sono usati per join spaziali con i dati TLC
    tramite la colonna LocationID (PULocationID / DOLocationID).
    """
    ZONE_SHP_DIR.mkdir(parents=True, exist_ok=True)

    # Controlla se tutti i file sono già presenti
    already_present = all(
        (ZONE_SHP_DIR / f).exists() and (ZONE_SHP_DIR / f).stat().st_size > 0
        for f in SHAPEFILE_PARTS
    )
    if already_present:
        print(f"[ZONES] Shapefile already present → {ZONE_SHP_DIR}")
        return

    print(f"[ZONES] Downloading taxi zones shapefile ...")
    print(f"        URL : {ZONE_SHP_URL}")
    print(f"        OUT : {ZONE_SHP_DIR}/")

    try:
        r = session.get(ZONE_SHP_URL, timeout=(30, 120))
        if r.status_code != 200:
            print(f"[ZONES] ERROR: HTTP {r.status_code}")
            return

        # Estrai lo zip in memoria, scrivi solo i file shapefile attesi
        with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
            extracted = []
            for member in zf.namelist():
                # Prendi solo i file con estensione attesa, ignora cartelle
                fname = Path(member).name
                if not fname or Path(member).suffix.lower() not in \
                        {".shp", ".dbf", ".shx", ".prj", ".cpg"}:
                    continue
                # Rinomina sempre in taxi_zones.<ext> per coerenza
                ext     = Path(fname).suffix.lower()
                out_name = f"taxi_zones{ext}"
                out_path = ZONE_SHP_DIR / out_name
                out_path.write_bytes(zf.read(member))
                extracted.append(f"{out_name} ({human_bytes(out_path.stat().st_size)})")

        if extracted:
            print(f"[ZONES] Extracted {len(extracted)} files:")
            for f in extracted:
                print(f"        → {f}")
        else:
            print("[ZONES] WARNING: no shapefile parts found inside the zip")

    except zipfile.BadZipFile:
        print("[ZONES] ERROR: downloaded file is not a valid zip")
    except Exception as e:
        print(f"[ZONES] ERROR: {e}")


# ============================================================
# Download singolo file parquet
# ============================================================
def download_one(
    session: requests.Session,
    job: Job,
    root: Path,
) -> Tuple[str, Optional[int]]:
    out_path = job.out_path(root)
    tmp_path = out_path.with_suffix(out_path.suffix + ".part")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if out_path.exists() and out_path.stat().st_size > 0:
        return ("ok", out_path.stat().st_size)

    resume_from = tmp_path.stat().st_size if tmp_path.exists() else 0
    req_headers: Dict[str, str] = {}
    if resume_from > 0:
        req_headers["Range"] = f"bytes={resume_from}-"

    try:
        with session.get(job.url, stream=True, timeout=(20, 180),
                         headers={**req_headers, "Accept-Encoding": "identity"}) as resp:

            if resp.status_code == 403:
                return ("throttled", None)
            if resp.status_code == 404:
                tmp_path.unlink(missing_ok=True)
                return ("missing", None)
            if resp.status_code == 416:
                os.replace(tmp_path, out_path)
                return ("ok", out_path.stat().st_size)
            if resp.status_code >= 400:
                return (f"error:{resp.status_code}", None)

            total_size: Optional[int] = None
            cl = resp.headers.get("Content-Length")
            if cl:
                try:
                    extra = resume_from if resp.status_code == 206 else 0
                    total_size = int(cl) + extra
                except Exception:
                    pass

            mode = "ab" if (resume_from > 0 and resp.status_code == 206) else "wb"
            done = resume_from if mode == "ab" else 0

            with open(tmp_path, mode) as f:
                for chunk in resp.iter_content(chunk_size=512 * 1024):
                    if chunk:
                        f.write(chunk)
                        done += len(chunk)
                        print_progress(
                            f"{job.t} {job.year}-{job.month:02d}",
                            done, total_size,
                        )

        sys.stdout.write("\n")
        sys.stdout.flush()

    except requests.exceptions.Timeout:
        return ("error:timeout", None)
    except requests.exceptions.ConnectionError:
        return ("error:connection", None)
    except Exception:
        return ("error:exception", None)

    if not tmp_path.exists() or tmp_path.stat().st_size == 0:
        return ("error:empty", None)

    os.replace(tmp_path, out_path)
    return ("ok", out_path.stat().st_size)


# ============================================================
# Riconciliazione disco -> progress
# ============================================================
def reconcile_disk(prog: Progress, all_jobs: List[Job], out_root: Path) -> int:
    added = 0
    print("[RECONCILE] Scanning files already present on disk...")
    fname_to_job = {j.fname: j for j in all_jobs}
    for t in TYPES:
        folder = out_root / t
        if not folder.exists():
            continue
        for f in sorted(folder.glob("*.parquet")):
            if f.stat().st_size == 0:
                continue
            job = fname_to_job.get(f.name)
            if job is None or prog.get(job.key) == "ok":
                continue
            prog.set(job.key, "ok")
            added += 1
            print(f"  [FOUND] {f.name}  ({human_bytes(f.stat().st_size)})")
    if added == 0:
        print("[RECONCILE] No new files found on disk.")
    else:
        print(f"[RECONCILE] {added} files registered in progress.\n")
    return added


# ============================================================
# Report
# ============================================================
MONTH_NAMES = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
               "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def write_report(
    progress: Progress,
    all_jobs: List[Job],
    out_root: Path,
    report_path: Path,
) -> None:
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    by_type: Dict[str, List[Job]] = {t: [] for t in TYPES}
    for j in all_jobs:
        by_type[j.t].append(j)

    lines: List[str] = [
        "=" * 72,
        "  NYC TLC TRIP DATA -- DATA DESCRIPTION",
        f"  Generated : {now}",
        f"  Root      : {out_root}",
        f"  Source    : CloudFront TLC  (d37ci6vzurychx.cloudfront.net)",
        f"  Format    : Monthly Parquet",
        "=" * 72,
        "",
        "  SUPPORT FILES",
        "-" * 72,
    ]

    # Stato file di supporto
    for label, path in [
        ("Zone lookup CSV ", ZONE_LOOKUP_OUT),
        ("Shapefile .shp  ", ZONE_SHP_DIR / "taxi_zones.shp"),
        ("Shapefile .dbf  ", ZONE_SHP_DIR / "taxi_zones.dbf"),
        ("Shapefile .shx  ", ZONE_SHP_DIR / "taxi_zones.shx"),
        ("Shapefile .prj  ", ZONE_SHP_DIR / "taxi_zones.prj"),
        ("Shapefile .cpg  ", ZONE_SHP_DIR / "taxi_zones.cpg"),
    ]:
        if path.exists() and path.stat().st_size > 0:
            lines.append(f"  {label}  OK   {human_bytes(path.stat().st_size):>10}  {path}")
        else:
            lines.append(f"  {label}  MISSING                {path}")

    grand_ok = grand_missing = grand_error = grand_unknown = 0

    for t in TYPES:
        jobs   = sorted(by_type[t], key=lambda j: (j.year, j.month))
        years  = sorted({j.year  for j in jobs})
        months = sorted({j.month for j in jobs})

        ok_jobs:      List[Job] = []
        missing_jobs: List[Job] = []
        error_jobs:   List[Job] = []
        unknown_jobs: List[Job] = []

        for j in jobs:
            st = progress.get(j.key)
            if st == "ok":                       ok_jobs.append(j)
            elif st == "missing":                missing_jobs.append(j)
            elif st and st.startswith("error:"): error_jobs.append(j)
            else:                                unknown_jobs.append(j)

        lines += ["", f"  [{t.upper()}]  (available from {FIRST_YEAR[t]})", "-" * 72]

        header = f"  {'Year':>4}  " + "  ".join(f"{MONTH_NAMES[m]:>4}" for m in months)
        lines.append(header)
        for y in years:
            row = []
            for m in months:
                st = progress.get(f"{t}:{y:04d}-{m:02d}")
                if st == "ok":                       row.append(" OK ")
                elif st == "missing":                row.append(" -- ")
                elif st and st.startswith("error:"): row.append(" ERR")
                else:                                row.append("  ? ")
            lines.append(f"  {y:>4}  " + "  ".join(row))

        lines += [
            "",
            f"  Summary: OK={len(ok_jobs)}  MISSING={len(missing_jobs)}  "
            f"ERRORS={len(error_jobs)}  NOT ATTEMPTED={len(unknown_jobs)}",
        ]

        if ok_jobs:
            lines += ["", "  Downloaded files:"]
            for j in ok_jobs:
                p = j.out_path(out_root)
                sz = human_bytes(p.stat().st_size) if p.exists() else "(missing on disk)"
                lines.append(f"    {j.fname:<52} {sz:>10}")

        if error_jobs:
            lines += ["", "  Files with errors (to retry):"]
            for j in error_jobs:
                lines.append(f"    {j.fname:<52} {progress.get(j.key)}")

        if unknown_jobs:
            lines += ["", f"  Files not yet attempted: {len(unknown_jobs)}"]

        grand_ok      += len(ok_jobs)
        grand_missing += len(missing_jobs)
        grand_error   += len(error_jobs)
        grand_unknown += len(unknown_jobs)

    lines += [
        "",
        "=" * 72,
        "  GLOBAL SUMMARY",
        "-" * 72,
        f"  Successfully downloaded : {grand_ok}",
        f"  Missing on server       : {grand_missing}  (files never published by TLC)",
        f"  Errors (to retry)       : {grand_error}",
        f"  Not yet attempted       : {grand_unknown}",
        "=" * 72,
        "",
    ]

    report_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"[INFO] Report written: {report_path}")


# ============================================================
# Runner
# ============================================================
def run(
    out_root: Path = OUT_ROOT,
    progress_path: Path = PROGRESS_PATH,
    log_path: Path = LOG_PATH,
    report_path: Path = REPORT_PATH,
    years: range = range(2009, LAST_YEAR + 1),
    months: range = range(1, 13),
    max_attempts: int = 5,
) -> None:
    out_root.mkdir(parents=True, exist_ok=True)

    session = make_session()

    # File di supporto (lookup CSV + shapefile) prima di tutto
    download_zone_lookup(session)
    download_zone_shapefile(session)

    prog   = Progress(progress_path)
    logger = Logger(log_path)

    all_jobs = iter_jobs(years, months)
    reconcile_disk(prog, all_jobs, out_root)

    todo  = [j for j in all_jobs if prog.should_attempt(j.key, max_attempts)]
    total = len(todo)

    logger.info(f"Total jobs: {len(all_jobs)} | To do: {total}")
    print(f"[INFO] Output  →  {out_root}")
    print(f"[INFO] Total jobs: {len(all_jobs)} | To do: {total}")
    print(f"[INFO] Sleep between files: {SLEEP_MIN}-{SLEEP_MAX}s | "
          f"Extra pause every {PAUSE_EVERY_N} files: {PAUSE_EXTRA_MIN}-{PAUSE_EXTRA_MAX}s\n")

    ok_streak = 0
    idx = 0

    while idx < len(todo):
        job = todo[idx]
        print(f"[{idx+1:4d}/{total}] {job.fname}")
        print(f"         URL: {job.url}")
        print(f"         OUT: {job.out_path(out_root)}")
        print(f"         ", end="", flush=True)

        status, size = download_one(session, job, out_root)

        if status == "throttled":
            print("403 -- checking whether throttled or file missing...")
            if is_throttled(session):
                wait = random.uniform(THROTTLE_WAIT_MIN, THROTTLE_WAIT_MAX)
                print(f"[THROTTLE] IP blocked. Waiting {wait:.0f}s.")
                logger.throttle(wait)
                session.close()
                session = make_session()
                time.sleep(wait)
                ok_streak = 0
                continue
            else:
                prog.set(job.key, "missing")
                logger.missing(job)
                print("MISSING")
                idx += 1

        elif status == "ok":
            prog.set(job.key, "ok")
            logger.ok(job, size)
            print(f"OK  ({human_bytes(size)})")
            ok_streak += 1
            idx += 1
            if ok_streak > 0 and ok_streak % PAUSE_EVERY_N == 0:
                wait = random.uniform(PAUSE_EXTRA_MIN, PAUSE_EXTRA_MAX)
                sleep_log(wait, f"extra pause after {PAUSE_EVERY_N} files")

        elif status == "missing":
            prog.set(job.key, "missing")
            logger.missing(job)
            print("MISSING")
            idx += 1

        else:
            code     = status.split(":", 1)[1] if ":" in status else "unknown"
            attempts = prog.inc_error(job.key, code)
            logger.error(job, code, attempts, max_attempts)
            print(f"ERROR [{code}] (attempt {attempts}/{max_attempts})")
            ok_streak = 0
            idx += 1

        time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))

    session.close()
    s = prog.summary()
    logger.close(s)
    write_report(prog, iter_jobs(years, months), out_root, report_path)

    print("\n--- FINAL SUMMARY ---")
    print(f"Root:    {out_root}")
    print(f"Log:     {log_path}")
    print(f"Report:  {report_path}")
    print(f"OK={s['ok']}  MISSING={s['missing']}  ERRORS={s['error']}  TOTAL={s['recorded']}")


# ============================================================
# Entry point
# ============================================================
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="TLC downloader CloudFront (anti-ban)")
    parser.add_argument("--report-only",    action="store_true",
                        help="Regenerate report only, without downloading")
    parser.add_argument("--reconcile-only", action="store_true",
                        help="Scan disk and update progress without downloading")
    parser.add_argument("--zones-only",     action="store_true",
                        help="Download zone lookup CSV + shapefile only, skip trip data")
    args = parser.parse_args()

    all_jobs = iter_jobs(range(2009, LAST_YEAR + 1), range(1, 13))
    session  = make_session()

    if args.zones_only:
        download_zone_lookup(session)
        download_zone_shapefile(session)
        session.close()
    elif args.report_only:
        prog = Progress(PROGRESS_PATH)
        write_report(prog, all_jobs, OUT_ROOT, REPORT_PATH)
    elif args.reconcile_only:
        prog = Progress(PROGRESS_PATH)
        reconcile_disk(prog, all_jobs, OUT_ROOT)
        write_report(prog, all_jobs, OUT_ROOT, REPORT_PATH)
    else:
        session.close()   # run() crea la propria sessione
        run()