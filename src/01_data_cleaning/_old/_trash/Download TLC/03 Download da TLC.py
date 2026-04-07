from __future__ import annotations

"""
tlc_downloader_v5.py
====================
Scarica i dati TLC NYC da CloudFront (parquet mensili).
Strategia anti-ban: pause lunghe, niente HEAD request, backoff
aggressivo al primo segnale di throttle.

Output: <OUT_ROOT>/<tipo>/<tipo>_tripdata_<anno>-<mm>.parquet
"""

import datetime
import json
import os
import random
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ============================================================
# CONFIG  -- modifica qui se necessario
# ============================================================
OUT_ROOT      = Path(r"D:\Tesi")
PROGRESS_PATH = OUT_ROOT / "_progress_v5.json"
LOG_PATH      = OUT_ROOT / "_download.log"
REPORT_PATH   = OUT_ROOT / "_data_description.txt"

CF_BASE    = "https://d37ci6vzurychx.cloudfront.net/trip-data"
CANARY_URL = f"{CF_BASE}/yellow_tripdata_2023-01.parquet"  # file sicuramente esistente

TYPES = ("yellow", "green", "fhv", "fhvhv")

# Pausa tra un file e l'altro (secondi, estratta a caso)
SLEEP_MIN = 15
SLEEP_MAX = 25

# Pausa aggiuntiva ogni N file scaricati con successo
PAUSE_EVERY_N   = 10
PAUSE_EXTRA_MIN = 3 * 60   # 3 minuti
PAUSE_EXTRA_MAX = 5 * 60   # 5 minuti

# Attesa dopo throttle rilevato
THROTTLE_WAIT_MIN = 20 * 60   # 20 minuti
THROTTLE_WAIT_MAX = 40 * 60   # 40 minuti

# Anni e tipi disponibili su CloudFront
# (green da 2013, fhv da 2015, fhvhv da 2019)
FIRST_YEAR: Dict[str, int] = {
    "yellow": 2009,
    "green":  2013,
    "fhv":    2015,
    "fhvhv":  2019,
}
LAST_YEAR = 2025   # aggiorna se escono nuovi dati


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
    print(f"[WAIT] {reason} -- attendo {seconds:.0f}s ...")
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
    """Genera solo i job che hanno senso (rispetta FIRST_YEAR per tipo)."""
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
            if v == "ok":       out["ok"] += 1
            elif v == "missing": out["missing"] += 1
            elif v.startswith("error:"): out["error"] += 1
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
        self._w(f"THROTTLE IP bloccato -- attendo {wait_s:.0f}s")

    def info(self, msg: str) -> None:
        self._w(f"INFO     {msg}")

    def close(self, summary: Dict[str, int]) -> None:
        self._w(f"SESSION END  OK={summary['ok']}  MISSING={summary['missing']}  "
                f"ERRORI={summary['error']}  TOT={summary['recorded']}")
        self._w(f"{'='*70}\n")
        self._f.close()


# ============================================================
# HTTP
# ============================================================
def make_session() -> requests.Session:
    s = requests.Session()
    # Retry solo su errori server (5xx), MAI su 403/404
    retry = Retry(
        total=3,
        backoff_factor=2.0,
        status_forcelist=(500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=1, pool_maxsize=1)
    s.mount("https://", adapter)
    # Header copiati esattamente dal browser (Edge 145 su Windows)
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
    """Controlla se l'IP è bloccato testando il file canary."""
    try:
        r = session.get(CANARY_URL, headers={"Range": "bytes=0-0"},
                        timeout=(10, 15), stream=True)
        r.close()
        return r.status_code == 403
    except Exception:
        return True


# ============================================================
# Download singolo file
# ============================================================
def download_one(
    session: requests.Session,
    job: Job,
    root: Path,
) -> Tuple[str, Optional[int]]:
    """
    Ritorna (status, size):
      "ok"        -> scaricato / già presente
      "missing"   -> file non esiste sul server
      "throttled" -> IP bloccato (da gestire nel chiamante)
      "error:XYZ" -> errore generico
    """
    out_path = job.out_path(root)
    tmp_path = out_path.with_suffix(out_path.suffix + ".part")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Già completo su disco
    if out_path.exists() and out_path.stat().st_size > 0:
        return ("ok", out_path.stat().st_size)

    # Resume da .part
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
                # .part già completo
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
                        print_progress(job.t + " " + f"{job.year}-{job.month:02d}",
                                       done, total_size)

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
    print("[RECONCILE] Scansione file già presenti su disco...")
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
            print(f"  [TROVATO] {f.name}  ({human_bytes(f.stat().st_size)})")
    if added == 0:
        print("[RECONCILE] Nessun file nuovo trovato su disco.")
    else:
        print(f"[RECONCILE] {added} file registrati nel progress.\n")
    return added


# ============================================================
# Report
# ============================================================
MONTH_NAMES = ["", "Gen", "Feb", "Mar", "Apr", "Mag", "Giu",
               "Lug", "Ago", "Set", "Ott", "Nov", "Dic"]


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
        f"  Generato : {now}",
        f"  Root     : {out_root}",
        f"  Fonte    : CloudFront TLC  (d37ci6vzurychx.cloudfront.net)",
        f"  Formato  : Parquet mensile",
        "=" * 72,
    ]

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
            if st == "ok":               ok_jobs.append(j)
            elif st == "missing":        missing_jobs.append(j)
            elif st and st.startswith("error:"): error_jobs.append(j)
            else:                        unknown_jobs.append(j)

        lines += ["", f"  [{t.upper()}]  (disponibile dal {FIRST_YEAR[t]})", "-" * 72]

        # Griglia anno x mese
        header = f"  {'Anno':>4}  " + "  ".join(f"{MONTH_NAMES[m]:>4}" for m in months)
        lines.append(header)
        for y in years:
            row = []
            for m in months:
                st = progress.get(f"{t}:{y:04d}-{m:02d}")
                if st == "ok":                   row.append(" OK ")
                elif st == "missing":            row.append(" -- ")
                elif st and st.startswith("error:"): row.append(" ERR")
                else:                            row.append("  ? ")
            lines.append(f"  {y:>4}  " + "  ".join(row))

        lines += [
            "",
            f"  Riepilogo: OK={len(ok_jobs)}  MISSING={len(missing_jobs)}  "
            f"ERRORI={len(error_jobs)}  NON TENTATI={len(unknown_jobs)}",
        ]

        # Dettaglio file scaricati
        if ok_jobs:
            lines += ["", "  File scaricati:"]
            for j in ok_jobs:
                p = j.out_path(out_root)
                sz = human_bytes(p.stat().st_size) if p.exists() else "(mancante su disco)"
                lines.append(f"    {j.fname:<52} {sz:>10}")

        # File con errori
        if error_jobs:
            lines += ["", "  File con errori (da riprovare):"]
            for j in error_jobs:
                lines.append(f"    {j.fname:<52} {progress.get(j.key)}")

        # File non ancora tentati
        if unknown_jobs:
            lines += ["", f"  File non ancora tentati: {len(unknown_jobs)}"]

        grand_ok      += len(ok_jobs)
        grand_missing += len(missing_jobs)
        grand_error   += len(error_jobs)
        grand_unknown += len(unknown_jobs)

    lines += [
        "",
        "=" * 72,
        "  RIEPILOGO GLOBALE",
        "-" * 72,
        f"  Scaricati con successo : {grand_ok}",
        f"  Mancanti sul server    : {grand_missing}  (file mai pubblicati dal TLC)",
        f"  Errori (da riprovare)  : {grand_error}",
        f"  Non ancora tentati     : {grand_unknown}",
        "=" * 72,
        "",
    ]

    report_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"[INFO] Report scritto: {report_path}")


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
    prog   = Progress(progress_path)
    logger = Logger(log_path)

    all_jobs = iter_jobs(years, months)

    # Marca come ok tutto quello già su disco
    reconcile_disk(prog, all_jobs, out_root)

    session = make_session()
    todo    = [j for j in all_jobs if prog.should_attempt(j.key, max_attempts)]
    total   = len(todo)

    logger.info(f"Job totali: {len(all_jobs)} | Da fare: {total}")
    print(f"[INFO] Job totali: {len(all_jobs)} | Da fare: {total}")
    print(f"[INFO] Pausa tra file: {SLEEP_MIN}-{SLEEP_MAX}s | "
          f"Pausa extra ogni {PAUSE_EVERY_N} file: {PAUSE_EXTRA_MIN}-{PAUSE_EXTRA_MAX}s\n")

    ok_streak = 0   # file ok consecutivi (per pause extra)
    idx = 0

    while idx < len(todo):
        job = todo[idx]
        print(f"[{idx+1:4d}/{total}] {job.fname}")
        print(f"         URL: {job.url}")
        print(f"         OUT: {job.out_path(out_root)}")
        print(f"         ", end="", flush=True)

        status, size = download_one(session, job, out_root)

        # ── Throttle ─────────────────────────────────────────────────
        if status == "throttled":
            print("403 -- verifico se è throttle o file mancante...")
            if is_throttled(session):
                wait = random.uniform(THROTTLE_WAIT_MIN, THROTTLE_WAIT_MAX)
                print(f"[THROTTLE] IP bloccato. Attendo {wait:.0f}s.")
                logger.throttle(wait)
                session.close()
                session = make_session()
                time.sleep(wait)
                ok_streak = 0
                continue   # riprova lo stesso file
            else:
                prog.set(job.key, "missing")
                logger.missing(job)
                print("MISSING")
                idx += 1

        # ── OK ───────────────────────────────────────────────────────
        elif status == "ok":
            prog.set(job.key, "ok")
            logger.ok(job, size)
            print(f"OK  ({human_bytes(size)})")
            ok_streak += 1
            idx += 1

            # Pausa extra periodica
            if ok_streak > 0 and ok_streak % PAUSE_EVERY_N == 0:
                wait = random.uniform(PAUSE_EXTRA_MIN, PAUSE_EXTRA_MAX)
                sleep_log(wait, f"pausa extra dopo {PAUSE_EVERY_N} file")

        # ── Missing ──────────────────────────────────────────────────
        elif status == "missing":
            prog.set(job.key, "missing")
            logger.missing(job)
            print("MISSING")
            idx += 1

        # ── Errore ───────────────────────────────────────────────────
        else:
            code     = status.split(":", 1)[1] if ":" in status else "unknown"
            attempts = prog.inc_error(job.key, code)
            logger.error(job, code, attempts, max_attempts)
            print(f"ERRORE [{code}] (tentativo {attempts}/{max_attempts})")
            ok_streak = 0
            idx += 1

        # Pausa normale tra file
        time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))

    session.close()
    s = prog.summary()
    logger.close(s)
    write_report(prog, iter_jobs(years, months), out_root, report_path)

    print("\n--- SOMMARIO FINALE ---")
    print(f"Root:    {out_root}")
    print(f"Log:     {log_path}")
    print(f"Report:  {report_path}")
    print(f"OK={s['ok']}  MISSING={s['missing']}  ERRORI={s['error']}  TOT={s['recorded']}")


# ============================================================
# Entry point
# ============================================================
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="TLC downloader CloudFront (anti-ban)")
    parser.add_argument("--report-only",    action="store_true",
                        help="Rigenera solo il report senza scaricare")
    parser.add_argument("--reconcile-only", action="store_true",
                        help="Scansiona il disco e aggiorna il progress senza scaricare")
    args = parser.parse_args()

    all_jobs = iter_jobs(range(2009, LAST_YEAR + 1), range(1, 13))

    if args.report_only:
        prog = Progress(PROGRESS_PATH)
        write_report(prog, all_jobs, OUT_ROOT, REPORT_PATH)
    elif args.reconcile_only:
        prog = Progress(PROGRESS_PATH)
        reconcile_disk(prog, all_jobs, OUT_ROOT)
        write_report(prog, all_jobs, OUT_ROOT, REPORT_PATH)
    else:
        run()