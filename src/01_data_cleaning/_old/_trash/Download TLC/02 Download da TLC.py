from __future__ import annotations

import json
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ----------------------------
# CONFIG
# ----------------------------
OUT_ROOT = Path(r"D:\Tesi")
PROGRESS_PATH = OUT_ROOT / "_progress.json"

BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data"
TYPES = ("yellow", "green", "fhv", "fhvhv")


# ----------------------------
# Helpers
# ----------------------------
def human_bytes(n: Optional[int]) -> str:
    if n is None:
        return "?"
    units = ["B", "KB", "MB", "GB", "TB"]
    x = float(n)
    for u in units:
        if x < 1024 or u == units[-1]:
            return f"{x:.2f} {u}" if u != "B" else f"{int(x)} {u}"
        x /= 1024
    return f"{n} B"


def print_progress(prefix: str, done: int, total: Optional[int]) -> None:
    # single-line progress bar
    bar_len = 28
    if total and total > 0:
        frac = min(max(done / total, 0.0), 1.0)
        filled = int(bar_len * frac)
        bar = "█" * filled + "░" * (bar_len - filled)
        pct = int(frac * 100)
        msg = f"{prefix} [{bar}] {pct:3d}%  {human_bytes(done)} / {human_bytes(total)}"
    else:
        # unknown total
        bar = "█" * (done // (1024 * 1024) % (bar_len + 1))
        msg = f"{prefix} {human_bytes(done)} / ?"

    sys.stdout.write("\r" + msg + " " * 10)
    sys.stdout.flush()


# ----------------------------
# Model
# ----------------------------
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
        return f"{BASE}/{self.fname}"

    def out_path(self, root: Path) -> Path:
        return root / self.t / self.fname

    @property
    def key(self) -> str:
        return f"{self.t}:{self.year:04d}-{self.month:02d}"


# ----------------------------
# Progress (resume + retry errors)
# ----------------------------
class Progress:
    """
    { key: "ok" | "missing" | "error:<code>:attempts=<n>" }
    """

    def __init__(self, path: Path):
        self.path = path
        self.data = self._load()

    def _load(self) -> dict[str, str]:
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
        # error:403:attempts=2
        parts = status.split(":attempts=")
        if len(parts) == 2:
            try:
                return int(parts[1])
            except Exception:
                return 0
        return 0

    def should_attempt(self, key: str, max_error_attempts: int) -> bool:
        prev = self.get(key)
        if prev is None:
            return True
        if prev.startswith("error:"):
            return self._parse_attempts(prev) < max_error_attempts
        # ok/missing -> no
        return False

    def inc_error(self, key: str, code: str) -> int:
        prev = self.get(key)
        attempts = 0
        if prev and prev.startswith("error:"):
            attempts = self._parse_attempts(prev)
        attempts += 1
        self.set(key, f"error:{code}:attempts={attempts}")
        return attempts

    def summary(self) -> dict[str, int]:
        out = {"ok": 0, "missing": 0, "error": 0, "recorded": len(self.data)}
        for v in self.data.values():
            if v == "ok":
                out["ok"] += 1
            elif v == "missing":
                out["missing"] += 1
            elif v.startswith("error:"):
                out["error"] += 1
        return out


# ----------------------------
# HTTP
# ----------------------------
def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=0.8,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "HEAD"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    s.mount("https://", adapter)
    s.headers.update({"User-Agent": "tlc-downloader/1.4"})
    return s


def iter_jobs(years: Iterable[int], months: Iterable[int], types: Iterable[str] = TYPES) -> Iterable[Job]:
    for t in types:
        for y in years:
            for m in months:
                yield Job(t=t, year=y, month=m)


# ----------------------------
# Download with per-file progress bar
# ----------------------------
def download_one(session: requests.Session, job: Job, root: Path) -> Tuple[str, Optional[int]]:
    """
    Returns (status, total_size_bytes_if_known)
    status: ok | missing | error:<code>
    """
    out_path = job.out_path(root)
    tmp_path = out_path.with_suffix(out_path.suffix + ".part")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Fast-skip if already complete (when size known via HEAD)
    total_size: Optional[int] = None
    try:
        h = session.head(job.url, timeout=30)
        if h.status_code == 404:
            return ("missing", None)
        # if HEAD forbidden (403), still try GET
        if h.status_code >= 400 and h.status_code not in (403, 405):
            return (f"error:{h.status_code}", None)
        if "Content-Length" in h.headers:
            try:
                total_size = int(h.headers["Content-Length"])
            except Exception:
                total_size = None
    except Exception:
        # if HEAD fails, just try GET
        total_size = None

    if out_path.exists() and total_size is not None:
        try:
            if out_path.stat().st_size == total_size:
                return ("ok", total_size)
        except Exception:
            pass

    # GET + progress
    try:
        with session.get(job.url, stream=True, timeout=60) as resp:
            if resp.status_code == 404:
                return ("missing", None)
            if resp.status_code >= 400:
                return (f"error:{resp.status_code}", None)

            # prefer GET length if present
            if total_size is None and resp.headers.get("Content-Length"):
                try:
                    total_size = int(resp.headers["Content-Length"])
                except Exception:
                    total_size = None

            done = 0
            prefix = f"{job.t} {job.year}-{job.month:02d} ({human_bytes(total_size)})"

            with open(tmp_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    if not chunk:
                        continue
                    f.write(chunk)
                    done += len(chunk)
                    print_progress(prefix, done, total_size)

            # end line after progress bar
            sys.stdout.write("\n")
            sys.stdout.flush()

    except Exception:
        return ("error:get_exception", total_size)

    os.replace(tmp_path, out_path)
    return ("ok", total_size)


# ----------------------------
# Runner
# ----------------------------
def run(
    out_root: Path = OUT_ROOT,
    progress_path: Path = PROGRESS_PATH,
    years: range = range(2009, 2026),   # 2009..2025
    months: range = range(1, 13),
    sleep_s: float = 0.10,
    max_error_attempts: int = 10,
    wait_on_first_403_seconds: int = 1 * 60 * 60,  # 2 hours
) -> None:
    out_root.mkdir(parents=True, exist_ok=True)
    prog = Progress(progress_path)
    session = make_session()

    jobs = list(iter_jobs(years, months))
    total_jobs = len(jobs)

    waited_for_403 = False
    attempted = 0

    for idx, job in enumerate(jobs, start=1):
        if not prog.should_attempt(job.key, max_error_attempts):
            continue

        while True:
            attempted += 1
            status, size = download_one(session, job, out_root)

            if status == "ok" or status == "missing":
                prog.set(job.key, status)
                print(f"[DONE] {idx:4d}/{total_jobs}  {job.fname} -> {status}")
                break

            # error path
            # extract code
            code = status.split(":", 1)[1] if ":" in status else "unknown"
            attempts = prog.inc_error(job.key, code)
            print(f"[ERR ] {idx:4d}/{total_jobs}  {job.fname} -> error:{code} (attempt {attempts}/{max_error_attempts})")

            # Special rule: first time we see a 403, wait 2 hours then retry this same file immediately
            if code == "403" and not waited_for_403 and attempts < max_error_attempts:
                waited_for_403 = True
                print(f"[WAIT] First 403 encountered. Sleeping {wait_on_first_403_seconds} seconds (2 hours) then retrying the same file...")
                time.sleep(wait_on_first_403_seconds)
                continue  # retry same job

            # otherwise: stop retrying immediately in this run; next run will retry (because it's saved as error)
            break

        time.sleep(sleep_s)

    s = prog.summary()
    print("\n--- SUMMARY ---")
    print(f"Root: {out_root}")
    print(f"Progress: {progress_path}")
    print(f"ok={s['ok']} missing={s['missing']} error={s['error']} recorded={s['recorded']}")


if __name__ == "__main__":
    run()
