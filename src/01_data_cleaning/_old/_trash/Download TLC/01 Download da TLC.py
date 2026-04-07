from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ----------------------------
# CONFIG (Windows paths)
# ----------------------------
OUT_ROOT = Path(
    r"D:\Tesi"
)
PROGRESS_PATH = OUT_ROOT / "_progress.json"

BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data"
TYPES = ("yellow", "green", "fhv", "fhvhv")


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
# Progress tracking with attempts for errors
# ----------------------------
class Progress:
    """
    Stores a map {job_key: status} on disk.
    status:
      - "ok"
      - "missing"
      - "error:<code>:attempts=<n>"
    """

    def __init__(self, path: Path):
        self.path = path
        self.data = self._load()

    def _load(self) -> dict[str, str]:
        if self.path.exists():
            try:
                return json.loads(self.path.read_text(encoding="utf-8"))
            except Exception:
                # corruption guard
                return {}
        return {}

    def get(self, key: str) -> str | None:
        return self.data.get(key)

    def set(self, key: str, status: str) -> None:
        self.data[key] = status
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.data, indent=2, sort_keys=True), encoding="utf-8")
        os.replace(tmp, self.path)

    def increment_error_attempts(self, key: str, code: str) -> int:
        """
        Increment attempts for an error and return new attempt count.
        """
        prev = self.get(key)
        if prev and prev.startswith("error:"):
            # possible formats: error:403 or error:403:attempts=2
            parts = prev.split(":attempts=")
            if len(parts) == 2:
                try:
                    n = int(parts[1])
                except Exception:
                    n = 0
            else:
                n = 0
        else:
            n = 0
        n += 1
        new_status = f"error:{code}:attempts={n}"
        self.set(key, new_status)
        return n

    def summary(self) -> dict[str, int]:
        out = {"ok": 0, "missing": 0, "error": 0, "total_recorded": 0}
        for v in self.data.values():
            if v == "ok":
                out["ok"] += 1
            elif v == "missing":
                out["missing"] += 1
            elif v and v.startswith("error:"):
                out["error"] += 1
        out["total_recorded"] = len(self.data)
        return out


# ----------------------------
# HTTP session with retry
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
    s.headers.update({"User-Agent": "tlc-downloader/1.3"})
    return s


# ----------------------------
# Core download logic
# ----------------------------
def iter_jobs(
    years: Iterable[int],
    months: Iterable[int],
    types: Iterable[str] = TYPES,
) -> Iterable[Job]:
    for t in types:
        for y in years:
            for m in months:
                yield Job(t=t, year=y, month=m)


def download_one(session: requests.Session, job: Job, root: Path) -> str:
    out_path = job.out_path(root)
    tmp_path = out_path.with_suffix(out_path.suffix + ".part")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # HEAD: skip fast if missing; handle 403 by trying GET below
    try:
        r = session.head(job.url, timeout=30)
    except Exception as e:
        return f"error:head_exception"

    if r.status_code == 404:
        return "missing"

    # if HEAD returns 403 we will still try GET (some CF endpoints block HEAD)
    if r.status_code >= 400 and r.status_code != 403 and r.status_code != 405:
        return f"error:{r.status_code}"

    remote_size = r.headers.get("Content-Length")
    if out_path.exists() and remote_size:
        try:
            if out_path.stat().st_size == int(remote_size):
                return "ok"
        except Exception:
            pass

    # GET streaming (defensive)
    try:
        with session.get(job.url, stream=True, timeout=60) as resp:
            if resp.status_code == 404:
                return "missing"
            if resp.status_code >= 400:
                return f"error:{resp.status_code}"

            with open(tmp_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
    except Exception:
        return "error:get_exception"

    os.replace(tmp_path, out_path)
    return "ok"


# ----------------------------
# Runner with retry for previous errors
# ----------------------------
def run(
    out_root: Path = OUT_ROOT,
    progress_path: Path = PROGRESS_PATH,
    years: range = range(2009, 2026),
    months: range = range(1, 13),
    sleep_s: float = 0.10,
    max_error_attempts: int = 5,
) -> None:
    out_root.mkdir(parents=True, exist_ok=True)
    prog = Progress(progress_path)
    session = make_session()

    jobs = list(iter_jobs(years, months))
    total = len(jobs)

    processed = 0
    for job in jobs:
        key = job.key
        prev = prog.get(key)

        # Decide whether to attempt:
        # - if never seen -> attempt
        # - if prev starts with error: and attempts < max_error_attempts -> attempt
        # - otherwise skip
        attempt_allowed = False
        if prev is None:
            attempt_allowed = True
        elif prev.startswith("error:"):
            # parse attempts
            parts = prev.split(":attempts=")
            if len(parts) == 2:
                try:
                    attempts = int(parts[1])
                except Exception:
                    attempts = 0
            else:
                attempts = 0
            if attempts < max_error_attempts:
                attempt_allowed = True
            else:
                print(
                    f"[SKIP] {job.t} {job.year}-{job.month:02d} -> previously errored {attempts} times (>= {max_error_attempts})"
                )

        if not attempt_allowed:
            continue

        status = download_one(session, job, out_root)

        # If error, increment attempts counter (and record code)
        if status.startswith("error:"):
            # extract code part (e.g., error:403 or error:get_exception or error:head_exception)
            parts = status.split(":", 1)
            code = parts[1] if len(parts) > 1 else "unknown"
            attempts = prog.increment_error_attempts(key, code)
            print(f"[ATTEMPT {attempts}] {job.t} {job.year}-{job.month:02d} -> error:{code}")
        else:
            # ok or missing -> write directly (overwrite any previous error)
            prog.set(key, status)
            print(f"[DONE]    {job.t} {job.year}-{job.month:02d} -> {status}")

        processed += 1
        time.sleep(sleep_s)

    s = prog.summary()
    print("\n--- SUMMARY ---")
    print(f"Saved in: {out_root}")
    print(f"Progress: {progress_path}")
    print(f"ok={s['ok']} missing={s['missing']} error={s['error']} recorded={s['total_recorded']}")


if __name__ == "__main__":
    run()
