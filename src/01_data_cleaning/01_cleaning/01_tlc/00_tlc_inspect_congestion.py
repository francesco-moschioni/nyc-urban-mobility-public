"""
00_tlc_inspect_congestion.py
----------------------------
Pipeline stage : diagnostic — run once, not part of the main pipeline
Input          : cfg.raw_tlc/{yellow,green,fhvhv,fhv} — monthly parquet files
Output         : cfg.reports/tlc_congestion_inspect.txt  (also printed to console)

What it does
------------
For each TLC dataset, compares the schema of 2024–2025 files against the
2021–2023 baseline to detect:
  - New columns related to congestion / CBD / MTA fees
  - Any schema drift (added or removed columns)
  - Sample values for congestion-related columns in the latest available file

Useful for deciding whether cbd_congestion_fee needs to be tracked separately
from the existing congestion_surcharge column.
"""

import importlib.util
import re
import sys
from pathlib import Path
import pyarrow.parquet as pq
import pandas as pd
import numpy as np

# ── Config ────────────────────────────────────────────────────────────────────
_cfg_path = Path(__file__).parents[4] / "00_config.py"
_spec     = importlib.util.spec_from_file_location("config", _cfg_path)
_mod      = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
cfg = _mod.cfg

out_path = cfg.reports / "tlc_congestion_inspect.txt"
out_path.parent.mkdir(parents=True, exist_ok=True)

class _Tee:
    def __init__(self, *streams):
        self._streams = streams
    def write(self, data):
        for s in self._streams:
            s.write(data)
    def flush(self):
        for s in self._streams:
            s.flush()

_log = open(out_path, "w", encoding="utf-8")
sys.stdout = _Tee(sys.__stdout__, _log)

# ── Congestion-related keywords to look for ───────────────────────────────────
CONGESTION_KEYWORDS = ["congestion", "cbd", "mta_fee", "central_business"]

SEP = "=" * 70

MODES = {
    "yellow": cfg.raw_tlc / "yellow",
    "green":  cfg.raw_tlc / "green",
    "hvfhv":  cfg.raw_tlc / "fhvhv",
    "fhv":    cfg.raw_tlc / "fhv",
}


def get_year(fpath: Path) -> int | None:
    m = re.search(r"(\d{4})", fpath.stem)
    return int(m.group(1)) if m else None


def congestion_cols(schema_names: list[str]) -> list[str]:
    return [
        c for c in schema_names
        if any(kw in c.lower() for kw in CONGESTION_KEYWORDS)
    ]


def inspect_mode(mode: str, raw_dir: Path) -> None:
    print(f"\n{SEP}")
    print(f"  MODE: {mode.upper()}  —  {raw_dir}")
    print(SEP)

    files = sorted(raw_dir.glob("*.parquet"))
    if not files:
        print("  No parquet files found.")
        return

    # split into baseline (2021–2023) and recent (2024–2025)
    baseline = [f for f in files if get_year(f) in (2021, 2022, 2023)]
    recent   = [f for f in files if get_year(f) in (2024, 2025)]

    if not baseline:
        print("  No 2021–2023 files found for baseline.")
        return
    if not recent:
        print("  No 2024–2025 files found.")
        return

    # read baseline schema from first available file
    baseline_schema = pq.read_schema(baseline[0])
    baseline_cols   = baseline_schema.names
    baseline_congestion = congestion_cols(baseline_cols)

    print(f"\n  Baseline ({baseline[0].name}):")
    print(f"    Total columns : {len(baseline_cols)}")
    print(f"    Congestion cols: {baseline_congestion if baseline_congestion else 'none'}")

    # check each recent file for schema drift
    print(f"\n  Schema drift (2024–2025 vs baseline):")
    any_drift = False
    for fpath in recent:
        schema = pq.read_schema(fpath)
        cols   = schema.names
        added   = [c for c in cols if c not in baseline_cols]
        removed = [c for c in baseline_cols if c not in cols]
        if added or removed:
            any_drift = True
            print(f"\n  ⚠  {fpath.name}:")
            for c in added:
                dtype = schema.field(c).type
                print(f"       + ADDED:   {c}  ({dtype})")
            for c in removed:
                print(f"       - REMOVED: {c}")
        else:
            print(f"    ✓  {fpath.name} — schema identical to baseline")

    if not any_drift:
        print("    No schema drift detected in any 2024–2025 file.")

    # for the latest file, show congestion column stats
    latest = recent[-1]
    latest_schema = pq.read_schema(latest)
    latest_congestion = congestion_cols(latest_schema.names)

    print(f"\n  Congestion-related columns in latest file ({latest.name}):")
    if not latest_congestion:
        print("    None found.")
    else:
        # read only congestion cols from first row group
        pf  = pq.ParquetFile(latest)
        rg0 = pf.read_row_group(0, columns=latest_congestion).to_pandas()
        for col in latest_congestion:
            if col not in rg0.columns:
                continue
            vals = pd.to_numeric(rg0[col], errors="coerce").dropna()
            if len(vals) == 0:
                print(f"    {col:<40s}  all null")
                continue
            n_nonzero = (vals != 0).sum()
            print(f"    {col:<40s}  "
                  f"mean={vals.mean():.4f}  "
                  f"min={vals.min():.4f}  "
                  f"max={vals.max():.4f}  "
                  f"non-zero: {n_nonzero:,}/{len(vals):,} "
                  f"({100*n_nonzero/len(vals):.1f}%)")

    # sample 3 rows with non-zero congestion values
    if latest_congestion:
        pf   = pq.ParquetFile(latest)
        rg0  = pf.read_row_group(0, columns=latest_congestion).to_pandas()
        mask = rg0[latest_congestion].apply(
            lambda col: pd.to_numeric(col, errors="coerce").fillna(0) != 0
        ).any(axis=1)
        sample = rg0[mask].head(3)
        if not sample.empty:
            print(f"\n  Sample rows with non-zero congestion values (row group 0):")
            print(sample.to_string(max_colwidth=20))


# ── Main ──────────────────────────────────────────────────────────────────────
print(f"\n{'='*70}")
print("TLC Congestion Fee Inspection — 00_tlc_inspect_congestion.py")
print(f"{'='*70}")
print(f"Looking for keywords: {CONGESTION_KEYWORDS}")

for mode, raw_dir in MODES.items():
    inspect_mode(mode, raw_dir)

print(f"\n\n{'='*70}")
print("SUMMARY")
print(f"{'='*70}")
print("""
Check above for:
  1. Any '+ ADDED' columns containing 'congestion', 'cbd', or 'mta_fee'
     in 2024-2025 files → these are candidates for separate tracking
  2. Non-zero share of the new columns → determines if they are populated
  3. Value ranges → helps decide if they should enter the price variable
""")

sys.stdout = sys.__stdout__
_log.close()
print(f"\nReport saved to: {out_path}")
