"""
00_tlc_inspect_hvfhv.py
-----------------------
Pipeline stage : diagnostic — run once, not part of the main pipeline
Input          : cfg.raw_tlc/fhvhv — monthly parquet files from 2021 onwards
Output         : cfg.reports/hvfhv_inspect.txt  (also printed to console)

What it does
------------
For each HVFHV parquet file >= 2021:
  - Lists all columns and their dtypes
  - Flags which columns differ from the first file (schema drift)
  - Shows min/max of pickup datetime to verify coverage
  - Shows 3 sample rows
"""

import importlib.util
import re
import sys
from pathlib import Path
import pyarrow.parquet as pq
import pandas as pd

# ── Config ────────────────────────────────────────────────────────────────────
_cfg_path = Path(__file__).parents[4] / "00_config.py"
_spec     = importlib.util.spec_from_file_location("config", _cfg_path)
_mod      = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
cfg = _mod.cfg

raw_dir  = cfg.raw_tlc / "fhvhv"
out_path = cfg.reports / "hvfhv_inspect.txt"
out_path.parent.mkdir(parents=True, exist_ok=True)

# Tee: write to both console and file simultaneously
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

# ── Discover files >= 2021 ────────────────────────────────────────────────────
all_files = sorted(raw_dir.glob("*.parquet"))
files = []
for f in all_files:
    m = re.search(r"(\d{4})", f.stem)
    if m and int(m.group(1)) >= 2021:
        files.append(f)

print(f"Found {len(all_files)} total HVFHV files, {len(files)} from 2021 onwards.\n")

# ── Inspect each file ─────────────────────────────────────────────────────────
reference_cols = None   # columns from the first file — used to detect drift

for fpath in files:
    pf     = pq.ParquetFile(fpath)
    schema = pf.schema_arrow
    cols   = {field.name: str(field.type) for field in schema}

    print("=" * 70)
    print(f"FILE: {fpath.name}  ({pf.metadata.num_rows:,} rows)")
    print("=" * 70)

    # schema drift check
    if reference_cols is None:
        reference_cols = cols
        print("  [REFERENCE] — all subsequent files compared to this one")
    else:
        added   = set(cols) - set(reference_cols)
        removed = set(reference_cols) - set(cols)
        changed = {c for c in cols if c in reference_cols and cols[c] != reference_cols[c]}
        if added or removed or changed:
            print(f"  ⚠ SCHEMA DRIFT vs reference:")
            for c in sorted(added):
                print(f"      + ADDED:   {c} ({cols[c]})")
            for c in sorted(removed):
                print(f"      - REMOVED: {c} ({reference_cols[c]})")
            for c in sorted(changed):
                print(f"      ~ CHANGED: {c}  {reference_cols[c]} → {cols[c]}")
        else:
            print("  ✓ Schema identical to reference")

    # print all columns
    print(f"\n  Columns ({len(cols)}):")
    for name, dtype in cols.items():
        print(f"    {name:<40s} {dtype}")

    # read first row group only for datetime range + sample
    rg0 = pf.read_row_group(0).to_pandas()

    # detect pickup col (handles case variation)
    pickup_col = next(
        (c for c in rg0.columns if "pickup" in c.lower() and "datetime" in c.lower()), None
    )
    if pickup_col:
        dt = pd.to_datetime(rg0[pickup_col], errors="coerce")
        print(f"\n  Pickup datetime range (row group 0):")
        print(f"    min: {dt.min()}   max: {dt.max()}")

    print(f"\n  Sample (3 rows, row group 0):")
    print(rg0.head(3).to_string(max_cols=20, max_colwidth=25))
    print()

sys.stdout = sys.__stdout__
_log.close()
print(f"\nReport saved to: {out_path}")