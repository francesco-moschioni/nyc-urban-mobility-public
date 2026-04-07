"""
pipeline_status.py
Utility script — run from anywhere inside Tesi/.
Checks which pipeline outputs exist and reports their status.
No data is written.
"""

import importlib.util
from pathlib import Path
import sys

# ── Config ────────────────────────────────────────────────────────────────────
_cfg_path = Path(__file__).parents[3] / "00_config.py"
_spec = importlib.util.spec_from_file_location("config", _cfg_path)
_mod  = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
cfg = _mod.cfg

def fmt_size(path):
    mb = path.stat().st_size / 1e6
    if mb >= 1000:
        return f"{mb/1000:.1f} GB"
    return f"{mb:.1f} MB"

def check(label, path, extra_fn=None):
    path = Path(path)
    if path.exists():
        size = fmt_size(path) if path.is_file() else ""
        extra = ""
        if extra_fn and path.is_file():
            try:
                extra = extra_fn(path)
            except Exception as e:
                extra = f"[read error: {e}]"
        print(f"  [OK]     {label:<50} {size:>10}  {extra}")
    else:
        print(f"  [MISSING] {label:<50} {path}")

def check_dir(label, path, glob="*", min_files=1):
    path = Path(path)
    if path.exists():
        files = list(path.glob(glob))
        print(f"  [OK]     {label:<50} {len(files)} file(s)")
    else:
        print(f"  [MISSING] {label:<50} {path}")

def parquet_info(path):
    import pyarrow.parquet as pq
    pf = pq.ParquetFile(path)
    return f"{pf.metadata.num_rows:,} rows | {pf.metadata.num_row_groups} row groups"

SEP = "=" * 70

# ── RAW DATA ──────────────────────────────────────────────────────────────────
print(f"\n{SEP}")
print("RAW DATA")
print(SEP)
check_dir("TLC yellow (parquet/month)",   cfg.raw_tlc / "yellow",  "*.parquet")
check_dir("TLC fhvhv (parquet/month)",    cfg.raw_tlc / "fhvhv",   "*.parquet")
check_dir("MTA ridership / alerts",       cfg.raw_mta,              "*.csv")
check_dir("Citi Bike unzipped",           cfg.raw_citibike / "unzipped", "*.csv")
check_dir("Weather NOAA",                 cfg.raw_weather,          "*.csv")
check_dir("Traffic",                      cfg.raw_traffic,          "*.csv")
check_dir("GTFS current feed",            cfg.raw_gtfs_current if hasattr(cfg, "raw_gtfs_current") else cfg.interim / "MISSING_gtfs_current", "*")
check_dir("Census shapefiles",            cfg.raw_census / "shapefiles", "*.shp")
check_dir("Census ACS",                   cfg.raw_census / "acs",   "*.csv")
check_dir("TLC zones shapefile",          cfg.external / "nyc_zones", "*.shp")

# ── STAGE 01 — CLEANING ───────────────────────────────────────────────────────
print(f"\n{SEP}")
print("STAGE 01 — DATA CLEANING  (src/01_data_cleaning/01_cleaning/)")
print(SEP)

# Citi Bike
check("citibike_clean.parquet",
      cfg.interim / "citibike" / "citibike_clean.parquet",
      parquet_info)
check("citibike_tlc.parquet",
      cfg.interim / "citibike" / "citibike_tlc.parquet",
      parquet_info)

# Shards leftover check
shard_dir = cfg.interim / "citibike" / "_shards"
if shard_dir.exists():
    n = len(list(shard_dir.glob("*.parquet")))
    print(f"  [WARN]   _shards/ still present ({n} shard files) — merge may be incomplete")

# ── STAGE 02 — SPATIAL/TEMPORAL ALIGNMENT ────────────────────────────────────
print(f"\n{SEP}")
print("STAGE 02 — SPATIAL / TEMPORAL ALIGNMENT")
print(SEP)
check("mta_flows_estimated.parquet",
      cfg.interim / "spatial_alignment" / "mta_flows_estimated.parquet",
      parquet_info)
check("mta_flows_tlc.parquet",
      cfg.interim / "spatial_alignment" / "mta_flows_tlc.parquet",
      parquet_info)
check_dir("Temporal panels",
          cfg.interim / "temporal_panels", "*.parquet")

# ── STAGES 03–07 ──────────────────────────────────────────────────────────────
print(f"\n{SEP}")
print("STAGES 03–07 — NOT YET STARTED (expected)")
print(SEP)
for label, path in [
    ("Outside option",        cfg.processed / "outside_option.parquet"),
    ("Instruments",           cfg.processed / "instruments.parquet"),
    ("Simple Logit results",  cfg.tables / "simple_logit.tex"),
    ("BLP results",           cfg.tables / "blp.tex"),
    ("SDI (NLP)",             cfg.processed / "sdi.parquet"),
]:
    p = Path(path)
    status = "[OK]    " if p.exists() else "[--]    "
    print(f"  {status} {label:<50} {'exists' if p.exists() else 'not yet'}")

# ── OPEN QUESTIONS REMINDER ───────────────────────────────────────────────────
print(f"\n{SEP}")
print("OPEN QUESTIONS (unresolved — blocking indicated stage)")
print(SEP)
oqs = [
    ("OQ-1", "Market thickness / temporal granularity",       "blocks 02, 03, 05"),
    ("OQ-2", "Alternative characteristics full spec",         "blocks 02, 05"),
    ("OQ-3", "HVFHV surge pricing IV strategy",               "blocks 04, 05"),
    ("OQ-4", "Taxi price: regulated tariff treatment",        "not blocking MVP"),
    ("OQ-5", "Citi Bike pricing for member trips",            "blocks 01, 05"),
    ("OQ-6", "MTA OD design for Subway/Bus",                  "blocks 02, 05"),
    ("OQ-7", "Nest structure for Nested Logit",               "not blocking MVP"),
]
for code, desc, blocks in oqs:
    print(f"  {code}  {desc:<48} ({blocks})")

print(f"\n{SEP}\n")