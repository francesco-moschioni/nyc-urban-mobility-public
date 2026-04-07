"""
00_inspect_weather_clean.py
Legge il primo row group di ogni parquet pulito e stampa un report rapido.
Output: salvato su file .txt nella stessa cartella dello script.

Usage:
  python src/01_data_cleaning/01_cleaning/00_inspect_weather_clean.py
"""

import importlib.util
from pathlib import Path
import pyarrow.parquet as pq
import pandas as pd

# ── Config ────────────────────────────────────────────────────────────────────
_cfg_path = Path(__file__).parents[4] / "00_config.py"
_spec = importlib.util.spec_from_file_location("config", _cfg_path)
_mod  = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
cfg = _mod.cfg

RAW_DIR  = Path(cfg.raw_weather)
STATIONS = ["central_park", "laguardia", "jfk"]

# ── Output file ───────────────────────────────────────────────────────────────
output_path = Path(__file__).parent / "inspect_weather_report.txt"
lines = []

def write(line=""):
    lines.append(line)

# ── Main ──────────────────────────────────────────────────────────────────────
for station in STATIONS:
    path = RAW_DIR / f"isd_{station}_clean.parquet"
    if not path.exists():
        write(f"\n[MANCANTE] {path.name}")
        continue

    pf      = pq.ParquetFile(path)
    n_rg    = pf.metadata.num_row_groups
    n_rows  = pf.metadata.num_rows
    size_mb = path.stat().st_size / 1_048_576
    df      = pf.read_row_group(0).to_pandas()

    write(f"\n{'='*60}")
    write(f"Stazione : {station}")
    write(f"File     : {path.name}  ({size_mb:.1f} MB)")
    write(f"Righe    : {n_rows:,}  |  Row group: {n_rg}")
    write(f"Periodo  : {df['datetime_utc'].min()}  →  {df['datetime_utc'].max()}")
    write(f"Schema   : {[f.name for f in pf.schema_arrow]}")
    write()

    # Fill rate colonne numeriche
    num_cols = ["temp_c","dewpoint_c","slp_hpa","wind_dir_deg",
                "wind_speed_ms","visibility_m","ceiling_ft","precip_mm","snow_depth_mm"]
    write(f"  {'Colonna':<18} {'non-null %':>10}  {'min':>8}  {'max':>8}  {'media':>8}")
    write(f"  {'-'*18} {'-'*10}  {'-'*8}  {'-'*8}  {'-'*8}")
    for col in num_cols:
        if col not in df.columns:
            write(f"  {col:<18} {'MANCANTE':>10}")
            continue
        s       = df[col]
        pct     = s.notna().mean() * 100
        mn      = f"{s.min():.1f}" if s.notna().any() else "—"
        mx      = f"{s.max():.1f}" if s.notna().any() else "—"
        mean    = f"{s.mean():.1f}" if s.notna().any() else "—"
        write(f"  {col:<18} {pct:>9.1f}%  {mn:>8}  {mx:>8}  {mean:>8}")

    # Anni coperti
    years = sorted(df["source_file"].str.extract(r"_(\d{4})\.csv")[0].dropna().unique())
    write(f"\n  Anni nel primo row group: {years}")

    # Sanity check: nessun timestamp duplicato?
    dupes = df["datetime_utc"].duplicated().sum()
    write(f"  Timestamp duplicati     : {dupes}")

# ── Scrittura file ────────────────────────────────────────────────────────────
with open(output_path, "w", encoding="utf-8") as f:
    f.write("\n".join(lines))

print(f"\nReport salvato in: {output_path}")