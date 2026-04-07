"""
01_clean_weather_isd.py
Pipeline stage : 01_data_cleaning / 01_cleaning
Input          : cfg.raw_weather / isd_<station_name>_<YEAR>.csv  (raw ISD Global Hourly)
Output         : cfg.raw_weather / isd_<station_name>_clean.parquet  (uno per stazione)

Per ogni stazione:
  - Legge tutti gli anni disponibili
  - Filtra: solo REPORT_TYPE FM-15 e FM-16 (drop SOD, SOM, ecc.)
  - Fa il parse delle colonne packed: TMP, DEW, SLP, WND, VIS, CIG, AA1, AJ1
  - Sostituisce i sentinel values con NaN
  - Localizza DATE a America/New_York → converte in UTC
  - Scrive un parquet pulito per stazione

Colonne output:
  datetime_utc, station_id, station_name,
  temp_c, dewpoint_c, slp_hpa,
  wind_dir_deg, wind_speed_ms,
  visibility_m, ceiling_ft,
  precip_mm, snow_depth_mm,
  report_type, source_file

Usage:
  python src/01_data_cleaning/01_cleaning/01_clean_weather_isd.py

Requirements:
  pip install pandas pyarrow
"""

import importlib.util
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ── 0. Config ─────────────────────────────────────────────────────────────────
_cfg_path = Path(__file__).parents[4] / "00_config.py"
_spec = importlib.util.spec_from_file_location("config", _cfg_path)
_mod  = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
cfg = _mod.cfg

RAW_DIR = Path(cfg.raw_weather)
OUT_DIR = Path(cfg.raw_weather)   # stessa cartella, file diverso

STATIONS = ["central_park", "laguardia", "jfk"]

# Solo report orari; SOD = daily summary, SOM = monthly summary
KEEP_REPORT_TYPES = {"FM-15", "FM-16"}

# Schema PyArrow canonico — definito upfront per evitare null-type inference
OUT_SCHEMA = pa.schema([
    ("datetime_utc",  pa.timestamp("us", tz="UTC")),
    ("station_id",    pa.int64()),
    ("station_name",  pa.string()),
    ("temp_c",        pa.float32()),
    ("dewpoint_c",    pa.float32()),
    ("slp_hpa",       pa.float32()),
    ("wind_dir_deg",  pa.float32()),
    ("wind_speed_ms", pa.float32()),
    ("visibility_m",  pa.float32()),
    ("ceiling_ft",    pa.float32()),
    ("precip_mm",     pa.float32()),
    ("snow_depth_mm", pa.float32()),
    ("report_type",   pa.string()),
    ("source_file",   pa.string()),
])

# ── 1. Parser delle colonne packed ────────────────────────────────────────────

def parse_tmp(series: pd.Series) -> pd.Series:
    """'+0067,5' → 6.7  |  '+9999,9' → NaN"""
    val = series.str.split(",", n=1).str[0]
    num = pd.to_numeric(val, errors="coerce") / 10.0
    num[num == 999.9] = float("nan")
    return num.astype("float32")

def parse_slp(series: pd.Series) -> pd.Series:
    """'10028,5' → 1002.8  |  '99999,9' → NaN"""
    val = series.str.split(",", n=1).str[0]
    num = pd.to_numeric(val, errors="coerce") / 10.0
    num[num == 9999.9] = float("nan")
    return num.astype("float32")

def parse_wnd_dir(series: pd.Series) -> pd.Series:
    """'200,5,N,0036,5' → 200.0  |  dir==999 → NaN"""
    val = series.str.split(",", n=1).str[0]
    num = pd.to_numeric(val, errors="coerce")
    num[num == 999] = float("nan")
    return num.astype("float32")

def parse_wnd_speed(series: pd.Series) -> pd.Series:
    """'200,5,N,0036,5' → 3.6 m/s  |  speed==9999 → NaN"""
    val = series.str.split(",").str[3]
    num = pd.to_numeric(val, errors="coerce") / 10.0
    num[num == 999.9] = float("nan")
    return num.astype("float32")

def parse_vis(series: pd.Series) -> pd.Series:
    """'016093,5,N,5' → 16093.0  |  999999 → NaN"""
    val = series.str.split(",", n=1).str[0]
    num = pd.to_numeric(val, errors="coerce")
    num[num >= 999999] = float("nan")
    return num.astype("float32")

def parse_cig(series: pd.Series) -> pd.Series:
    """'01494,5,M,N' → 1494.0  |  99999 → NaN"""
    val = series.str.split(",", n=1).str[0]
    num = pd.to_numeric(val, errors="coerce")
    num[num >= 99999] = float("nan")
    return num.astype("float32")

def parse_aa1(series: pd.Series) -> pd.Series:
    """'01,0005,9,5' → 0.5 mm  |  '9999' nel campo amount → NaN"""
    # formato: period_hours,amount,condition,flag
    # amount è in 1/10 mm
    val = series.str.split(",").str[1]
    num = pd.to_numeric(val, errors="coerce") / 10.0
    num[num >= 999.9] = float("nan")
    return num.astype("float32")

def parse_aj1(series: pd.Series) -> pd.Series:
    """'0000,9,5,999999,9,9' → 0.0 mm  |  primo campo 9999 → NaN"""
    val = series.str.split(",", n=1).str[0]
    num = pd.to_numeric(val, errors="coerce")
    num[num >= 9999] = float("nan")
    return num.astype("float32")

# ── 2. Funzione principale per una stazione ───────────────────────────────────

def clean_station(station_name: str) -> int:
    """
    Legge tutti i CSV raw per la stazione, li pulisce e scrive il parquet.
    Ritorna il numero totale di righe scritte.
    """
    csv_files = sorted(RAW_DIR.glob(f"isd_{station_name}_*.csv"))
    if not csv_files:
        print(f"  [warn] nessun file trovato per {station_name}")
        return 0

    out_path = OUT_DIR / f"isd_{station_name}_clean.parquet"
    writer   = None
    total    = 0

    for csv_path in csv_files:
        print(f"    Elaborazione {csv_path.name} ...", end=" ")

        # Leggi — SOURCE può essere object in alcuni anni (jfk_2024 contiene "O")
        df = pd.read_csv(csv_path, low_memory=False, dtype={"SOURCE": str})

        # ── Filtro REPORT_TYPE ──────────────────────────────────────────────
        df["REPORT_TYPE"] = df["REPORT_TYPE"].str.strip()
        df = df[df["REPORT_TYPE"].isin(KEEP_REPORT_TYPES)].copy()

        if df.empty:
            print("0 righe dopo filtro REPORT_TYPE, skip")
            continue

        # ── Timestamp ──────────────────────────────────────────────────────
        df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
        df = df.dropna(subset=["DATE"])
        df["DATE"] = (
            df["DATE"]
            .dt.tz_localize("America/New_York", ambiguous="NaT", nonexistent="NaT")
            .dt.tz_convert("UTC")
        )
        df = df.dropna(subset=["DATE"])

        # ── Parse colonne meteorologiche ────────────────────────────────────
        out = pd.DataFrame()
        out["datetime_utc"]  = df["DATE"]
        out["station_id"]    = pd.to_numeric(df["STATION"], errors="coerce").astype("Int64")
        out["station_name"]  = station_name
        out["temp_c"]        = parse_tmp(df["TMP"])      if "TMP" in df.columns else float("nan")
        out["dewpoint_c"]    = parse_tmp(df["DEW"])      if "DEW" in df.columns else float("nan")
        out["slp_hpa"]       = parse_slp(df["SLP"])      if "SLP" in df.columns else float("nan")
        out["wind_dir_deg"]  = parse_wnd_dir(df["WND"])  if "WND" in df.columns else float("nan")
        out["wind_speed_ms"] = parse_wnd_speed(df["WND"])if "WND" in df.columns else float("nan")
        out["visibility_m"]  = parse_vis(df["VIS"])      if "VIS" in df.columns else float("nan")
        out["ceiling_ft"]    = parse_cig(df["CIG"])      if "CIG" in df.columns else float("nan")
        out["precip_mm"]     = parse_aa1(df["AA1"])      if "AA1" in df.columns else float("nan")
        out["snow_depth_mm"] = parse_aj1(df["AJ1"])      if "AJ1" in df.columns else float("nan")
        out["report_type"]   = df["REPORT_TYPE"].values
        out["source_file"]   = csv_path.name

        # ── Cast a float32 per colonne float rimaste come nan scalare ───────
        for col in ["temp_c","dewpoint_c","slp_hpa","wind_dir_deg","wind_speed_ms",
                    "visibility_m","ceiling_ft","precip_mm","snow_depth_mm"]:
            if out[col].dtype != "float32":
                out[col] = out[col].astype("float32")

        # ── Scrivi incrementalmente ─────────────────────────────────────────
        table = pa.Table.from_pandas(out, schema=OUT_SCHEMA, safe=False)
        if writer is None:
            writer = pq.ParquetWriter(out_path, OUT_SCHEMA)
        writer.write_table(table)

        total += len(out)
        print(f"{len(out):,} righe")

    if writer:
        writer.close()
        size_mb = out_path.stat().st_size / 1_048_576
        print(f"  → {out_path.name}  ({total:,} righe totali, {size_mb:.1f} MB)")
    else:
        print(f"  [warn] nessun output scritto per {station_name}")

    return total


# ── 3. Main ───────────────────────────────────────────────────────────────────

def main():
    print(f"Input/Output dir: {RAW_DIR}\n")

    summary = {}
    for station in STATIONS:
        print(f"\n{'='*55}")
        print(f"Stazione: {station}")
        print(f"{'='*55}")
        n = clean_station(station)
        summary[station] = n

    print(f"\n{'='*55}")
    print("Riepilogo")
    print(f"{'='*55}")
    for station, n in summary.items():
        print(f"  {station:15s}  {n:>10,} righe")


if __name__ == "__main__":
    main()
