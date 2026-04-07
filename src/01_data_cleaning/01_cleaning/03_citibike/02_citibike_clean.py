"""
02_citibike_clean.py
Stage: src/01_data_cleaning/01_cleaning/
Purpose: Read all raw Citi Bike CSVs (2019-2025), normalise schema differences,
         standardise member_casual, drop bad rows, and write a single cleaned
         parquet to cfg.interim / "citibike" / "citibike_clean.parquet".

Memory strategy: each CSV is read, cleaned, and written to an individual
parquet shard. At the end the shards are merged into one final file via
PyArrow ParquetWriter (low memory). Peak RAM = one CSV at a time (~200 MB).

Schema fix: start_station_id and end_station_id are forced to string in the
canonical schema to handle mixed int/float/str across old and new schemas.
"""

import importlib.util
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import sys

# ── Config ────────────────────────────────────────────────────────────────────
_cfg_path = Path(__file__).parents[4] / "00_config.py"
_spec = importlib.util.spec_from_file_location("config", _cfg_path)
_mod  = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
cfg = _mod.cfg

RAW_DIR   = cfg.raw_citibike / "unzipped"
OUT_DIR   = cfg.interim / "citibike"
OUT_FILE  = OUT_DIR / "citibike_clean.parquet"
SHARD_DIR = OUT_DIR / "_shards"

YEAR_START = 2019
YEAR_END   = 2025

# Canonical schema — station IDs as string to handle mixed types across schemas
SCHEMA = pa.schema([
    pa.field("started_at",       pa.timestamp("us")),
    pa.field("ended_at",         pa.timestamp("us")),
    pa.field("start_lat",        pa.float64()),
    pa.field("start_lng",        pa.float64()),
    pa.field("end_lat",          pa.float64()),
    pa.field("end_lng",          pa.float64()),
    pa.field("start_station_id", pa.string()),
    pa.field("end_station_id",   pa.string()),
    pa.field("member_casual",    pa.string()),
    pa.field("rideable_type",    pa.string()),
    pa.field("source_file",      pa.string()),
])

# ── Column maps ───────────────────────────────────────────────────────────────
OLD_RENAME = {
    "starttime":                "started_at",
    "Start Time":               "started_at",
    "stoptime":                 "ended_at",
    "Stop Time":                "ended_at",
    "start station latitude":   "start_lat",
    "Start Station Latitude":   "start_lat",
    "start station longitude":  "start_lng",
    "Start Station Longitude":  "start_lng",
    "end station latitude":     "end_lat",
    "End Station Latitude":     "end_lat",
    "end station longitude":    "end_lng",
    "End Station Longitude":    "end_lng",
    "start station id":         "start_station_id",
    "Start Station ID":         "start_station_id",
    "end station id":           "end_station_id",
    "End Station ID":           "end_station_id",
    "usertype":                 "member_casual",
    "User Type":                "member_casual",
}

MEMBER_NORMALISE = {
    "Subscriber": "member",
    "Customer":   "casual",
    "member":     "member",
    "casual":     "casual",
}

KEEP_COLS = [
    "started_at", "ended_at",
    "start_lat", "start_lng",
    "end_lat", "end_lng",
    "start_station_id", "end_station_id",
    "member_casual", "rideable_type",
    "source_file",
]


# ── File collection ───────────────────────────────────────────────────────────
def collect_files(root: Path) -> list[Path]:
    files = []
    for entry in sorted(root.iterdir()):
        if not entry.is_dir():
            continue
        if entry.name.startswith("__") or entry.name.startswith("JC"):
            continue
        try:
            dir_year = int(entry.name[:4])
        except ValueError:
            continue
        if not (YEAR_START <= dir_year <= YEAR_END):
            continue
        for f in sorted(entry.rglob("*.csv")):
            if f.name.startswith("JC") or "__MACOSX" in str(f):
                continue
            files.append(f)
    for f in sorted(root.glob("*.csv")):
        if f.name.startswith("JC") or f.name.startswith("__"):
            continue
        try:
            file_year = int(f.name[:4])
        except ValueError:
            continue
        if YEAR_START <= file_year <= YEAR_END:
            files.append(f)
    return sorted(files, key=lambda p: p.name)


def already_sharded(files: list[Path], shard_dir: Path) -> tuple[list[Path], int]:
    """
    Return (remaining_files, next_shard_idx) by checking which shards
    already exist. Allows safe resume after a crash.
    """
    existing = sorted(shard_dir.glob("shard_*.parquet"))
    n_done = len(existing)
    if n_done == 0:
        return files, 0
    print(f"  Found {n_done} existing shards — resuming from file {n_done + 1}.")
    return files[n_done:], n_done


# ── Per-file processing ───────────────────────────────────────────────────────
def read_and_normalise(path: Path) -> pd.DataFrame | None:
    try:
        df = pd.read_csv(path, low_memory=False)
    except Exception as e:
        print(f"  [ERROR reading] {path.name}: {e}")
        return None

    df = df.rename(columns={k: v for k, v in OLD_RENAME.items() if k in df.columns})

    if "rideable_type" not in df.columns:
        df["rideable_type"] = "unknown"

    df["source_file"] = path.name

    available = [c for c in KEEP_COLS if c in df.columns]
    df = df[available].copy()

    for col in ["started_at", "ended_at"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    if "member_casual" in df.columns:
        df["member_casual"] = df["member_casual"].map(MEMBER_NORMALISE)
    else:
        df["member_casual"] = pd.NA

    # Force station IDs to string immediately — prevents int/float/str conflicts
    for col in ["start_station_id", "end_station_id"]:
        if col in df.columns:
            df[col] = df[col].astype(str).replace("nan", pd.NA)

    return df


def clean(df: pd.DataFrame):
    n0 = len(df)
    df = df.dropna(subset=["started_at"])
    df = df[df["started_at"].dt.year.between(YEAR_START, YEAR_END)]
    df = df.dropna(subset=["member_casual"])
    for col in ["start_lat", "start_lng", "end_lat", "end_lng"]:
        if col in df.columns:
            df = df.dropna(subset=[col])
            df = df[df[col] != 0.0]
    df["rideable_type"] = df["rideable_type"].str.strip().str.lower()
    df["rideable_type"] = df["rideable_type"].where(
        df["rideable_type"].isin(["classic_bike", "electric_bike"]), other="unknown"
    )
    n1 = len(df)
    return df, n0, n1


def df_to_table(df: pd.DataFrame) -> pa.Table:
    """Cast dataframe to canonical PyArrow schema."""
    cols = {}
    for field in SCHEMA:
        if field.name in df.columns:
            series = df[field.name]
            # Cast categoricals to plain python types first
            if hasattr(series, "cat"):
                series = series.astype(str)
            arr = pa.array(series, type=field.type, safe=False)
        else:
            arr = pa.nulls(len(df), type=field.type)
        cols[field.name] = arr
    return pa.table(cols, schema=SCHEMA)


# ── Shard merge ───────────────────────────────────────────────────────────────
def merge_shards(shard_dir: Path, out_file: Path) -> int:
    shard_files = sorted(shard_dir.glob("shard_*.parquet"))
    if not shard_files:
        print("No shards found to merge.")
        sys.exit(1)

    print(f"\nMerging {len(shard_files)} shards into {out_file} ...")

    if out_file.exists():
        out_file.unlink()

    writer = pq.ParquetWriter(out_file, SCHEMA, compression="snappy")
    total_rows = 0

    for i, sf in enumerate(shard_files):
        table = pq.read_table(sf)
        # Re-cast to canonical schema (handles any shard written before a fix)
        recasted_cols = {}
        for field in SCHEMA:
            col = table.column(field.name)
            recasted_cols[field.name] = col.cast(field.type, safe=False) if col.type != field.type else col
        table = pa.table(recasted_cols, schema=SCHEMA)
        total_rows += len(table)
        writer.write_table(table)
        del table
        if (i + 1) % 50 == 0:
            print(f"  ... {i+1}/{len(shard_files)} shards ({total_rows:,} rows)")

    writer.close()
    return total_rows


def delete_shards(shard_dir: Path):
    for f in shard_dir.glob("shard_*.parquet"):
        f.unlink()
    shard_dir.rmdir()
    print("Shards deleted.")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    SHARD_DIR.mkdir(parents=True, exist_ok=True)

    all_files = collect_files(RAW_DIR)
    if not all_files:
        print(f"No CSV files found in {RAW_DIR} for years {YEAR_START}-{YEAR_END}")
        sys.exit(1)

    remaining_files, shard_idx = already_sharded(all_files, SHARD_DIR)

    print(f"Total files      : {len(all_files)}")
    print(f"Already sharded  : {shard_idx}")
    print(f"To process       : {len(remaining_files)}")
    print(f"Output           : {OUT_FILE}\n")

    total_raw   = 0
    total_clean = 0

    for i, f in enumerate(remaining_files):
        global_idx = shard_idx + i + 1
        print(f"  [{global_idx:>3}/{len(all_files)}] {f.name} ...", end=" ")

        df_raw = read_and_normalise(f)
        if df_raw is None or df_raw.empty:
            print("SKIP (empty or error)")
            continue

        df_clean, n0, n1 = clean(df_raw)
        del df_raw

        total_raw   += n0
        total_clean += n1
        print(f"{n0:>10,} rows -> {n1:>10,} kept  (dropped {n0-n1:,})")

        if not df_clean.empty:
            shard_path = SHARD_DIR / f"shard_{shard_idx:04d}.parquet"
            table = df_to_table(df_clean)
            pq.write_table(table, shard_path, compression="snappy")
            del table
            shard_idx += 1

        del df_clean

    total_rows = merge_shards(SHARD_DIR, OUT_FILE)
    delete_shards(SHARD_DIR)

    print(f"\n{'='*60}")
    print(f"DONE")
    print(f"  Total raw rows processed : {total_raw:>12,}")
    print(f"  Total clean rows written : {total_clean:>12,}")
    if total_raw > 0:
        print(f"  Drop rate                : {(total_raw - total_clean) / total_raw:.2%}")
    print(f"  Output file              : {OUT_FILE}")
    print(f"  Output size              : {OUT_FILE.stat().st_size / 1e6:.1f} MB")

    # Summary stats — read only light columns
    print(f"\nSummary stats (reading 3 columns) ...")
    df_check = pd.read_parquet(
        OUT_FILE, columns=["member_casual", "rideable_type", "started_at"]
    )
    print(f"\nmember_casual:\n{df_check['member_casual'].value_counts()}")
    print(f"\nrideable_type:\n{df_check['rideable_type'].value_counts()}")
    print(f"\nYear distribution:\n{df_check['started_at'].dt.year.value_counts().sort_index()}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()