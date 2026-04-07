"""
01_mta_flows.py
---------------
Stage   : 02_spatial_temporal_alignment
Input   : cfg.interim / temporal_panels / mta_entries_clean.parquet
          cfg.interim / temporal_panels / mta_od_weights.parquet
Output  : cfg.interim / spatial_alignment / mta_flows_estimated.parquet
            Colonne: origin_id | dest_id | date | hour | fare_class_category
                     | estimated_flow | origin_lat | origin_lon
                     | dest_lat | dest_lon

Logica  : Per ogni riga degli entries (stazione A, data specifica, ora,
          fare_class_category):
            1. Ricava dow e month dalla data
            2. Join con i pesi OD su (origin_id, year, month, dow, hour)
               -- i pesi sono identici per tutte le fare class, perche la OD
               non ha questa dimensione.
            3. estimated_flow = ridership(fare_class) x weight(A->B | contesto)

Strategia memoria:
          - Entries: caricato tutto in RAM (43M righe x poche colonne ~ gestibile)
          - OD weights: caricato un mese alla volta via pyarrow filter pushdown
          - Merge: eseguito per sotto-chunk di stazioni (STATIONS_PER_CHUNK)
            per evitare OOM sui mesi con molte OD pairs (es. apr 2024: 5.8M pesi)
          - Output: scritto chunk per chunk con ParquetWriter incrementale
          - Diagnostica: campionaria sul primo row group

TODO    : Step successivo -- aggrega flussi da station->station a
          tlc_zone->tlc_zone via spatial join.
          Script: src/02_spatial_temporal_alignment/02_mta_flows_to_tlc.py
"""

import importlib.util
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np

# Config
_cfg_path = Path(__file__).parents[4] / "00_config.py"
_spec = importlib.util.spec_from_file_location("config", _cfg_path)
_mod  = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
cfg = _mod.cfg

INTERIM    = Path(cfg.interim)
IN_ENTRIES = INTERIM / "temporal_panels"  / "mta_entries_clean.parquet"
IN_OD      = INTERIM / "temporal_panels"  / "mta_od_weights.parquet"
OUT_DIR    = INTERIM / "spatial_alignment"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_PATH   = OUT_DIR / "mta_flows_estimated.parquet"

DOW_MAP = {
    0: "Monday", 1: "Tuesday", 2: "Wednesday", 3: "Thursday",
    4: "Friday",  5: "Saturday", 6: "Sunday",
}

OD_COLS = ["origin_id", "dest_id", "year", "month", "dow", "hour",
           "weight", "origin_lat", "origin_lon", "dest_lat", "dest_lon"]

# Numero di stazioni origine processate per volta nel merge.
# Con ~5.8M pesi OD per mese e ~113 dest per origine, ogni chunk di stazioni
# produce al massimo STATIONS_PER_CHUNK x 113 x n_fare_class righe.
# 50 stazioni x 113 dest x 12 fare_class x 30 giorni x 24 ore ~ gestibile.
STATIONS_PER_CHUNK = 10

# Carico entries (43M righe x poche colonne -- entra in RAM)
print("Carico entries...")
entries = pd.read_parquet(IN_ENTRIES)
entries["date"]  = pd.to_datetime(entries["date"])
entries["year"]  = entries["date"].dt.year.astype("int16")
entries["month"] = entries["date"].dt.month.astype("int8")
entries["dow"]   = entries["date"].dt.dayofweek.map(DOW_MAP)
entries["hour"]  = entries["hour"].astype("int8")
print(f"  Entries: {len(entries):,} righe")
print(f"  Periodo: {entries['date'].min().date()} -> {entries['date'].max().date()}")

# Lista periodi da processare
periods = (
    entries[["year", "month"]]
    .drop_duplicates()
    .sort_values(["year", "month"])
    .values.tolist()
)
print(f"\nPeriodi da processare: {len(periods)}")

od_pf = pq.ParquetFile(IN_OD)
print(f"OD weights: {od_pf.metadata.num_rows:,} righe totali "
      f"({od_pf.metadata.num_row_groups} row groups)")

# Schema di output fisso — definito al primo chunk scritto
writer         = None
total_matched   = 0
total_unmatched = 0

OUT_COLS = [
    "origin_id", "dest_id", "date", "hour", "fare_class_category",
    "estimated_flow", "origin_lat", "origin_lon", "dest_lat", "dest_lon",
]


def write_chunk(df, writer_ref):
    """Applica tipi, converte in Arrow, scrive. Ritorna writer aggiornato."""
    df["estimated_flow"] = df["estimated_flow"].astype("float32")
    df["origin_lat"]     = df["origin_lat"].astype("float32")
    df["origin_lon"]     = df["origin_lon"].astype("float32")
    df["dest_lat"]       = df["dest_lat"].astype("float32")
    df["dest_lon"]       = df["dest_lon"].astype("float32")
    df["origin_id"]      = df["origin_id"].astype("int32")
    df["dest_id"]        = df["dest_id"].fillna(-1).astype("int32")
    table = pa.Table.from_pandas(df[OUT_COLS], preserve_index=False)
    if writer_ref[0] is None:
        writer_ref[0] = pq.ParquetWriter(OUT_PATH, table.schema)
    writer_ref[0].write_table(table)
    del table


# Uso lista mutabile come riferimento al writer per passarlo alla funzione
writer_ref = [None]

for year, month in periods:
    mask_e  = (entries["year"] == year) & (entries["month"] == month)
    e_month = entries[mask_e]

    # Carica OD di questo mese
    od_table = pq.read_table(
        IN_OD,
        columns=OD_COLS,
        filters=[("year", "=", year), ("month", "=", month)],
    )
    od_month = od_table.to_pandas()
    del od_table

    if od_month.empty:
        print(f"  {year}-{month:02d}: no OD disponibile, skip.")
        total_unmatched += len(e_month)
        continue

    od_month["origin_id"] = od_month["origin_id"].astype("int32")
    od_month["dest_id"]   = od_month["dest_id"].astype("int32")

    # Stazioni presenti negli entries di questo mese
    stations = e_month["station_complex_id"].unique()
    n_chunks  = int(np.ceil(len(stations) / STATIONS_PER_CHUNK))

    n_matched_month   = 0
    n_unmatched_month = 0

    for i, sta_chunk in enumerate(np.array_split(stations, n_chunks)):
        e_chunk = e_month[e_month["station_complex_id"].isin(sta_chunk)].copy()

        # OD filtrata sulle sole stazioni origine di questo chunk
        od_chunk = od_month[od_month["origin_id"].isin(sta_chunk)]

        merged = e_chunk.merge(
            od_chunk,
            left_on=["station_complex_id", "year", "month", "dow", "hour"],
            right_on=["origin_id",          "year", "month", "dow", "hour"],
            how="left",
        )

        merged["estimated_flow"] = merged["ridership"] * merged["weight"]

        n_un = int(merged["weight"].isna().sum())
        n_ma = len(merged) - n_un
        n_matched_month   += n_ma
        n_unmatched_month += n_un

        merged["origin_id"] = merged["origin_id"].fillna(
            merged["station_complex_id"]
        )

        write_chunk(merged, writer_ref)
        del merged, e_chunk, od_chunk

    total_matched   += n_matched_month
    total_unmatched += n_unmatched_month
    del od_month

    print(f"  {year}-{month:02d}: "
          f"{len(e_month):>8,} entries -> "
          f"{n_matched_month:>10,} righe flusso | "
          f"no-match: {n_unmatched_month:,}")

if writer_ref[0] is not None:
    writer_ref[0].close()

# Diagnostica finale campionaria
print(f"\n-- Diagnostica (campione primo row group) ----------")
pf_out = pq.ParquetFile(OUT_PATH)
print(f"Righe totali scritte     : {total_matched:,}")
print(f"Righe senza match OD     : {total_unmatched:,}  "
      f"({100*total_unmatched/(total_matched+total_unmatched+1e-9):.1f}%)")
print(f"Row groups nel file      : {pf_out.metadata.num_row_groups}")

sample = pf_out.read_row_group(0).to_pandas()
print(f"Campione (row group 0)   : {len(sample):,} righe")
print(f"Fare class nel campione  : {sorted(sample['fare_class_category'].dropna().unique())}")
print(f"Flusso medio campione    : {sample['estimated_flow'].mean():.4f}")
print(f"Flusso max campione      : {sample['estimated_flow'].max():.1f}")
print("\nPrime 5 righe:")
print(sample.head().to_string(index=False))

print(f"\nSalvato in: {OUT_PATH}")
print(f"Dimensione file: {OUT_PATH.stat().st_size / 1_048_576:.1f} MB")