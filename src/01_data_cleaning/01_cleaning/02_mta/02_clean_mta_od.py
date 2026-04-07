"""
02_clean_mta_od.py
------------------
Stage   : 01_data_cleaning / 01_cleaning
Input   : cfg.raw_mta  — cinque CSV MTA Subway Origin-Destination
            - MTA Subway Origin-Destination Ridership Estimate_ 2021__rapa-97zv.csv
            - MTA Subway Origin-Destination Ridership Estimate_ 2022__nqnz-e9z9.csv
            - MTA Subway Origin-Destination Ridership Estimate_ 2023__uhf3-t34z.csv
            - MTA Subway Origin-Destination Ridership Estimate_ 2024__jsu2-fbtj.csv
            - MTA Subway Origin-Destination Ridership Estimate_ Beginning 2025__y2qv-fytt.csv
Output  : cfg.interim / temporal_panels / mta_od_weights.parquet
            Colonne: origin_id | dest_id | year | month | dow | hour
                     avg_ridership | weight
                     origin_lat | origin_lon | dest_lat | dest_lon

Logica  : Per ogni (origin_id, year, month, dow, hour) calcola il peso
          condizionale di ciascuna destinazione:
              weight(A->B) = AvgRidership(A->B) / sum_B[ AvgRidership(A->B) ]
          I pesi sommano a 1 per ogni (origin, year, month, dow, hour).

Strategia memoria (due passaggi per file):
          Passaggio 1 -- legge il file a chunk e accumula solo i denominatori
              (groupby leggero: origin x contesto -> sum ridership).
              Il denominatore aggregato e piccolo (n_stazioni x n_contesti).
          Passaggio 2 -- rilegge il file a chunk, fa merge con il denominatore
              (ora piccolo) e calcola weight = avg_ridership / totale.
              Scrive ogni chunk direttamente nel parquet di output (append).
          In nessun momento l intero file e in RAM.

Note    : - I file sono grandi (fino a 7.5 GB): strategia due passaggi
          - Day of Week e stringa (Monday, Tuesday, ...): mantenuto as-is
          - Righe con AvgRidership <= 0 vengono scartate prima del calcolo pesi
          - Coordinate: portate direttamente dal raw, nessuna aggregazione
          - Sanity check rolling (no accumulo weight_sums in RAM)
"""

import importlib.util
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Config
_cfg_path = Path(__file__).parents[4] / "00_config.py"
_spec = importlib.util.spec_from_file_location("config", _cfg_path)
_mod  = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
cfg = _mod.cfg

RAW = Path(cfg.raw_mta)
OUT = Path(cfg.interim) / "temporal_panels"
OUT.mkdir(parents=True, exist_ok=True)
OUT_PATH = OUT / "mta_od_weights.parquet"

OD_FILES = [
    "MTA Subway Origin-Destination Ridership Estimate_ 2021__rapa-97zv.csv",
    "MTA Subway Origin-Destination Ridership Estimate_ 2022__nqnz-e9z9.csv",
    "MTA Subway Origin-Destination Ridership Estimate_ 2023__uhf3-t34z.csv",
    "MTA Subway Origin-Destination Ridership Estimate_ 2024__jsu2-fbtj.csv",
    "MTA Subway Origin-Destination Ridership Estimate_ Beginning 2025__y2qv-fytt.csv",
]

USECOLS = [
    "Year", "Month", "Day of Week", "Hour of Day",
    "Origin Station Complex ID", "Destination Station Complex ID",
    "Estimated Average Ridership",
    "Origin Latitude", "Origin Longitude",
    "Destination Latitude", "Destination Longitude",
]

RENAME = {
    "Year":                           "year",
    "Month":                          "month",
    "Day of Week":                    "dow",
    "Hour of Day":                    "hour",
    "Origin Station Complex ID":      "origin_id",
    "Destination Station Complex ID": "dest_id",
    "Estimated Average Ridership":    "avg_ridership",
    "Origin Latitude":                "origin_lat",
    "Origin Longitude":               "origin_lon",
    "Destination Latitude":           "dest_lat",
    "Destination Longitude":          "dest_lon",
}

CONTEXT_KEYS = ["origin_id", "year", "month", "dow", "hour"]
CHUNKSIZE    = 2_000_000


def apply_types(df):
    df["origin_id"]     = df["origin_id"].astype("int32")
    df["dest_id"]       = df["dest_id"].astype("int32")
    df["year"]          = df["year"].astype("int16")
    df["month"]         = df["month"].astype("int8")
    df["hour"]          = df["hour"].astype("int8")
    df["avg_ridership"] = df["avg_ridership"].astype("float32")
    df["weight"]        = df["weight"].astype("float32")
    df["origin_lat"]    = df["origin_lat"].astype("float32")
    df["origin_lon"]    = df["origin_lon"].astype("float32")
    df["dest_lat"]      = df["dest_lat"].astype("float32")
    df["dest_lon"]      = df["dest_lon"].astype("float32")
    return df


# Parquet writer incrementale (aperto al primo chunk)
writer = None
total_rows_written = 0
total_weight_devs  = []

for fname in OD_FILES:
    fpath = RAW / fname
    print(f"\n{'='*60}")
    print(f"File: {fname}")
    print(f"Dimensione: {fpath.stat().st_size / 1_048_576:.0f} MB")

    # PASSAGGIO 1: denominatori
    print("  Passaggio 1/2 — calcolo denominatori...")
    denom_parts = []
    reader1 = pd.read_csv(fpath, usecols=USECOLS, chunksize=CHUNKSIZE,
                          low_memory=False)
    for chunk in reader1:
        chunk = chunk.rename(columns=RENAME)
        chunk = chunk[chunk["avg_ridership"] > 0]
        if chunk.empty:
            continue
        chunk["origin_id"] = chunk["origin_id"].astype("int32")
        chunk["year"]      = chunk["year"].astype("int16")
        chunk["month"]     = chunk["month"].astype("int8")
        chunk["hour"]      = chunk["hour"].astype("int8")
        part = (
            chunk
            .groupby(CONTEXT_KEYS, as_index=False)["avg_ridership"]
            .sum()
            .rename(columns={"avg_ridership": "total_from_origin"})
        )
        denom_parts.append(part)

    denom = (
        pd.concat(denom_parts, ignore_index=True)
        .groupby(CONTEXT_KEYS, as_index=False)["total_from_origin"]
        .sum()
    )
    denom["origin_id"] = denom["origin_id"].astype("int32")
    denom["year"]      = denom["year"].astype("int16")
    denom["month"]     = denom["month"].astype("int8")
    denom["hour"]      = denom["hour"].astype("int8")
    print(f"  Denominatori unici: {len(denom):,}  "
          f"(~{denom.memory_usage(deep=True).sum() / 1_048_576:.1f} MB in RAM)")
    del denom_parts

    # PASSAGGIO 2: pesi e scrittura chunk per chunk
    print("  Passaggio 2/2 — calcolo pesi e scrittura...")
    file_rows       = 0
    max_dev_running = 0.0  # sanity check rolling -- nessun accumulo in RAM

    reader2 = pd.read_csv(fpath, usecols=USECOLS, chunksize=CHUNKSIZE,
                          low_memory=False)
    for i, chunk in enumerate(reader2):
        chunk = chunk.rename(columns=RENAME)
        chunk = chunk[chunk["avg_ridership"] > 0].copy()
        if chunk.empty:
            continue

        chunk["origin_id"] = chunk["origin_id"].astype("int32")
        chunk["dest_id"]   = chunk["dest_id"].astype("int32")
        chunk["year"]      = chunk["year"].astype("int16")
        chunk["month"]     = chunk["month"].astype("int8")
        chunk["hour"]      = chunk["hour"].astype("int8")

        # Merge con denominatori (piccolo -> veloce, nessun OOM)
        chunk = chunk.merge(denom, on=CONTEXT_KEYS, how="left")
        chunk["weight"] = chunk["avg_ridership"] / chunk["total_from_origin"]
        chunk = chunk.drop(columns=["total_from_origin"])

        chunk = apply_types(chunk)

        # Sanity check rolling (evita accumulo in RAM)
        w_dev = (chunk.groupby(CONTEXT_KEYS)["weight"].sum() - 1.0).abs().max()
        if w_dev > max_dev_running:
            max_dev_running = w_dev

        # Scrittura incrementale parquet
        table = pa.Table.from_pandas(chunk, preserve_index=False)
        if writer is None:
            writer = pq.ParquetWriter(OUT_PATH, table.schema)
        writer.write_table(table)

        file_rows += len(chunk)
        if (i + 1) % 5 == 0:
            print(f"    chunk {i+1} — righe scritte: {file_rows:,}")

    total_weight_devs.append(max_dev_running)
    print(f"  -> righe scritte: {file_rows:,}")
    print(f"  Deviazione massima dalla somma=1: {max_dev_running:.2e}")
    total_rows_written += file_rows
    del denom

# Chiudi writer
if writer is not None:
    writer.close()

# Diagnostica finale
print(f"\n{'='*60}")
print(f"Righe totali scritte     : {total_rows_written:,}")
print(f"Dev. max somma=1 per anno: {[f'{d:.2e}' for d in total_weight_devs]}")

# Diagnostica leggera: legge solo il primo row group del parquet
pf = pq.ParquetFile(OUT_PATH)
print(f"Row groups nel file      : {pf.metadata.num_row_groups}")
print(f"Schema colonne           : {pf.schema_arrow.names}")

sample = pf.read_row_group(0, columns=["origin_id","dest_id","year",
                                        "month","dow","hour",
                                        "avg_ridership","weight"]).to_pandas()
print(f"\nCampione (primo row group): {len(sample):,} righe")
print(f"Anni nel campione        : {sorted(sample['year'].unique())}")
print(f"Weight min / max         : {sample['weight'].min():.6f} / {sample['weight'].max():.6f}")
print(f"Righe con weight NaN     : {sample['weight'].isna().sum():,}")
print("\nPrime 5 righe:")
print(sample.head().to_string(index=False))
print(f"\nSalvato in: {OUT_PATH}")
print(f"Dimensione file: {OUT_PATH.stat().st_size / 1_048_576:.1f} MB")