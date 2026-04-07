"""
01_clean_mta_entries.py
-----------------------
Stage   : 01_data_cleaning / 01_cleaning
Input   : cfg.raw_mta  — tre CSV MTA Subway Hourly Ridership
            - MTA Subway Hourly Ridership_ 2017-2019__t69i-h2me.csv
            - MTA Subway Hourly Ridership_ 2020-2024__wujg-7c2s.csv
            - MTA Subway Hourly Ridership_ Beginning 2025__5wq4-mkjj.csv
Output  : cfg.interim / temporal_panels / mta_entries_clean.parquet
            Colonne: station_complex_id | date | hour | fare_class_category
                     | ridership | latitude | longitude

Note    : - Filtro transit_mode == 'subway' (esclude tram e staten_island_railway)
          - Periodo tenuto: 2021-01-01 → 2025-12-31 (allineato alla OD)
          - ridership NON aggregato su fare_class_category: ogni combinazione
            (station, date, hour, fare_class) è una riga separata.
            Questo permette di scegliere a posteriori se aggregare o stratificare.
          - Le 12 fare_class_category presenti nel raw sono:
              OMNY: Full Fare, Seniors & Disability, Students, Other
              Metrocard: Full Fare, Unlimited 7-Day, Unlimited 30-Day,
                         Seniors & Disability, Students, Fair Fare, Other
            (la lista esatta può variare leggermente per anno)
          - station_complex_id nel file 2017-2019 è object (es. 'TRAM1'),
            nel 2020-2024 è int64: cast a str per concatenazione sicura,
            poi si riconverte a int dopo il filtro subway (che esclude i TRAM*)
          - Coordinate: costruite come lookup separata (mediana globale per
            stazione su tutti e tre i file) e poi mergeate alla fine.
            NON entrano mai nell'aggregazione per chunk — alcuni station_complex
            (es. Times Sq, Grand Central) hanno coordinate diverse per
            ingresso/linea e la mediana per chunk produrrebbe valori instabili.
"""

import importlib.util
from pathlib import Path
import pandas as pd

# ── Config ───────────────────────────────────────────────────────────────────
_cfg_path = Path(__file__).parents[4] / "00_config.py"
_spec = importlib.util.spec_from_file_location("config", _cfg_path)
_mod  = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
cfg = _mod.cfg

RAW   = Path(cfg.raw_mta)
OUT   = Path(cfg.interim) / "temporal_panels"
OUT.mkdir(parents=True, exist_ok=True)

# ── File da caricare ──────────────────────────────────────────────────────────
ENTRY_FILES = [
    "MTA Subway Hourly Ridership_ 2017-2019__t69i-h2me.csv",
    "MTA Subway Hourly Ridership_ 2020-2024__wujg-7c2s.csv",
    "MTA Subway Hourly Ridership_ Beginning 2025__5wq4-mkjj.csv",
]

# Colonne che ci servono
USECOLS = [
    "transit_timestamp",
    "transit_mode",
    "station_complex_id",
    "fare_class_category",
    "ridership",
    "latitude",
    "longitude",
]

DATE_START = pd.Timestamp("2021-01-01")
DATE_END   = pd.Timestamp("2025-12-31 23:59:59")

# ── Passo 0 — Lookup table coordinate (costruita una volta sola) ──────────────
# Le coordinate vengono tenute SEPARATE dall'aggregazione ridership.
# Per ogni station_complex_id si calcola la mediana globale di lat/lon
# leggendo tutti e tre i file subway in un solo passaggio leggero
# (solo 3 colonne, nessun filtro temporale pesante).
# Questo garantisce una coordinata stabile per stazione indipendentemente
# da come i chunk dividono le righe.

print("Costruisco lookup coordinate stazioni...")
coord_chunks = []
for fname in ENTRY_FILES:
    fpath = RAW / fname
    reader = pd.read_csv(
        fpath,
        usecols=["transit_mode", "station_complex_id", "latitude", "longitude"],
        dtype={"station_complex_id": str},
        chunksize=2_000_000,
        low_memory=False,
    )
    for chunk in reader:
        chunk = chunk[chunk["transit_mode"] == "subway"].copy()
        if chunk.empty:
            continue
        chunk["station_complex_id"] = chunk["station_complex_id"].astype(int)
        coord_chunks.append(
            chunk[["station_complex_id", "latitude", "longitude"]]
        )

coords_raw = pd.concat(coord_chunks, ignore_index=True)
station_coords = (
    coords_raw
    .groupby("station_complex_id", as_index=False)
    .agg(latitude=("latitude", "median"), longitude=("longitude", "median"))
)
print(f"  Stazioni con coordinate: {len(station_coords):,}")
del coord_chunks, coords_raw

# ── Passo 1 — Aggregazione ridership (senza coordinate) ──────────────────────
chunks = []

for fname in ENTRY_FILES:
    fpath = RAW / fname
    print(f"\nCarico: {fname}")
    print(f"  Dimensione: {fpath.stat().st_size / 1_048_576:.0f} MB")

    reader = pd.read_csv(
        fpath,
        usecols=USECOLS,
        dtype={"station_complex_id": str},
        chunksize=2_000_000,
        low_memory=False,
    )

    file_rows = 0
    for i, chunk in enumerate(reader):

        # 1. Filtro modalità
        chunk = chunk[chunk["transit_mode"] == "subway"].copy()
        if chunk.empty:
            continue

        # 2. Parse timestamp
        chunk["transit_timestamp"] = pd.to_datetime(
            chunk["transit_timestamp"], format="%m/%d/%Y %I:%M:%S %p"
        )

        # 3. Filtro periodo 2021-2025
        chunk = chunk[
            (chunk["transit_timestamp"] >= DATE_START) &
            (chunk["transit_timestamp"] <= DATE_END)
        ]
        if chunk.empty:
            continue

        # 4. Estrai date e ora
        chunk["date"] = chunk["transit_timestamp"].dt.date
        chunk["hour"] = chunk["transit_timestamp"].dt.hour

        # 5. station_complex_id → int (ora sicuro: tram esclusi)
        chunk["station_complex_id"] = chunk["station_complex_id"].astype(int)

        # 6. Aggrega: somma ridership per (station, date, hour, fare_class)
        #    La fare_class_category rimane come dimensione — NON viene collassata.
        #    L'aggregazione qui serve solo a sommare eventuali duplicati
        #    intra-chunk (non dovrebbero esistere, ma per sicurezza).
        agg = (
            chunk
            .groupby(
                ["station_complex_id", "date", "hour", "fare_class_category"],
                as_index=False
            )
            .agg(ridership=("ridership", "sum"))
        )

        chunks.append(agg)
        file_rows += len(agg)

        if (i + 1) % 10 == 0:
            print(f"  chunk {i+1} processato — righe output finora: {file_rows:,}")

    print(f"  → righe aggregate per questo file: {file_rows:,}")

# ── Concatena ────────────────────────────────────────────────────────────────
# I tre file hanno periodi non sovrapposti (2017-2019 / 2020-2024 / 2025)
# quindi non serve deduplicare: concat diretto è sufficiente e molto più
# leggero in RAM rispetto a un groupby su 40+ milioni di righe.
print("\nConcat finale...")
df = pd.concat(chunks, ignore_index=True)

# ── Passo 2 — Merge coordinate dalla lookup ───────────────────────────────────
df = df.merge(station_coords, on="station_complex_id", how="left")

n_missing_coords = df["latitude"].isna().sum()
if n_missing_coords > 0:
    print(f"⚠ Stazioni senza coordinate: {n_missing_coords:,} righe")
else:
    print("✓ Tutte le stazioni hanno coordinate.")

# ── Tipi finali ───────────────────────────────────────────────────────────────
df["date"] = pd.to_datetime(df["date"])
df["hour"] = df["hour"].astype("int8")
df["ridership"] = df["ridership"].astype("int32")

# ── Diagnostica ───────────────────────────────────────────────────────────────
print("\n── Diagnostica ──────────────────────────────────────")
print(f"Righe totali       : {len(df):,}")
print(f"Stazioni uniche    : {df['station_complex_id'].nunique():,}")
print(f"Periodo            : {df['date'].min().date()} → {df['date'].max().date()}")
print(f"Ore uniche         : {sorted(df['hour'].unique())}")
print(f"Fare class uniche  : {sorted(df['fare_class_category'].unique())}")
print(f"Ridership totale   : {df['ridership'].sum():,}")
print(f"Righe con rid=0    : {(df['ridership'] == 0).sum():,}")
print("\nRidership per fare class (totale):")
print(df.groupby("fare_class_category")["ridership"].sum().sort_values(ascending=False).to_string())
print("\nPrime 5 righe:")
print(df.head().to_string(index=False))

# ── Salvataggio ───────────────────────────────────────────────────────────────
out_path = OUT / "mta_entries_clean.parquet"
df.to_parquet(out_path, index=False)
print(f"\n✓ Salvato in: {out_path}")
print(f"  Dimensione file: {out_path.stat().st_size / 1_048_576:.1f} MB")