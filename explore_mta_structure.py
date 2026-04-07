"""
explore_mta_structure.py
------------------------
Esegui questo script UNA VOLTA per verificare la struttura dei dataset MTA.
Incolla l'output nella chat con Claude per procedere con la pipeline.

Posizionamento: puoi eseguirlo da qualsiasi cartella, carica cfg automaticamente.
"""

import importlib.util
from pathlib import Path
import os
import pandas as pd

# ── Carica cfg ──────────────────────────────────────────────────────────────
_cfg_path = Path(__file__).parents[0] / "00_config.py"
if not _cfg_path.exists():
    # Fallback: cerca 00_config.py risalendo l'albero
    for p in Path(__file__).parents:
        candidate = p / "00_config.py"
        if candidate.exists():
            _cfg_path = candidate
            break

_spec = importlib.util.spec_from_file_location("config", _cfg_path)
_mod  = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
cfg = _mod.cfg

# ── Lista file in raw/mta ────────────────────────────────────────────────────
mta_dir = Path(cfg.raw_mta)
print("=" * 60)
print(f"FILES IN {mta_dir}")
print("=" * 60)
files = sorted(mta_dir.glob("*.csv"))
for f in files:
    size_mb = f.stat().st_size / 1_048_576
    print(f"  {f.name:60s}  {size_mb:8.1f} MB")

print()

# ── Funzione di ispezione ────────────────────────────────────────────────────
def inspect(label: str, path: Path, nrows: int = 3):
    print("=" * 60)
    print(f"{label}")
    print(f"  File: {path.name}")
    print("=" * 60)
    try:
        df = pd.read_csv(path, nrows=nrows, low_memory=False)
        print(f"\nShape (prime {nrows} righe): {df.shape}")
        print("\nColonne e dtype:")
        for col, dtype in df.dtypes.items():
            sample = df[col].iloc[0] if len(df) > 0 else "—"
            print(f"  {col:40s}  {str(dtype):10s}  es: {sample}")
        print(f"\nPrime {nrows} righe:")
        print(df.to_string(index=False))

        # Conta righe totali
        total = sum(1 for _ in open(path, encoding="utf-8", errors="replace")) - 1
        print(f"\nRighe totali (escluso header): {total:,}")

    except Exception as e:
        print(f"  ERRORE: {e}")
    print()


# ── Cerca automaticamente i file entries e OD ────────────────────────────────
# Pattern comuni nei nomi MTA Socrata
entries_candidates = [f for f in files if any(
    kw in f.name.lower() for kw in ["entr", "turnstile", "ridership", "hourly"]
)]
od_candidates = [f for f in files if any(
    kw in f.name.lower() for kw in ["od", "origin", "destination", "o_d"]
)]

# Se non trovati automaticamente, prende tutti i CSV
if not entries_candidates:
    entries_candidates = files
if not od_candidates:
    od_candidates = files

print("\n>>> CANDIDATI ENTRIES:", [f.name for f in entries_candidates])
print(">>> CANDIDATI OD:     ", [f.name for f in od_candidates])
print()

# Ispeziona tutti i file trovati
all_inspected = set()
for f in entries_candidates + od_candidates:
    if f not in all_inspected:
        inspect(f"DATASET: {f.name}", f, nrows=3)
        all_inspected.add(f)

# ── Analisi colonne temporali e categoriche ──────────────────────────────────
print("=" * 60)
print("ANALISI VALORI UNICI — colonne temporali e tipo abbonamento")
print("=" * 60)

for f in all_inspected:
    try:
        df = pd.read_csv(f, nrows=50_000, low_memory=False)
        print(f"\n--- {f.name} ---")
        for col in df.columns:
            col_lower = col.lower()
            if any(kw in col_lower for kw in [
                "hour", "time", "date", "day", "month", "year",
                "fare", "payment", "type", "category", "class",
                "transit", "metro", "subway"
            ]):
                n_unique = df[col].nunique()
                sample_vals = df[col].dropna().unique()[:8].tolist()
                print(f"  {col:40s}  nunique={n_unique:6d}  vals={sample_vals}")
    except Exception as e:
        print(f"  ERRORE su {f.name}: {e}")

print("\n\n>>> COPIA TUTTO L'OUTPUT E INCOLLALO NELLA CHAT CON CLAUDE <<<\n")
