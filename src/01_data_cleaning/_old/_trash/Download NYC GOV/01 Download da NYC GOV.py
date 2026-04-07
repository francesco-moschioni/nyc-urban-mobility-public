import json
import time
from pathlib import Path

import requests


# =========================
# CONFIG
# =========================
DOMAIN = "https://data.ny.gov"
OUT_DIR = Path(r"D:\Tesi\Nyc gov")
OUT_DIR.mkdir(parents=True, exist_ok=True)

DATASETS = {
    "MTA Subway Major Incidents: 2015-2019": "ereg-mcvp",
    "MTA Subway Major Incidents: 2020-2024": "j6d2-s8m2",
    "MTA Subway Major Incidents: Beginning 2025": "uqnw-2qfk",
    "MTA Service Alerts: 2012-2020": "3h5b-5ktz",
    "MTA Service Alerts: Beginning April 2020": "7kct-peq7",
    "MTA Subway Hourly Ridership: 2017-2019": "t69i-h2me",
    "MTA Subway Hourly Ridership: Beginning 2025": "5wq4-mkjj",
    "MTA Daily Ridership and Traffic: Beginning 2020": "sayj-mze2",
    "MTA Bus Hourly Ridership: 2020-2024": "kv7t-n8in",
    "MTA Bridges & Tunnels Hourly Traffic Rates: 2010-2025": "ebfx-2m7v",
    "MTA Subway Origin-Destination Ridership Estimate: Beginning 2025": "y2qv-fytt",
    "MTA Subway Origin-Destination Ridership Estimate: 2024": "jsu2-fbtj",
    "MTA Subway Origin-Destination Ridership Estimate: 2023": "uhf3-t34z",
    "MTA Subway Origin-Destination Ridership Estimate: 2022": "nqnz-e9z9",
    "MTA Subway Origin-Destination Ridership Estimate: 2021": "rapa-97zv",
}


# =========================
# DOWNLOAD LOGIC
# =========================
def safe_filename(name: str) -> str:
    # filename Windows-safe
    bad = '<>:"/\\|?*'
    for ch in bad:
        name = name.replace(ch, "_")
    name = name.replace("  ", " ").strip()
    return name


def download_socrata_jsonl(
    dataset_id: str,
    out_file: Path,
    page_size: int = 50000,
    sleep_s: float = 0.2,
    timeout_s: int = 120,
) -> int:
    """
    Scarica l'intero dataset da /resource/<id>.json paginando con $limit/$offset.
    Salva in NDJSON (.jsonl): un record JSON per riga.
    """
    session = requests.Session()
    endpoint = f"{DOMAIN}/resource/{dataset_id}.json"

    offset = 0
    total = 0

    with out_file.open("w", encoding="utf-8") as f:
        while True:
            params = {"$limit": page_size, "$offset": offset}
            r = session.get(endpoint, params=params, timeout=timeout_s)

            if r.status_code != 200:
                raise RuntimeError(
                    f"HTTP {r.status_code} for {dataset_id}\n"
                    f"URL: {r.url}\n"
                    f"Body (first 500 chars): {r.text[:500]}"
                )

            batch = r.json()
            if not batch:
                break

            for row in batch:
                f.write(json.dumps(row, ensure_ascii=False))
                f.write("\n")

            got = len(batch)
            total += got
            offset += got

            print(f"    +{got:,} (tot {total:,})")
            time.sleep(sleep_s)

    return total


# =========================
# UI (CONSOLE MENU)
# =========================
def print_menu(items: list[tuple[str, str]]):
    print("\nCosa vuoi scaricare?")
    print("  0) Esci")
    print("  A) Scarica TUTTO")
    print("  M) Selezione multipla (es: 1,3,5-7)")
    print("-" * 60)
    for i, (name, dsid) in enumerate(items, start=1):
        print(f"  {i:2d}) {name}  [{dsid}]")


def parse_multi_selection(s: str, max_n: int) -> list[int]:
    """
    Accetta input tipo:
      "1,3,5-7"
    Ritorna lista di indici 1-based unici, ordinati.
    """
    s = s.strip().lower()
    if not s:
        return []
    parts = [p.strip() for p in s.split(",") if p.strip()]
    chosen = set()

    for p in parts:
        if "-" in p:
            a, b = p.split("-", 1)
            a = int(a.strip())
            b = int(b.strip())
            if a > b:
                a, b = b, a
            for k in range(a, b + 1):
                if 1 <= k <= max_n:
                    chosen.add(k)
        else:
            k = int(p)
            if 1 <= k <= max_n:
                chosen.add(k)

    return sorted(chosen)


def main():
    items = list(DATASETS.items())  # [(name, id), ...]

    while True:
        print_menu(items)
        choice = input("\nScelta: ").strip()

        if not choice:
            continue

        c = choice.lower()

        if c == "0":
            print("Bye.")
            return

        # scarica tutto
        if c == "a":
            selected = list(range(1, len(items) + 1))

        # selezione multipla
        elif c == "m":
            s = input("Inserisci selezione (es: 1,3,5-7): ").strip()
            selected = parse_multi_selection(s, len(items))
            if not selected:
                print("Selezione vuota/non valida.")
                continue

        # singolo numero
        else:
            try:
                k = int(choice)
            except ValueError:
                print("Input non valido. Usa 0, A, M, oppure un numero.")
                continue
            if k < 1 or k > len(items):
                print("Numero fuori range.")
                continue
            selected = [k]

        # esegui download
        for idx in selected:
            name, dsid = items[idx - 1]
            fname = f"{safe_filename(name)}__{dsid}.jsonl"
            out_file = OUT_DIR / fname

            print(f"\nDownloading: {name} [{dsid}]")
            print(f"Output: {out_file}")

            try:
                n = download_socrata_jsonl(dsid, out_file)
                print(f"✓ Salvato: {n:,} righe")
            except Exception as e:
                print(f"✗ Errore su {dsid}: {e}")

        print("\nOperazione completata.")


if __name__ == "__main__":
    main()
