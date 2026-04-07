"""
check_structure.py
------------------
Displays the actual folder structure of the project (OneDrive + disk D)
and checks it against the expected layout.

Usage:
    python check_structure.py
"""

import importlib.util
from pathlib import Path

# =========================
# PROJECT CONFIG
# =========================
PROJECT_ROOT = Path(__file__).parent.resolve()

_cfg_path = PROJECT_ROOT / "00_config.py"
_spec     = importlib.util.spec_from_file_location("config", _cfg_path)
_mod      = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
cfg = _mod.cfg

# ============================================================
# EXPECTED STRUCTURE
# ============================================================

EXPECTED_CODE = [
    # root files
    ".env",
    ".env.example",
    ".gitignore",
    "00_config.py",
    "setup_project.py",
    "check_structure.py",
    # src pipeline folders
    "src/01_data_cleaning",
    "src/02_spatial_temporal_alignment",
    "src/03_outside_option",
    "src/04_instruments",
    "src/05_models",
    "src/06_nlp_disruption_index",
    "src/07_results",
    # other code folders
    "notebooks",
    "thesis/chapters",
    "thesis/bibliography",
    # docs
    "docs/PROJECT_CONTEXT.md",
]

EXPECTED_DATA = [
    # raw data
    "data/raw/tlc",
    "data/raw/tlc/yellow",
    "data/raw/tlc/green",
    "data/raw/tlc/fhv",
    "data/raw/tlc/fhvhv",
    "data/raw/mta",
    "data/raw/gtfs",
    "data/raw/gtfs/current",
    "data/raw/gtfs/archive",
    "data/raw/citibike",
    "data/raw/weather",
    "data/raw/census",
    "data/raw/traffic",
    # external support files
    "data/external/nyc_zones",
    # interim
    "data/interim/spatial_alignment",
    "data/interim/temporal_panels",
    "data/processed",
    "models",
    "outputs/tables",
    "outputs/figures",
    "outputs/reports",
]

# Files/dirs to skip when printing the tree
IGNORE = {
    "__pycache__", ".git", ".ipynb_checkpoints",
    "node_modules", "Thumbs.db", ".DS_Store",
}

# Extensions too heavy to list individually — show count instead
HEAVY_EXTENSIONS = {".parquet", ".csv", ".pkl", ".zip"}

# Max depth for tree display
MAX_DEPTH = 4


# ============================================================
# TREE PRINTER
# ============================================================
def print_tree(root: Path, prefix: str = "", depth: int = 0) -> None:
    if depth > MAX_DEPTH:
        return
    if not root.exists():
        return

    try:
        entries = sorted(root.iterdir(), key=lambda p: (p.is_file(), p.name.lower()))
    except PermissionError:
        print(f"{prefix}  [permission denied]")
        return

    entries = [e for e in entries if e.name not in IGNORE]

    # Group heavy files — show count instead of listing each
    heavy = [e for e in entries if e.is_file() and e.suffix in HEAVY_EXTENSIONS]
    other = [e for e in entries if e not in heavy]

    display = other[:]
    if heavy:
        # Add a synthetic summary entry (represented as None + label)
        display.append(None)

    for i, entry in enumerate(display):
        is_last = (i == len(display) - 1)
        connector = "└── " if is_last else "├── "
        extender  = "    " if is_last else "│   "

        if entry is None:
            # Heavy files summary
            by_ext: dict[str, int] = {}
            for h in heavy:
                by_ext[h.suffix] = by_ext.get(h.suffix, 0) + 1
            summary = "  ".join(f"{ext}: {n}" for ext, n in sorted(by_ext.items()))
            print(f"{prefix}{connector}[{len(heavy)} data files — {summary}]")
        elif entry.is_dir():
            print(f"{prefix}{connector}{entry.name}/")
            print_tree(entry, prefix + extender, depth + 1)
        else:
            size = entry.stat().st_size
            size_str = _human(size)
            print(f"{prefix}{connector}{entry.name}  ({size_str})")


def _human(n: int) -> str:
    for unit in ["B", "KB", "MB", "GB"]:
        if n < 1024:
            return f"{n:.0f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


# ============================================================
# CHECKLIST
# ============================================================
def check_expected(root: Path, expected: list[str], label: str) -> None:
    print(f"\n{'─'*60}")
    print(f"  Checklist — {label}")
    print(f"{'─'*60}")
    all_ok = True
    for rel in expected:
        p = root / rel
        exists = p.exists()
        status = "✓" if exists else "✗ MISSING"
        if not exists:
            all_ok = False
        print(f"  {status:<12} {rel}")
    if all_ok:
        print(f"\n  ✅ All expected paths present.")
    else:
        print(f"\n  ⚠️  Some paths are missing — run setup_project.py to create them.")


# ============================================================
# MAIN
# ============================================================
def main() -> None:
    sep = "=" * 60

    # ── OneDrive ──────────────────────────────────────────────
    print(f"\n{sep}")
    print(f"  CODE ROOT (OneDrive)")
    print(f"  {cfg.code}")
    print(sep)
    print_tree(cfg.code)
    check_expected(cfg.code, EXPECTED_CODE, "OneDrive")

    # ── Disk D ────────────────────────────────────────────────
    data_root = cfg.data.parent   # D:\tesi
    print(f"\n{sep}")
    print(f"  DATA ROOT (Disk D)")
    print(f"  {data_root}")
    print(sep)
    print_tree(data_root)
    check_expected(data_root, EXPECTED_DATA, "Disk D")

    print()


if __name__ == "__main__":
    main()