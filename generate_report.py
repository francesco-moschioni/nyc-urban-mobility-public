"""
generate_report.py
──────────────────
Checks which pipeline outputs exist, then writes docs/report.html.

Usage (from the project root):
    python generate_report.py

The generated HTML is committed and pushed to make it visible at:
    https://francesco-moschioni.github.io/nyc-urban-mobility-public/report.html
"""

import importlib.util
from pathlib import Path
from datetime import datetime, timezone
import sys

# ── load config ──────────────────────────────────────────────────────────────
_root     = Path(__file__).parent
_cfg_path = _root / "00_config.py"
_spec     = importlib.util.spec_from_file_location("config", _cfg_path)
_mod      = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
cfg = _mod.cfg


# ── helpers ──────────────────────────────────────────────────────────────────
def fmt_size(path: Path) -> str:
    mb = path.stat().st_size / 1e6
    return f"{mb/1000:.1f} GB" if mb >= 1000 else f"{mb:.1f} MB"


def parquet_rows(path: Path) -> str:
    try:
        import pyarrow.parquet as pq
        n = pq.ParquetFile(path).metadata.num_rows
        return f"{n:,} rows"
    except Exception:
        return ""


def check_file(label: str, path, note_fn=None) -> dict:
    p = Path(path)
    if p.exists() and p.is_file():
        size  = fmt_size(p)
        note  = note_fn(p) if note_fn else ""
        return {"label": label, "status": "ok", "detail": f"{size}  {note}".strip()}
    return {"label": label, "status": "missing", "detail": str(p)}


def check_dir(label: str, path, glob: str = "*", min_files: int = 1) -> dict:
    p = Path(path)
    if p.exists():
        n = len(list(p.glob(glob)))
        if n >= min_files:
            return {"label": label, "status": "ok", "detail": f"{n} file(s)"}
        return {"label": label, "status": "warn", "detail": f"{n} file(s) (expected ≥ {min_files})"}
    return {"label": label, "status": "missing", "detail": str(p)}


def check_future(label: str, path) -> dict:
    p = Path(path)
    if p.exists():
        return {"label": label, "status": "ok", "detail": "exists"}
    return {"label": label, "status": "future", "detail": "not yet"}


# ── pipeline checks ───────────────────────────────────────────────────────────
SECTIONS = []

# Raw data
raw_checks = [
    check_dir("TLC yellow (parquet/month)",  cfg.raw_tlc / "yellow",         "*.parquet"),
    check_dir("TLC fhvhv (parquet/month)",   cfg.raw_tlc / "fhvhv",          "*.parquet"),
    check_dir("MTA ridership / alerts",      cfg.raw_mta,                    "*.csv"),
    check_dir("Citi Bike (unzipped CSVs)",   cfg.raw_citibike / "unzipped",  "*.csv"),
    check_dir("Weather NOAA",                cfg.raw_weather,                "*.csv"),
    check_dir("Traffic",                     cfg.raw_traffic,                "*.csv"),
    check_dir("GTFS current feed",           cfg.raw_gtfs_current,           "*"),
    check_dir("Census shapefiles",           cfg.raw_census / "shapefiles",  "*.shp"),
    check_dir("Census ACS",                  cfg.raw_census / "acs",         "*.csv"),
    check_dir("TLC zones shapefile",         cfg.external / "nyc_zones",     "*.shp"),
]
SECTIONS.append({"title": "Raw Data", "tag": "01", "checks": raw_checks})

# Stage 01 — Cleaning
s01_checks = [
    check_file("citibike_clean.parquet",
               cfg.interim / "citibike" / "citibike_clean.parquet",  parquet_rows),
    check_file("citibike_tlc.parquet",
               cfg.interim / "citibike" / "citibike_tlc.parquet",    parquet_rows),
    check_file("tlc_yellow_clean.parquet",
               cfg.interim / "tlc" / "tlc_yellow_clean.parquet",     parquet_rows)
               if (cfg.interim / "tlc").exists() else
               {"label": "tlc_yellow_clean.parquet", "status": "missing", "detail": ""},
    check_file("tlc_fhvhv_clean.parquet",
               cfg.interim / "tlc" / "tlc_fhvhv_clean.parquet",      parquet_rows)
               if (cfg.interim / "tlc").exists() else
               {"label": "tlc_fhvhv_clean.parquet", "status": "missing", "detail": ""},
]
SECTIONS.append({"title": "Stage 01 — Data Cleaning", "tag": "01", "checks": s01_checks})

# Stage 02 — Spatial/Temporal Alignment
s02_checks = [
    check_file("mta_flows_estimated.parquet",
               cfg.interim / "spatial_alignment" / "mta_flows_estimated.parquet", parquet_rows),
    check_file("mta_flows_tlc.parquet",
               cfg.interim / "spatial_alignment" / "mta_flows_tlc.parquet",       parquet_rows),
    check_dir("Temporal panels",
              cfg.interim / "temporal_panels", "*.parquet"),
]
SECTIONS.append({"title": "Stage 02 — Spatial & Temporal Alignment", "tag": "02", "checks": s02_checks})

# Stages 03–07 — future
future_checks = [
    check_future("Outside option",       cfg.processed / "outside_option.parquet"),
    check_future("Instruments",          cfg.processed / "instruments.parquet"),
    check_future("Simple Logit results", cfg.tables / "simple_logit.tex"),
    check_future("BLP results",          cfg.tables / "blp.tex"),
    check_future("SDI (NLP)",            cfg.processed / "sdi.parquet"),
]
SECTIONS.append({"title": "Stages 03–07 — Model Estimation", "tag": "03-07", "checks": future_checks})

# Open questions
OPEN_QUESTIONS = [
    ("OQ-1", "Market thickness / temporal granularity",  "blocks 02, 03, 05", "open"),
    ("OQ-2", "Alternative characteristics full spec",    "blocks 02, 05",     "open"),
    ("OQ-3", "HVFHV surge pricing IV strategy",          "blocks 04, 05",     "open"),
    ("OQ-4", "Taxi price: regulated tariff treatment",   "not blocking MVP",  "open"),
    ("OQ-5", "Citi Bike pricing for member trips",       "blocks 01, 05",     "open"),
    ("OQ-6", "MTA OD design for Subway/Bus",             "blocks 02, 05",     "open"),
    ("OQ-7", "Nest structure for Nested Logit",          "not blocking MVP",  "open"),
]


# ── compute summary stats ─────────────────────────────────────────────────────
def section_pct(checks):
    actionable = [c for c in checks if c["status"] != "future"]
    if not actionable:
        return None
    done = sum(1 for c in actionable if c["status"] == "ok")
    return round(100 * done / len(actionable))


# ── HTML builder ─────────────────────────────────────────────────────────────
STATUS_CLASS = {
    "ok":      "ok",
    "missing": "missing",
    "warn":    "warn",
    "future":  "future",
}
STATUS_LABEL = {
    "ok":      "OK",
    "missing": "MISSING",
    "warn":    "WARN",
    "future":  "—",
}

def rows_html(checks: list) -> str:
    out = []
    for c in checks:
        cls   = STATUS_CLASS.get(c["status"], "")
        label = STATUS_LABEL.get(c["status"], c["status"])
        out.append(
            f'<tr>'
            f'<td class="check-name">{c["label"]}</td>'
            f'<td><span class="badge {cls}">{label}</span></td>'
            f'<td class="detail">{c["detail"]}</td>'
            f'</tr>'
        )
    return "\n".join(out)


def section_html(sec: dict) -> str:
    pct = section_pct(sec["checks"])
    pct_html = ""
    if pct is not None:
        color = "#4cde8a" if pct == 100 else "#e8c84a" if pct >= 50 else "#e86a4a"
        pct_html = (
            f'<div class="prog-wrap">'
            f'<div class="prog-bar" style="width:{pct}%;background:{color}"></div>'
            f'</div>'
            f'<span class="prog-label">{pct}%</span>'
        )
    return f"""
<section class="pipeline-section">
  <div class="sec-header">
    <span class="sec-title">{sec["title"]}</span>
    <div class="sec-prog">{pct_html}</div>
  </div>
  <table>
    <thead><tr><th>Output</th><th>Status</th><th>Detail</th></tr></thead>
    <tbody>{rows_html(sec["checks"])}</tbody>
  </table>
</section>"""


def oq_rows_html() -> str:
    out = []
    for code, desc, blocks, status in OPEN_QUESTIONS:
        out.append(
            f'<tr>'
            f'<td class="oq-code">{code}</td>'
            f'<td>{desc}</td>'
            f'<td class="detail">{blocks}</td>'
            f'</tr>'
        )
    return "\n".join(out)


def build_html() -> str:
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    sections_html = "\n".join(section_html(s) for s in SECTIONS)

    # overall progress (exclude future-only sections)
    all_checks = [c for s in SECTIONS for c in s["checks"]]
    actionable = [c for c in all_checks if c["status"] != "future"]
    done_total = sum(1 for c in actionable if c["status"] == "ok")
    total_pct  = round(100 * done_total / len(actionable)) if actionable else 0

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>Thesis Progress — F. Moschioni</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=Instrument+Serif:ital@0;1&display=swap" rel="stylesheet">
<style>
  :root {{
    --bg:        #0d0f14;
    --panel:     #13161e;
    --border:    #242836;
    --accent:    #e8c84a;
    --accent2:   #4a9ee8;
    --text:      #e2e4ec;
    --muted:     #6b7280;
    --green:     #4cde8a;
    --red:       #e86a4a;
    --font-mono: 'DM Mono', monospace;
    --font-serif:'Instrument Serif', serif;
  }}
  *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{
    background: var(--bg);
    color: var(--text);
    font-family: var(--font-mono);
    font-size: 13px;
    line-height: 1.6;
    min-height: 100vh;
  }}
  a {{ color: var(--accent2); text-decoration: none; }}
  a:hover {{ text-decoration: underline; }}

  .container {{ max-width: 860px; margin: 0 auto; padding: 52px 24px 100px; }}

  /* header */
  .site-header {{ margin-bottom: 48px; border-bottom: 1px solid var(--border); padding-bottom: 28px; }}
  .site-header h1 {{
    font-family: var(--font-serif); font-size: 30px; font-weight: 400;
    font-style: italic; color: var(--accent); letter-spacing: -0.3px; margin-bottom: 4px;
  }}
  .site-header .sub {{ font-size: 11px; color: var(--muted); letter-spacing: 0.1em; text-transform: uppercase; }}
  .back-link {{ display: inline-block; margin-top: 16px; font-size: 11px; color: var(--muted);
    letter-spacing: 0.08em; text-transform: uppercase; text-decoration: none; transition: color .15s; }}
  .back-link:hover {{ color: var(--accent); text-decoration: none; }}

  /* overall summary */
  .summary-card {{
    background: var(--panel); border: 1px solid var(--border); border-radius: 6px;
    padding: 20px 24px; margin-bottom: 40px; display: flex; align-items: center; gap: 24px;
  }}
  .summary-big {{
    font-family: var(--font-serif); font-style: italic;
    font-size: 42px; color: var(--accent); line-height: 1; white-space: nowrap;
  }}
  .summary-right {{ flex: 1; }}
  .summary-label {{ font-size: 10px; color: var(--muted); letter-spacing: 0.12em; text-transform: uppercase; margin-bottom: 8px; }}
  .prog-wrap-main {{
    height: 6px; background: var(--border); border-radius: 3px; overflow: hidden; margin-bottom: 6px;
  }}
  .prog-bar-main {{ height: 100%; border-radius: 3px; transition: width .4s; background: var(--accent); }}
  .summary-generated {{ font-size: 11px; color: var(--muted); margin-top: 10px; }}

  /* section */
  .pipeline-section {{ margin-bottom: 40px; }}
  .sec-header {{
    display: flex; align-items: center; gap: 16px;
    margin-bottom: 12px; padding-bottom: 10px; border-bottom: 1px solid var(--border);
  }}
  .sec-title {{ font-size: 11px; letter-spacing: 0.12em; text-transform: uppercase; color: var(--muted); }}
  .sec-prog {{ display: flex; align-items: center; gap: 8px; margin-left: auto; }}
  .prog-wrap {{ width: 100px; height: 4px; background: var(--border); border-radius: 2px; overflow: hidden; }}
  .prog-bar  {{ height: 100%; border-radius: 2px; }}
  .prog-label {{ font-size: 11px; color: var(--muted); width: 34px; text-align: right; }}

  /* table */
  table {{ width: 100%; border-collapse: collapse; }}
  thead th {{
    font-size: 10px; letter-spacing: 0.1em; text-transform: uppercase;
    color: var(--muted); padding: 0 8px 8px; text-align: left; border-bottom: 1px solid var(--border);
  }}
  tbody tr {{ border-bottom: 1px solid var(--border); }}
  tbody tr:last-child {{ border-bottom: none; }}
  tbody tr:hover {{ background: rgba(255,255,255,.02); }}
  td {{ padding: 9px 8px; vertical-align: middle; }}
  .check-name {{ color: var(--text); font-size: 12px; }}
  .detail {{ color: var(--muted); font-size: 11px; }}
  .oq-code {{ color: var(--accent); font-size: 11px; white-space: nowrap; }}

  /* badges */
  .badge {{
    display: inline-block; padding: 2px 8px; border-radius: 3px; font-size: 10px;
    letter-spacing: 0.06em; font-weight: 500; white-space: nowrap;
  }}
  .badge.ok      {{ background: #1a3a2a; color: var(--green);  border: 1px solid #2a5a3a; }}
  .badge.missing {{ background: #3a1a1a; color: var(--red);    border: 1px solid #5a2a2a; }}
  .badge.warn    {{ background: #3a2a0a; color: var(--accent); border: 1px solid #5a440a; }}
  .badge.future  {{ background: transparent; color: var(--muted); border: 1px solid var(--border); }}

  /* open questions */
  .oq-section {{ margin-bottom: 40px; }}
  .section-label {{
    font-size: 10px; letter-spacing: 0.14em; text-transform: uppercase; color: var(--muted);
    margin-bottom: 16px; display: flex; align-items: center; gap: 12px;
  }}
  .section-label::after {{ content: ''; flex: 1; height: 1px; background: var(--border); }}

  footer {{ margin-top: 60px; padding-top: 20px; border-top: 1px solid var(--border);
    font-size: 11px; color: var(--muted); letter-spacing: 0.06em; }}
</style>
</head>
<body>
<div class="container">

  <header class="site-header">
    <h1>Thesis Progress Report</h1>
    <p class="sub">NYC Urban Mobility · Francesco Moschioni · Bocconi University</p>
    <a class="back-link" href="index.html">← back to site</a>
  </header>

  <div class="summary-card">
    <div class="summary-big">{total_pct}%</div>
    <div class="summary-right">
      <div class="summary-label">Overall pipeline progress</div>
      <div class="prog-wrap-main">
        <div class="prog-bar-main" style="width:{total_pct}%"></div>
      </div>
      <div class="summary-label">{done_total} of {len(actionable)} outputs ready</div>
      <div class="summary-generated">Generated: {generated}</div>
    </div>
  </div>

  {sections_html}

  <div class="oq-section">
    <div class="section-label">Open Questions</div>
    <table>
      <thead><tr><th>Code</th><th>Question</th><th>Blocking</th></tr></thead>
      <tbody>{oq_rows_html()}</tbody>
    </table>
  </div>

  <footer>
    Francesco Moschioni · NYC Urban Mobility Thesis ·
    <a href="https://github.com/francesco-moschioni/nyc-urban-mobility-public" target="_blank">GitHub</a>
  </footer>

</div>
</body>
</html>"""


# ── write output ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    out_path = _root / "docs" / "report.html"
    out_path.write_text(build_html(), encoding="utf-8")
    print(f"✓ Written: {out_path}")
    print(f"  Commit and push to publish at:")
    print(f"  https://francesco-moschioni.github.io/nyc-urban-mobility-public/report.html")
