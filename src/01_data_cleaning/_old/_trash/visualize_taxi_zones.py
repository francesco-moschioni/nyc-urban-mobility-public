"""
visualize_zones.py
==================
Visualizza le NYC Taxi Zones dallo shapefile TLC.
Produce una mappa interattiva HTML (apri nel browser).

Dipendenze:
    pip install geopandas folium mapclassify

Il path dello shapefile viene letto dalla stessa config
usata da download_tlc.py (cfg.external / "nyc_zones").
"""

import importlib.util
from pathlib import Path

import folium
import geopandas as gpd
from folium.features import GeoJsonTooltip

# ── PROJECT CONFIG (stessa logica di download_tlc.py) ─────────
PROJECT_ROOT = Path(__file__).parents[4].resolve()
_cfg_path    = PROJECT_ROOT / "00_config.py"
_spec        = importlib.util.spec_from_file_location("config", _cfg_path)
_mod         = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
cfg = _mod.cfg

# ── PATH (identico a ZONE_SHP_DIR in download_tlc.py) ─────────
SHP_PATH = cfg.external / "nyc_zones" / "taxi_zones.shp"
OUT_HTML = cfg.external / "nyc_zones" / "taxi_zones_map.html"

# ── Colori per borough ─────────────────────────────────────────
BOROUGH_COLORS = {
    "Manhattan":    "#e63946",
    "Brooklyn":     "#457b9d",
    "Queens":       "#2a9d8f",
    "Bronx":        "#e9c46a",
    "Staten Island":"#f4a261",
    "EWR":          "#adb5bd",
}


def load_zones(shp_path: Path) -> gpd.GeoDataFrame:
    gdf = gpd.read_file(shp_path)
    # Lo shapefile TLC è in EPSG:2263 (NY State Plane, piedi)
    # Convertiamo in WGS84 (lat/lon) per folium
    gdf = gdf.to_crs("EPSG:4326")
    return gdf


def make_map(gdf: gpd.GeoDataFrame) -> folium.Map:
    center = [40.7128, -74.0060]
    m = folium.Map(location=center, zoom_start=11, tiles="CartoDB positron")

    for borough, color in BOROUGH_COLORS.items():
        subset = gdf[gdf["borough"] == borough]
        if subset.empty:
            continue

        folium.GeoJson(
            subset.__geo_interface__,
            name=borough,
            style_function=lambda _, c=color: {
                "fillColor":   c,
                "color":       "white",
                "weight":      0.8,
                "fillOpacity": 0.55,
            },
            highlight_function=lambda _: {
                "fillOpacity": 0.85,
                "weight":      2,
                "color":       "white",
            },
            tooltip=GeoJsonTooltip(
                fields=["zone", "borough", "LocationID"],
                aliases=["Zone:", "Borough:", "LocationID:"],
                sticky=True,
            ),
        ).add_to(m)

    folium.LayerControl(collapsed=False).add_to(m)

    legend_html = """
    <div style="
        position: fixed; bottom: 40px; left: 40px; z-index: 1000;
        background: white; padding: 12px 16px; border-radius: 8px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.25); font-family: sans-serif;
        font-size: 13px; line-height: 1.8;
    ">
    <b>Borough</b><br>
    """ + "".join(
        f'<span style="display:inline-block;width:14px;height:14px;'
        f'background:{c};border-radius:3px;margin-right:6px;vertical-align:middle;"></span>'
        f'{b}<br>'
        for b, c in BOROUGH_COLORS.items()
    ) + "</div>"
    m.get_root().html.add_child(folium.Element(legend_html))

    return m


def main() -> None:
    if not SHP_PATH.exists():
        raise FileNotFoundError(
            f"Shapefile non trovato: {SHP_PATH}\n"
            f"Esegui prima: python download_tlc.py --zones-only"
        )

    print(f"[INFO] Shapefile: {SHP_PATH}")
    gdf = load_zones(SHP_PATH)
    print(f"[INFO] {len(gdf)} zone caricate | CRS originale convertito in WGS84")
    print(f"[INFO] Borough: {sorted(gdf['borough'].unique())}")

    m = make_map(gdf)
    m.save(str(OUT_HTML))
    print(f"[INFO] Mappa salvata → {OUT_HTML}")
    print(f"[INFO] Apri nel browser: file:///{OUT_HTML}")


if __name__ == "__main__":
    main()