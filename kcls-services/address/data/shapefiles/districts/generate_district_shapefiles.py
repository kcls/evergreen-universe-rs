"""
Generate Washington State library district service-area shapefiles.

Downloads two US Census TIGER/Line 2023 boundary datasets:
  1. US county boundaries  — used to obtain county outlines for WA (STATEFP=53)
  2. WA place boundaries   — used to obtain city limits (incorporated places)

Downloaded zips are cached locally in the cache/ directory so subsequent runs
skip the download step. Each output shapefile is a single-feature geometry
built by merging county boundaries, optionally adding city boundaries, and
subtracting excluded city boundaries via geometric difference operations.

Output shapefiles are written to the output/ directory.
"""

import io
import os
import zipfile

import geopandas as gpd
import requests

COUNTY_URL = "https://www2.census.gov/geo/tiger/TIGER2023/COUNTY/tl_2023_us_county.zip"
PLACE_URL = "https://www2.census.gov/geo/tiger/TIGER2023/PLACE/tl_2023_53_place.zip"

CACHE_DIR = "cache"
OUTPUT_DIR = "output"
OUTPUT_NAME = "_KCLS"


def download_shapefile(url: str) -> gpd.GeoDataFrame:
    """Download a zipped shapefile (with local cache) and return it as a GeoDataFrame."""
    os.makedirs(CACHE_DIR, exist_ok=True)
    filename = url.rsplit("/", 1)[-1]
    cache_path = os.path.join(CACHE_DIR, filename)

    if os.path.exists(cache_path):
        print(f"Using cached {cache_path}")
    else:
        print(f"Downloading {url} ...")
        resp = requests.get(url, timeout=120)
        resp.raise_for_status()
        with open(cache_path, "wb") as f:
            f.write(resp.content)
        print(f"  Saved to {cache_path}")

    zf = zipfile.ZipFile(cache_path)
    shp_names = [n for n in zf.namelist() if n.endswith(".shp")]
    if not shp_names:
        raise RuntimeError(f"No .shp file found in {cache_path}")
    print(f"  Reading {shp_names[0]} ...")
    return gpd.read_file(cache_path, engine="pyogrio")


def main() -> None:
    # --- Download source datasets ---
    # All US county boundaries (filtered to WA STATEFP=53 as needed below)
    counties = download_shapefile(COUNTY_URL)
    # All WA incorporated places (cities/towns), filtered by PLACEFP below
    # --- Filter counties and places used by multiple shapefiles below ---
    king = counties[(counties["STATEFP"] == "53") & (counties["COUNTYFP"] == "033")]
    if king.empty:
        raise RuntimeError("King County (STATEFP=53, COUNTYFP=033) not found")
    print(f"King County found: {king.iloc[0]['NAMELSAD']}")

    places = download_shapefile(PLACE_URL)

    seattle = places[places["PLACEFP"] == "63000"]
    if seattle.empty:
        raise RuntimeError("Seattle (PLACEFP=63000) not found")
    print(f"Seattle found: {seattle.iloc[0]['NAMELSAD']}")

    bothell = places[places["PLACEFP"] == "07380"]
    if bothell.empty:
        raise RuntimeError("Bothell (PLACEFP=07380) not found")
    print(f"Bothell found: {bothell.iloc[0]['NAMELSAD']}")

    auburn = places[places["PLACEFP"] == "03180"]
    if auburn.empty:
        raise RuntimeError("Auburn (PLACEFP=03180) not found")
    print(f"Auburn found: {auburn.iloc[0]['NAMELSAD']}")

    everett = places[places["PLACEFP"] == "22640"]
    if everett.empty:
        raise RuntimeError("Everett (PLACEFP=22640) not found")
    print(f"Everett found: {everett.iloc[0]['NAMELSAD']}")

    woodway = places[places["PLACEFP"] == "79835"]
    if woodway.empty:
        raise RuntimeError("Woodway (PLACEFP=79835) not found")
    print(f"Woodway found: {woodway.iloc[0]['NAMELSAD']}")

    woodland = places[places["PLACEFP"] == "79625"]
    if woodland.empty:
        raise RuntimeError("Woodland (PLACEFP=79625) not found")
    print(f"Woodland found: {woodland.iloc[0]['NAMELSAD']}")

    camas = places[places["PLACEFP"] == "09480"]
    if camas.empty:
        raise RuntimeError("Camas (PLACEFP=09480) not found")
    print(f"Camas found: {camas.iloc[0]['NAMELSAD']}")

    from shapely.ops import unary_union

    # ==========================================================================
    # _KCLS — King County Library System
    #   Counties: King
    #   Added cities: Bothell, Auburn (extend beyond King County boundary)
    #   Excluded cities: Seattle
    # ==========================================================================
    king_geom = king.geometry.iloc[0]
    seattle_geom = seattle.geometry.iloc[0]
    diff_geom = king_geom.difference(seattle_geom)
    print(f"Difference computed. Result type: {diff_geom.geom_type}")

    combined = unary_union([diff_geom, bothell.geometry.iloc[0], auburn.geometry.iloc[0]])
    print(f"Combined geometry type: {combined.geom_type}")

    result = gpd.GeoDataFrame(
        {"NAME": ["King County + Bothell + Auburn (excl. Seattle)"]},
        geometry=[combined],
        crs=king.crs,
    )

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = os.path.join(OUTPUT_DIR, f"{OUTPUT_NAME}.shp")
    result.to_file(out_path)
    print(f"Shapefile written to {out_path}")

    # ==========================================================================
    # Sno-Isle Regional Library
    #   Counties: Island, Snohomish
    #   Excluded cities: Bothell, Everett, Woodway
    # ==========================================================================
    snohomish = counties[(counties["STATEFP"] == "53") & (counties["COUNTYFP"] == "061")]
    if snohomish.empty:
        raise RuntimeError("Snohomish County (STATEFP=53, COUNTYFP=061) not found")
    print(f"Snohomish County found: {snohomish.iloc[0]['NAMELSAD']}")

    island = counties[(counties["STATEFP"] == "53") & (counties["COUNTYFP"] == "029")]
    if island.empty:
        raise RuntimeError("Island County not found")
    print(f"Island County found: {island.iloc[0]['NAMELSAD']}")

    sno_geom = unary_union([snohomish.geometry.iloc[0], island.geometry.iloc[0]])
    sno_geom = sno_geom.difference(bothell.geometry.iloc[0])
    sno_geom = sno_geom.difference(everett.geometry.iloc[0])
    sno_geom = sno_geom.difference(woodway.geometry.iloc[0])
    print(f"Snohomish + Island minus Everett/Woodway: {sno_geom.geom_type}")

    sno_result = gpd.GeoDataFrame(
        {"NAME": ["Snohomish + Island County (excl. Everett, Woodway)"]},
        geometry=[sno_geom],
        crs=snohomish.crs,
    )
    sno_path = os.path.join(OUTPUT_DIR, "Sno-Isle_Regional_Library.shp")
    sno_result.to_file(sno_path)
    print(f"Shapefile written to {sno_path}")

    # ==========================================================================
    # Fort Vancouver Regional Library
    #   Counties: Clark, Klickitat, Skamania
    #   Added cities: Woodland
    #   Excluded cities: Camas
    # ==========================================================================
    clark = counties[(counties["STATEFP"] == "53") & (counties["COUNTYFP"] == "011")]
    if clark.empty:
        raise RuntimeError("Clark County (STATEFP=53, COUNTYFP=011) not found")
    print(f"Clark County found: {clark.iloc[0]['NAMELSAD']}")

    klickitat = counties[(counties["STATEFP"] == "53") & (counties["COUNTYFP"] == "039")]
    if klickitat.empty:
        raise RuntimeError("Klickitat County not found")
    print(f"Klickitat County found: {klickitat.iloc[0]['NAMELSAD']}")

    skamania = counties[(counties["STATEFP"] == "53") & (counties["COUNTYFP"] == "059")]
    if skamania.empty:
        raise RuntimeError("Skamania County not found")
    print(f"Skamania County found: {skamania.iloc[0]['NAMELSAD']}")

    fvrl_geom = unary_union([
        clark.geometry.iloc[0], woodland.geometry.iloc[0],
        klickitat.geometry.iloc[0], skamania.geometry.iloc[0],
    ])
    fvrl_geom = fvrl_geom.difference(camas.geometry.iloc[0])
    print(f"Fort Vancouver Regional Library geometry: {fvrl_geom.geom_type}")

    clark_result = gpd.GeoDataFrame(
        {"NAME": ["Clark + Klickitat + Skamania + Woodland (excl. Camas)"]},
        geometry=[fvrl_geom],
        crs=clark.crs,
    )
    clark_path = os.path.join(OUTPUT_DIR, "Fort_Vancouver_Regional_Library.shp")
    clark_result.to_file(clark_path)
    print(f"Shapefile written to {clark_path}")

    # ==========================================================================
    # NCW Libraries
    #   Counties: Chelan, Douglas, Ferry, Grant, Okanogan
    #   Excluded cities: Conconully, Hartline, Mansfield, Marlin (Krupp),
    #                    Rock Island
    # ==========================================================================
    douglas = counties[(counties["STATEFP"] == "53") & (counties["COUNTYFP"] == "017")]
    if douglas.empty:
        raise RuntimeError("Douglas County not found")
    print(f"Douglas County found: {douglas.iloc[0]['NAMELSAD']}")

    ferry = counties[(counties["STATEFP"] == "53") & (counties["COUNTYFP"] == "019")]
    if ferry.empty:
        raise RuntimeError("Ferry County not found")
    print(f"Ferry County found: {ferry.iloc[0]['NAMELSAD']}")

    grant = counties[(counties["STATEFP"] == "53") & (counties["COUNTYFP"] == "025")]
    if grant.empty:
        raise RuntimeError("Grant County not found")
    print(f"Grant County found: {grant.iloc[0]['NAMELSAD']}")

    mansfield = places[places["PLACEFP"] == "42800"]
    if mansfield.empty:
        raise RuntimeError("Mansfield (PLACEFP=42800) not found")
    print(f"Mansfield found: {mansfield.iloc[0]['NAMELSAD']}")

    rock_island = places[places["PLACEFP"] == "59180"]
    if rock_island.empty:
        raise RuntimeError("Rock Island (PLACEFP=59180) not found")
    print(f"Rock Island found: {rock_island.iloc[0]['NAMELSAD']}")

    hartline = places[places["PLACEFP"] == "29920"]
    if hartline.empty:
        raise RuntimeError("Hartline (PLACEFP=29920) not found")
    print(f"Hartline found: {hartline.iloc[0]['NAMELSAD']}")

    krupp = places[places["PLACEFP"] == "36395"]
    if krupp.empty:
        raise RuntimeError("Krupp/Marlin (PLACEFP=36395) not found")
    print(f"Krupp/Marlin found: {krupp.iloc[0]['NAMELSAD']}")

    chelan = counties[(counties["STATEFP"] == "53") & (counties["COUNTYFP"] == "007")]
    if chelan.empty:
        raise RuntimeError("Chelan County not found")
    print(f"Chelan County found: {chelan.iloc[0]['NAMELSAD']}")

    okanogan = counties[(counties["STATEFP"] == "53") & (counties["COUNTYFP"] == "047")]
    if okanogan.empty:
        raise RuntimeError("Okanogan County not found")
    print(f"Okanogan County found: {okanogan.iloc[0]['NAMELSAD']}")

    conconully = places[places["PLACEFP"] == "14310"]
    if conconully.empty:
        raise RuntimeError("Conconully (PLACEFP=14310) not found")
    print(f"Conconully found: {conconully.iloc[0]['NAMELSAD']}")

    ncw_geom = unary_union([
        douglas.geometry.iloc[0],
        ferry.geometry.iloc[0],
        grant.geometry.iloc[0],
        chelan.geometry.iloc[0],
        okanogan.geometry.iloc[0],
    ])
    for city in [mansfield, rock_island, hartline, krupp, conconully]:
        ncw_geom = ncw_geom.difference(city.geometry.iloc[0])
    print(f"NCW Libraries geometry: {ncw_geom.geom_type}")

    ncw_result = gpd.GeoDataFrame(
        {"NAME": ["Chelan + Douglas + Ferry + Grant + Okanogan "
                   "(excl. Mansfield, Rock Island, Hartline, Marlin, Conconully)"]},
        geometry=[ncw_geom],
        crs=counties.crs,
    )
    ncw_path = os.path.join(OUTPUT_DIR, "NCW_Libraries.shp")
    ncw_result.to_file(ncw_path)
    print(f"Shapefile written to {ncw_path}")

    # ==========================================================================
    # Timberland Regional Library
    #   Counties: Grays Harbor, Lewis, Mason, Pacific, Thurston
    #   Excluded cities: Mossyrock, Napavine, Ocean Shores, Pe Ell, Vader
    # ==========================================================================
    trl_county_codes = {"027": "Grays Harbor", "041": "Lewis", "045": "Mason",
                        "049": "Pacific", "067": "Thurston"}
    trl_county_geoms = []
    for code, name in trl_county_codes.items():
        c = counties[(counties["STATEFP"] == "53") & (counties["COUNTYFP"] == code)]
        if c.empty:
            raise RuntimeError(f"{name} County not found")
        print(f"{name} County found: {c.iloc[0]['NAMELSAD']}")
        trl_county_geoms.append(c.geometry.iloc[0])

    trl_exclude_codes = {"50570": "Ocean Shores", "47315": "Mossyrock",
                         "47980": "Napavine", "53930": "Pe Ell", "73780": "Vader"}
    trl_exclude_geoms = []
    for code, name in trl_exclude_codes.items():
        p = places[places["PLACEFP"] == code]
        if p.empty:
            raise RuntimeError(f"{name} (PLACEFP={code}) not found")
        print(f"{name} found: {p.iloc[0]['NAMELSAD']}")
        trl_exclude_geoms.append(p.geometry.iloc[0])

    trl_geom = unary_union(trl_county_geoms)
    for exc in trl_exclude_geoms:
        trl_geom = trl_geom.difference(exc)
    print(f"Timberland Regional Library geometry: {trl_geom.geom_type}")

    trl_result = gpd.GeoDataFrame(
        {"NAME": ["Grays Harbor + Lewis + Mason + Pacific + Thurston "
                   "(excl. Ocean Shores, Mossyrock, Napavine, Pe Ell, Vader)"]},
        geometry=[trl_geom],
        crs=counties.crs,
    )
    trl_path = os.path.join(OUTPUT_DIR, "Timberland_Regional_Library.shp")
    trl_result.to_file(trl_path)
    print(f"Shapefile written to {trl_path}")

    # ==========================================================================
    # North Olympic Library System
    #   Counties: Clallam
    # ==========================================================================
    clallam = counties[(counties["STATEFP"] == "53") & (counties["COUNTYFP"] == "009")]
    if clallam.empty:
        raise RuntimeError("Clallam County not found")
    print(f"Clallam County found: {clallam.iloc[0]['NAMELSAD']}")

    nols_result = gpd.GeoDataFrame(
        {"NAME": ["Clallam County"]},
        geometry=[clallam.geometry.iloc[0]],
        crs=clallam.crs,
    )
    nols_path = os.path.join(OUTPUT_DIR, "North_Olympic_Library_System.shp")
    nols_result.to_file(nols_path)
    print(f"Shapefile written to {nols_path}")

    # ==========================================================================
    # Kitsap Regional Library
    #   Counties: Kitsap
    # ==========================================================================
    kitsap = counties[(counties["STATEFP"] == "53") & (counties["COUNTYFP"] == "035")]
    if kitsap.empty:
        raise RuntimeError("Kitsap County not found")
    print(f"Kitsap County found: {kitsap.iloc[0]['NAMELSAD']}")

    kitsap_result = gpd.GeoDataFrame(
        {"NAME": ["Kitsap County"]},
        geometry=[kitsap.geometry.iloc[0]],
        crs=kitsap.crs,
    )
    kitsap_path = os.path.join(OUTPUT_DIR, "Kitsap_Regional_Library.shp")
    kitsap_result.to_file(kitsap_path)
    print(f"Shapefile written to {kitsap_path}")

    # ==========================================================================
    # Jefferson County Rural Library District
    #   Counties: Jefferson
    #   Excluded cities: Port Townsend
    # ==========================================================================
    jefferson = counties[(counties["STATEFP"] == "53") & (counties["COUNTYFP"] == "031")]
    if jefferson.empty:
        raise RuntimeError("Jefferson County not found")
    print(f"Jefferson County found: {jefferson.iloc[0]['NAMELSAD']}")

    port_townsend = places[places["PLACEFP"] == "55855"]
    if port_townsend.empty:
        raise RuntimeError("Port Townsend (PLACEFP=55855) not found")
    print(f"Port Townsend found: {port_townsend.iloc[0]['NAMELSAD']}")

    jeff_geom = jefferson.geometry.iloc[0].difference(port_townsend.geometry.iloc[0])
    print(f"Jefferson minus Port Townsend: {jeff_geom.geom_type}")

    jeff_result = gpd.GeoDataFrame(
        {"NAME": ["Jefferson County (excl. Port Townsend)"]},
        geometry=[jeff_geom],
        crs=jefferson.crs,
    )
    jeff_path = os.path.join(OUTPUT_DIR, "Jefferson_County_Rural_Library_District.shp")
    jeff_result.to_file(jeff_path)
    print(f"Shapefile written to {jeff_path}")

    # ==========================================================================
    # Port Townsend Public Library
    #   City limits: Port Townsend
    # ==========================================================================
    pt_result = gpd.GeoDataFrame(
        {"NAME": ["Port Townsend"]},
        geometry=[port_townsend.geometry.iloc[0]],
        crs=places.crs,
    )
    pt_path = os.path.join(OUTPUT_DIR, "Port_Townsend_Public_Library.shp")
    pt_result.to_file(pt_path)
    print(f"Shapefile written to {pt_path}")

    # ==========================================================================
    # Pierce County Library System
    #   Counties: Pierce
    #   Excluded cities: Auburn, Carbonado, Fircrest, Puyallup, Roy, Ruston, Tacoma
    # ==========================================================================
    pierce = counties[(counties["STATEFP"] == "53") & (counties["COUNTYFP"] == "053")]
    if pierce.empty:
        raise RuntimeError("Pierce County not found")
    print(f"Pierce County found: {pierce.iloc[0]['NAMELSAD']}")

    pierce_exclude_codes = {
        "03180": "Auburn", "56695": "Puyallup", "70000": "Tacoma",
        "09970": "Carbonado", "23970": "Fircrest", "60160": "Roy",
        "60510": "Ruston",
    }
    pierce_geom = pierce.geometry.iloc[0]
    for code, name in pierce_exclude_codes.items():
        p = places[places["PLACEFP"] == code]
        if p.empty:
            raise RuntimeError(f"{name} (PLACEFP={code}) not found")
        print(f"{name} found: {p.iloc[0]['NAMELSAD']}")
        pierce_geom = pierce_geom.difference(p.geometry.iloc[0])
    print(f"Pierce County Library System geometry: {pierce_geom.geom_type}")

    pierce_result = gpd.GeoDataFrame(
        {"NAME": ["Pierce County (excl. Puyallup, Tacoma, Carbonado, Fircrest, Roy, Ruston)"]},
        geometry=[pierce_geom],
        crs=pierce.crs,
    )
    pierce_path = os.path.join(OUTPUT_DIR, "Pierce_County_Library_System.shp")
    pierce_result.to_file(pierce_path)
    print(f"Shapefile written to {pierce_path}")

    # ==========================================================================
    # Puyallup Public Library
    #   City limits: Puyallup
    # ==========================================================================
    puyallup = places[places["PLACEFP"] == "56695"]
    puyallup_result = gpd.GeoDataFrame(
        {"NAME": ["Puyallup"]},
        geometry=[puyallup.geometry.iloc[0]],
        crs=places.crs,
    )
    puyallup_path = os.path.join(OUTPUT_DIR, "Puyallup_Public_Library.shp")
    puyallup_result.to_file(puyallup_path)
    print(f"Shapefile written to {puyallup_path}")

    # ==========================================================================
    # Tacoma Public Library
    #   City limits: Tacoma
    # ==========================================================================
    tacoma = places[places["PLACEFP"] == "70000"]
    tacoma_result = gpd.GeoDataFrame(
        {"NAME": ["Tacoma"]},
        geometry=[tacoma.geometry.iloc[0]],
        crs=places.crs,
    )
    tacoma_path = os.path.join(OUTPUT_DIR, "Tacoma_Public_Library.shp")
    tacoma_result.to_file(tacoma_path)
    print(f"Shapefile written to {tacoma_path}")

    # ==========================================================================
    # Seattle Public Library
    #   City limits: Seattle
    # ==========================================================================
    seattle_result = gpd.GeoDataFrame(
        {"NAME": ["Seattle"]},
        geometry=[seattle.geometry.iloc[0]],
        crs=places.crs,
    )
    seattle_path = os.path.join(OUTPUT_DIR, "Seattle_Public_Library.shp")
    seattle_result.to_file(seattle_path)
    print(f"Shapefile written to {seattle_path}")

    # ==========================================================================
    # Everett Public Library
    #   City limits: Everett
    # ==========================================================================
    everett_result = gpd.GeoDataFrame(
        {"NAME": ["Everett"]},
        geometry=[everett.geometry.iloc[0]],
        crs=places.crs,
    )
    everett_path = os.path.join(OUTPUT_DIR, "Everett_Public_Library.shp")
    everett_result.to_file(everett_path)
    print(f"Shapefile written to {everett_path}")


if __name__ == "__main__":
    main()
