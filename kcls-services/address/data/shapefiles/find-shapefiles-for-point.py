#!/usr/bin/env python3
"""Report which shapefiles contain a given latitude/longitude point.

Scans every shapefile under districts/ and home-orgs/ and prints the
ones whose geometry contains the point.

Usage:
    find-shapefiles-for-point.py 47.6062 -122.3321
    find-shapefiles-for-point.py --lat 47.6062 --lon -122.3321
"""

import argparse
import sys
from pathlib import Path

import geopandas as gpd
from shapely.geometry import Point

HERE = Path(__file__).resolve().parent
DISTRICTS_DIR = HERE / "districts"
HOME_ORGS_DIR = HERE / "home-orgs"

# Coordinates arrive as plain lat/lon degrees (WGS84).
POINT_CRS = "EPSG:4326"


def discover():
    districts = [
        (p, p.stem.replace("_", " ").strip())
        for p in sorted(DISTRICTS_DIR.glob("*.shp"))
    ]
    home_orgs = [(p, p.parent.name) for p in sorted(HOME_ORGS_DIR.glob("*/*.shp"))]
    return districts, home_orgs


def point_in_shapefile(path, point):
    gdf = gpd.read_file(path)
    if gdf.crs is None:
        gdf = gdf.set_crs(POINT_CRS)
    pt = gpd.GeoSeries([point], crs=POINT_CRS).to_crs(gdf.crs).iloc[0]
    return gdf.geometry.contains(pt).any()


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("lat", nargs="?", type=float, help="latitude in degrees")
    parser.add_argument("lon", nargs="?", type=float, help="longitude in degrees")
    parser.add_argument("--lat", dest="lat_opt", type=float, help="latitude in degrees")
    parser.add_argument("--lon", dest="lon_opt", type=float, help="longitude in degrees")
    args = parser.parse_args()

    lat = args.lat_opt if args.lat_opt is not None else args.lat
    lon = args.lon_opt if args.lon_opt is not None else args.lon
    if lat is None or lon is None:
        parser.error("latitude and longitude are both required")
    if not -90 <= lat <= 90:
        parser.error(f"latitude {lat} out of range [-90, 90]")
    if not -180 <= lon <= 180:
        parser.error(f"longitude {lon} out of range [-180, 180]")

    district_entries, home_org_entries = discover()
    if not district_entries and not home_org_entries:
        sys.exit(f"No shapefiles found under {DISTRICTS_DIR} or {HOME_ORGS_DIR}")

    point = Point(lon, lat)  # shapely points are (x, y) == (lon, lat)
    matches = []

    for kind, entries in (("district", district_entries),
                          ("home-org", home_org_entries)):
        for path, label in entries:
            try:
                if point_in_shapefile(path, point):
                    matches.append((kind, label, path))
            except Exception as exc:
                print(f"Error reading {path}: {exc}", file=sys.stderr)

    if not matches:
        print(f"No shapefiles contain point ({lat}, {lon})")
        return

    print(f"Point ({lat}, {lon}) falls within:")
    for kind, label, path in matches:
        print(f"  [{kind}] {label} ({path.relative_to(HERE)})")


if __name__ == "__main__":
    main()
