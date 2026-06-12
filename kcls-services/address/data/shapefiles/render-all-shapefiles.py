#!/usr/bin/env python3
"""Render every shapefile under districts/ and home-orgs/ on a single map.

Two panels share one figure: the left shows the full extent of all the
library districts; the right zooms in on the KCLS home-org service areas
so the dense cluster of branches is readable. Districts are drawn as pale
regions with bold outlines, home orgs on top with saturated fills, so you
can see where they overlap.

Usage:
    render-all-shapefiles.py            # interactive window (falls back to PNG)
    render-all-shapefiles.py -o map.png # render straight to a file
    render-all-shapefiles.py --no-labels
"""

import argparse
import sys
from pathlib import Path

import geopandas as gpd
import matplotlib
import matplotlib.patheffects

HERE = Path(__file__).resolve().parent
DISTRICTS_DIR = HERE / "districts"
HOME_ORGS_DIR = HERE / "home-orgs"

# Common CRS for the map; all source files are EPSG:4269 but reproject
# anyway in case new files show up in something else.
MAP_CRS = "EPSG:4269"


def discover():
    districts = [
        (p, p.stem.replace("_", " ").strip())
        for p in sorted(DISTRICTS_DIR.glob("*.shp"))
    ]
    home_orgs = [(p, p.parent.name) for p in sorted(HOME_ORGS_DIR.glob("*/*.shp"))]
    return districts, home_orgs


def load_layers(entries):
    layers = []
    for path, label in entries:
        gdf = gpd.read_file(path)
        if gdf.crs is None:
            gdf = gdf.set_crs(MAP_CRS)
        layers.append((label, gdf.to_crs(MAP_CRS)))
    return layers


def plot_layers(ax, layers, cmap_name, alpha, edge, lw, fontsize, labels=True):
    cmap = matplotlib.colormaps[cmap_name]
    for i, (label, gdf) in enumerate(layers):
        gdf.plot(ax=ax, color=cmap(i % cmap.N), alpha=alpha,
                 edgecolor=edge, linewidth=lw)
        if labels:
            # representative_point is guaranteed to fall inside the polygon,
            # unlike the centroid of a concave/multipart shape
            pt = gdf.geometry.union_all().representative_point()
            ax.annotate(
                label,
                xy=(pt.x, pt.y),
                ha="center",
                va="center",
                fontsize=fontsize,
                fontweight="bold",
                color="black",
                path_effects=[
                    matplotlib.patheffects.withStroke(linewidth=2, foreground="white")
                ],
            )


def total_bounds(layers, pad=0.05):
    """Bounding box (minx, miny, maxx, maxy) of all layers, padded a bit."""
    import numpy as np

    bounds = np.array([gdf.total_bounds for _, gdf in layers])
    minx, miny = bounds[:, :2].min(axis=0)
    maxx, maxy = bounds[:, 2:].max(axis=0)
    dx, dy = (maxx - minx) * pad, (maxy - miny) * pad
    return minx - dx, miny - dy, maxx + dx, maxy + dy


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-o", "--output", help="write the map to this image file "
                        "instead of opening a window")
    parser.add_argument("--no-labels", action="store_true", help="skip text labels")
    parser.add_argument("--dpi", type=int, default=150)
    args = parser.parse_args()

    if args.output:
        matplotlib.use("Agg")
    else:
        try:
            matplotlib.use("QtAgg")
        except ImportError:
            print("No Qt backend available; writing shapefiles-map.png instead",
                  file=sys.stderr)
            args.output = "shapefiles-map.png"
            matplotlib.use("Agg")

    import matplotlib.pyplot as plt

    district_entries, home_org_entries = discover()
    if not district_entries and not home_org_entries:
        sys.exit(f"No shapefiles found under {DISTRICTS_DIR} or {HOME_ORGS_DIR}")

    print(f"Loading {len(district_entries)} districts "
          f"and {len(home_org_entries)} home orgs...")
    districts = load_layers(district_entries)
    home_orgs = load_layers(home_org_entries)

    fig, (ax_full, ax_zoom) = plt.subplots(1, 2, figsize=(24, 13))

    for ax, ho_fontsize in ((ax_full, 5), (ax_zoom, 9)):
        # Districts first: pale fills, heavy outlines
        plot_layers(ax, districts, "Pastel1", alpha=0.5, edge="dimgray", lw=1.2,
                    fontsize=8, labels=not args.no_labels)
        # Home orgs on top: saturated fills, small code labels
        plot_layers(ax, home_orgs, "tab20", alpha=0.7, edge="black", lw=0.4,
                    fontsize=ho_fontsize, labels=not args.no_labels)

    ax_full.set_title("All districts and KCLS home orgs")

    if home_orgs:
        minx, miny, maxx, maxy = total_bounds(home_orgs)
        ax_zoom.set_xlim(minx, maxx)
        ax_zoom.set_ylim(miny, maxy)
        ax_zoom.set_title("KCLS home-org service areas (zoomed)")

    fig.tight_layout()

    if args.output:
        fig.savefig(args.output, dpi=args.dpi, bbox_inches="tight")
        print(f"Wrote {args.output}")
    else:
        plt.show()


if __name__ == "__main__":
    main()
