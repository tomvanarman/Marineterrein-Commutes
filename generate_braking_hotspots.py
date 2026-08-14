#!/usr/bin/env python3
"""
generate_braking_hotspots.py

Reads trips.geojson and clusters braking events into geographic hotspots.
Outputs braking_hotspots.json — a GeoJSON FeatureCollection where each
feature is a point with:
  - count          : number of braking events at this location
  - avg_intensity  : mean deceleration (km/h/s)
  - max_intensity  : peak deceleration seen here
  - trip_count     : number of distinct trips that braked here
  - severity       : 'Gentle' / 'Firm' / 'Hard' / 'Emergency'

Run: python generate_braking_hotspots.py [trips.geojson] [output.json]

Grid resolution is controlled by CELL_DEG (~35 m at Amsterdam latitude).
"""

import json
import math
import sys
from collections import defaultdict
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────

INPUT_FILE  = "trips.geojson"
OUTPUT_FILE = "braking_hotspots.json"

# ~35 m grid cells at 52° N latitude — tune up for coarser, down for finer
CELL_DEG = 0.0003

# Only count events above this threshold (mirrors integrated_process.py)
MIN_INTENSITY = 5.0

# ── Helpers ───────────────────────────────────────────────────────────────────

def cell_key(lon, lat, cell_deg=CELL_DEG):
    """Snap a coordinate to the nearest grid cell centre."""
    cx = math.floor(lon / cell_deg) * cell_deg + cell_deg / 2
    cy = math.floor(lat / cell_deg) * cell_deg + cell_deg / 2
    return (round(cx, 8), round(cy, 8))


def severity_label(avg_intensity):
    if avg_intensity >= 20:
        return "Emergency"
    if avg_intensity >= 10:
        return "Hard"
    if avg_intensity >= 5:
        return "Firm"
    return "Gentle"


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    input_path  = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(INPUT_FILE)
    output_path = Path(sys.argv[2]) if len(sys.argv) > 2 else Path(OUTPUT_FILE)

    print(f"📖 Reading {input_path}…")
    with open(input_path) as f:
        data = json.load(f)

    features = data.get("features", [])
    print(f"   {len(features)} total segments")

    # ── Collect braking events ────────────────────────────────────────────────
    # Each braking segment is a LineString; we use its midpoint.
    cells = defaultdict(lambda: {
        "intensities": [],
        "trip_ids":    set(),
        "lon_sum":     0.0,
        "lat_sum":     0.0,
        "n":           0,
    })

    braking_count = 0
    for feat in features:
        props = feat.get("properties", {})

        is_braking  = props.get("is_braking", False)
        intensity   = float(props.get("braking_intensity") or 0)

        if not is_braking or intensity < MIN_INTENSITY:
            continue

        coords = feat["geometry"]["coordinates"]
        mid    = coords[len(coords) // 2]
        lon, lat = float(mid[0]), float(mid[1])

        key = cell_key(lon, lat)
        c = cells[key]
        c["intensities"].append(intensity)
        c["trip_ids"].add(props.get("trip_id", "unknown"))
        c["lon_sum"] += lon
        c["lat_sum"] += lat
        c["n"]       += 1
        braking_count += 1

    print(f"   {braking_count} braking events → {len(cells)} hotspot cells")

    # ── Build output features ─────────────────────────────────────────────────
    out_features = []
    for (cx, cy), c in sorted(cells.items(), key=lambda x: -x[1]["n"]):
        n              = c["n"]
        avg_intensity  = sum(c["intensities"]) / n
        max_intensity  = max(c["intensities"])

        # Use true centroid of contributing points, not grid centre
        lon = c["lon_sum"] / n
        lat = c["lat_sum"] / n

        out_features.append({
            "type": "Feature",
            "geometry": {
                "type":        "Point",
                "coordinates": [round(lon, 7), round(lat, 7)],
            },
            "properties": {
                "count":         n,
                "avg_intensity": round(avg_intensity, 2),
                "max_intensity": round(max_intensity, 2),
                "trip_count":    len(c["trip_ids"]),
                "severity":      severity_label(avg_intensity),
            },
        })

    geojson = {"type": "FeatureCollection", "features": out_features}
    with open(output_path, "w") as f:
        json.dump(geojson, f, separators=(",", ":"))

    size_kb = output_path.stat().st_size / 1024
    print(f"✅ Written {output_path}  ({len(out_features)} hotspots, {size_kb:.1f} KB)")

    if out_features:
        top = out_features[:5]
        print("\n🔴 Top hotspots:")
        for h in top:
            p = h["properties"]
            print(f"   count={p['count']:3d}  avg={p['avg_intensity']:5.1f} km/h/s"
                  f"  severity={p['severity']:10s}  trips={p['trip_count']}")


if __name__ == "__main__":
    main()
