import json
import math
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
from road_quality_calculator import calculate_road_quality

# =========================
# CONFIG
# =========================

DEFAULT_WHEEL_DIAMETER_MM = 711
SAMPLE_RATE_HZ = 50
SECONDS_PER_SAMPLE = 1 / SAMPLE_RATE_HZ

INPUT_ROOT = "sensor_data"
OUTPUT_ROOT = "processed_sensor_data"

BRAKING_DECEL_THRESHOLD_KMH_S = 35.0
BRAKING_MIN_SPEED_KMH = 8.0

MIN_TIME_S = 0.5
MAX_BRAKING_CAP = 80.0

SMOOTHING_ALPHA = 0.7  # higher = smoother

SKIP_TRIPS = {
    "602CD": ["Trip1"],
    "604F0": ["Trip1"]
}

# =========================
# UTIL
# =========================

def safe_float(v, default=0.0):
    try:
        return float(v)
    except:
        return default

def safe_int(v, default=0):
    try:
        return int(v)
    except:
        return default

def haversine(lon1, lat1, lon2, lat2):
    if not all([lon1, lat1, lon2, lat2]):
        return 0
    R = 6371000
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)

    a = math.sin(dp/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

def calc_braking(prev, curr, dt):
    if dt <= 0 or prev < BRAKING_MIN_SPEED_KMH:
        return 0.0
    delta = prev - curr
    if delta <= 0:
        return 0.0
    return delta / dt

def smooth(prev, curr):
    if prev is None:
        return curr
    return SMOOTHING_ALPHA * prev + (1 - SMOOTHING_ALPHA) * curr

# =========================
# CORE PROCESSING
# =========================

def process_geojson_file(filepath, trip_id, metadata):
    with open(filepath) as f:
        data = json.load(f)

    features = data.get("features", [])

    points = []
    for i, f in enumerate(features):
        coords = f["geometry"]["coordinates"]
        lon, lat = coords[-1]

        props = f["properties"]

        points.append({
            "lon": lon,
            "lat": lat,
            "samples": safe_int(props.get("Samples", 0)),
            "gps_speed": safe_float(props.get("Speed GPS"), 0.0) * 3.6
        })

    points.sort(key=lambda x: x["samples"])

    segments = []

    prev_speed = None
    prev_raw_speed = None
    event_id = 0
    in_event = False

    for i in range(len(points) - 1):
        a, b = points[i], points[i + 1]

        dist = haversine(a["lon"], a["lat"], b["lon"], b["lat"])
        if dist > 1000:
            prev_speed = None
            continue

        raw_speed = (a["gps_speed"] + b["gps_speed"]) / 2
        raw_speed = min(raw_speed, 40)

        smoothed_speed = smooth(prev_speed, raw_speed)

        sample_diff = b["samples"] - a["samples"]
        dt = sample_diff * SECONDS_PER_SAMPLE

        if dt < MIN_TIME_S:
            prev_speed = None
            continue

        braking = calc_braking(prev_speed if prev_speed else smoothed_speed,
                               smoothed_speed, dt)

        braking = min(braking, MAX_BRAKING_CAP)

        is_braking = braking >= BRAKING_DECEL_THRESHOLD_KMH_S

        # =========================
        # EVENT LOGIC (NEW)
        # =========================
        is_start = False
        is_end = False

        if is_braking and not in_event:
            in_event = True
            event_id += 1
            is_start = True

        elif not is_braking and in_event:
            in_event = False
            is_end = True

        segments.append({
            "type": "Feature",
            "geometry": {
                "type": "LineString",
                "coordinates": [[a["lon"], a["lat"]], [b["lon"], b["lat"]]]
            },
            "properties": {
                "Speed": round(smoothed_speed, 2),
                "raw_speed": round(raw_speed, 2),
                "braking_intensity": round(braking, 2),
                "is_braking": is_braking,

                "event_id": event_id if in_event or is_start else None,
                "event_start": is_start,
                "event_end": is_end,

                "trip_id": trip_id
            }
        })

        prev_speed = smoothed_speed

    return {
        "type": "FeatureCollection",
        "features": segments
    }, {}

# =========================
# PIPELINE
# =========================

def process_all():
    inp = Path(INPUT_ROOT)
    out = Path(OUTPUT_ROOT)
    out.mkdir(exist_ok=True)

    for folder in inp.iterdir():
        if not folder.is_dir():
            continue

        for file in folder.glob("*_clean.geojson"):
            trip_id = file.stem.replace("_clean", "")

            out_dir = out / folder.name
            out_dir.mkdir(exist_ok=True)

            out_file = out_dir / f"{trip_id}_processed.geojson"

            if out_file.exists():
                continue

            print(f"Processing {trip_id}")

            result, _ = process_geojson_file(file, trip_id, {})

            with open(out_file, "w") as f:
                json.dump(result, f)

            print(f"✔ {trip_id}: {len(result['features'])} segments")

# =========================
# RUN
# =========================

if __name__ == "__main__":
    process_all()