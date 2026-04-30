#!/usr/bin/env python3
"""
generate_trips_geojson.py

Builds trips.geojson by merging two sources:
  1. Existing processed_sensor_data/ files (real road quality + wheel speed)
  2. New trips fetched from Supabase (GPS speed + road quality from accelerometer)

Local processed files take priority — if a trip_id exists in both,
the local version wins (it has better wheel-rotation speed data).
Supabase trips get road quality calculated on the fly from acc_y data
using the same road_quality_calculator.py module as the main pipeline.

Run: python generate_trips_geojson.py
"""

import json
import math
import os
import sys
from pathlib import Path

import numpy as np
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

# Import road quality calculator (same module used by integrated_processor.py)
try:
    from road_quality_calculator import calculate_road_quality
    ROAD_QUALITY_AVAILABLE = True
except ImportError:
    print("⚠️  road_quality_calculator.py not found — road_quality will be 0")
    ROAD_QUALITY_AVAILABLE = False

# ── Config ────────────────────────────────────────────────────────────────────

OUTPUT_FILE           = "trips.geojson"
PROCESSED_ROOT        = Path("processed_sensor_data")
MAX_SPEED_KMH         = 40
MAX_GPS_JUMP_M        = 1000
TRIM_M                = 100
INITIAL_DAYS          = None      # None = all trips; set e.g. 90 to limit
STATEMENT_TIMEOUT     = "30s"

# ── DB connection ─────────────────────────────────────────────────────────────

def get_connection():
    conn = psycopg2.connect(
        host            = os.environ["SUPABASE_HOST"],
        port            = int(os.environ.get("SUPABASE_PORT", 6543)),
        dbname          = os.environ["SUPABASE_DB"],
        user            = os.environ["SUPABASE_USER"],
        password        = os.environ["SUPABASE_PASSWORD"],
        sslmode         = "require",
        connect_timeout = 30,
    )
    conn.autocommit = True
    return conn

# ── Trip list ─────────────────────────────────────────────────────────────────

TRIPS_QUERY = """
select id, trip_start, trip_end, system_id, wheel_diam
from public.trips
{where}
order by trip_start desc
"""

DEFAULT_WHEEL_DIAM_INCH = 28.0  # fallback if not in DB

def fetch_trips(cur):
    where = ""
    if INITIAL_DAYS:
        where = f"where trip_start >= now() - interval '{INITIAL_DAYS} days'"
    cur.execute(TRIPS_QUERY.format(where=where))
    return cur.fetchall()

# ── Reconstruction query ──────────────────────────────────────────────────────

RECONSTRUCTION_QUERY = """
with params as (
    select %(trip_id)s::int as trip_id
),
marker_bounds as (
    select
        p.trip_id,
        (select d1.samples from public.data1 d1
         where d1.trip_id = p.trip_id and d1.marker = 9
         order by d1.samples limit 1) as start_sample,
        (select d1.samples from public.data1 d1
         where d1.trip_id = p.trip_id and d1.marker = 10
         order by d1.samples limit 1) as end_sample
    from params p
),
x as (
    select
        rd.trip_id,
        rd.samples as raw_samples,
        rd.samples - 9 + gs.i as output_samples,
        trim(vals[gs.i * 4 + 1])::integer as acc_low,
        trim(vals[gs.i * 4 + 2])::integer as acc_high
    from (
        select rd.trip_id, rd.samples,
               string_to_array(
                   replace(replace(convert_from(rd.data, 'UTF8'), '[', ''), ']', ''),
                   ','
               ) as vals
        from public.raw_data rd
        join marker_bounds mb on mb.trip_id = rd.trip_id
        where rd.trip_id = (select trip_id from params)
          and rd.samples >= mb.start_sample
          and rd.samples - 9 <= mb.end_sample
    ) rd
    cross join generate_series(0, 9) as gs(i)
),
x_filtered as (
    select x.* from x
    join marker_bounds mb on mb.trip_id = x.trip_id
    where x.output_samples >= mb.start_sample
      and x.output_samples <= mb.end_sample
),
base as (
    select
        x.*,
        (select d1.marker from public.data1 d1
         where d1.trip_id = x.trip_id and d1.samples = x.output_samples
           and d1.marker != 1 and d1.marker != 3 limit 1) as marker,
        (select d1.timestamp from public.data1 d1
         where d1.trip_id = x.trip_id and d1.samples = x.output_samples
           and d1.marker != 1 and d1.marker != 3 limit 1) as d1_ts
    from x_filtered x
)
select
    g.latitude,
    g.longitude,
    g.speed  as speed_gps,
    b.marker,
    b.output_samples as samples,
    round((
        case
            when (b.acc_low + b.acc_high * 256) >= 32768
                then (b.acc_low + b.acc_high * 256) - 65536
            else (b.acc_low + b.acc_high * 256)
        end
    ) / 1024.0, 3) as acc_y
from base b
left join lateral (
    select g.latitude, g.longitude, g.speed
    from public.gnss g
    where g.trip_id = b.trip_id and b.d1_ts is not null
    order by abs(extract(epoch from (g.timestamp - b.d1_ts)))
    limit 1
) g on true
where g.latitude is not null and g.longitude is not null
order by b.output_samples
"""

def reconstruct_trip(cur, trip_id):
    cur.execute(f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT}'")
    cur.execute(RECONSTRUCTION_QUERY, {"trip_id": trip_id})
    return cur.fetchall(), [desc[0] for desc in cur.description]

# ── Geometry helpers ──────────────────────────────────────────────────────────

def haversine(a, b):
    R = 6_371_000
    lat1 = math.radians(float(a["latitude"]))
    lat2 = math.radians(float(b["latitude"]))
    dlon = math.radians(float(b["longitude"]) - float(a["longitude"]))
    dlat = lat2 - lat1
    x = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(x), math.sqrt(1 - x))

def rows_to_dicts(rows, cols):
    return [dict(zip(cols, row)) for row in rows]

def privacy_trim(rows):
    if len(rows) < 2:
        return rows
    cum, start_idx = 0, 0
    for k in range(1, len(rows)):
        d = haversine(rows[k - 1], rows[k])
        if d > 500: continue
        cum += d
        if cum >= TRIM_M:
            start_idx = k
            break
    cum, end_idx = 0, len(rows) - 1
    for k in range(len(rows) - 1, 0, -1):
        d = haversine(rows[k], rows[k - 1])
        if d > 500: continue
        cum += d
        if cum >= TRIM_M:
            end_idx = k
            break
    return rows[start_idx:end_idx] if start_idx < end_idx else rows

def compute_road_quality_lookup(rows):
    """
    Build a sample-index → road_quality lookup from acc_y data.
    Returns a function that maps a sample index to a quality score (1-5),
    or None if there is not enough data or the module is unavailable.
    """
    if not ROAD_QUALITY_AVAILABLE or len(rows) < 200:
        return None

    acc_y = np.array([float(r["acc_y"] or 0) for r in rows])
    try:
        rq_data = calculate_road_quality(acc_y, window_size=100, overlap=0.5)
    except Exception as e:
        print(f"    ⚠️  Road quality calculation failed: {e}")
        return None

    quality_scores = rq_data["road_quality"]
    time_windows   = rq_data["time_windows"]

    def lookup(sample_idx):
        if len(time_windows) == 0:
            return 0
        closest = int(np.argmin(np.abs(time_windows - sample_idx)))
        return int(quality_scores[closest])

    return lookup


def rows_to_features(rows, trip_id, db_trip_id, wheel_diam_mm):
    """
    Convert reconstructed rows to GeoJSON features using GPS speed.
    Each consecutive pair of points becomes one LineString segment.
    Speed is taken directly from the matched GNSS speed (m/s → km/h),
    smoothed with a 5-point rolling average to reduce GPS noise.
    """
    features = []
    trimmed  = privacy_trim(rows)

    # Build road quality lookup from full untrimmed acc_y series
    quality_lookup = compute_road_quality_lookup(rows)

    # Pre-compute smoothed GPS speeds (5-point rolling average)
    # This reduces the jumpiness of raw GNSS speed readings
    raw_speeds = [float(r["speed_gps"] or 0) * 3.6 for r in trimmed]
    smoothed_speeds = []
    window = 5
    for k in range(len(raw_speeds)):
        start = max(0, k - window // 2)
        end   = min(len(raw_speeds), k + window // 2 + 1)
        smoothed_speeds.append(sum(raw_speeds[start:end]) / (end - start))

    for i in range(len(trimmed) - 1):
        a = trimmed[i]
        b = trimmed[i + 1]

        if not all([a["latitude"], a["longitude"], b["latitude"], b["longitude"]]):
            continue

        dist = haversine(a, b)
        if dist > MAX_GPS_JUMP_M:
            continue
        if a["longitude"] == b["longitude"] and a["latitude"] == b["latitude"]:
            continue

        # Average smoothed speed of the two endpoints, capped at 40 km/h
        speed_kmh = min((smoothed_speeds[i] + smoothed_speeds[i + 1]) / 2, MAX_SPEED_KMH)

        mid_sample   = (int(a["samples"] or 0) + int(b["samples"] or 0)) // 2
        road_quality = quality_lookup(mid_sample) if quality_lookup else 0

        features.append({
            "type": "Feature",
            "geometry": {
                "type": "LineString",
                "coordinates": [
                    [float(a["longitude"]), float(a["latitude"])],
                    [float(b["longitude"]), float(b["latitude"])],
                ],
            },
            "properties": {
                "trip_id":      trip_id,
                "db_trip_id":   db_trip_id,
                "Speed":        round(speed_kmh, 1),
                "marker":       a["marker"] or 0,
                "Acc Y (g)":    float(a["acc_y"] or 0),
                "road_quality": road_quality,
            },
        })

    return features

def make_trip_id(system_id, db_trip_id):
    hex_slug = format(int(system_id) & 0xFFFFFFFFFFFFFFFF, 'X')[-5:]
    return f"{hex_slug}_Trip{db_trip_id}"

# ── Step 1: load existing processed files ─────────────────────────────────────

def load_local_processed():
    """Load all *_processed.geojson files and return (features, set of trip_ids)."""
    features = []
    trip_ids = set()

    if not PROCESSED_ROOT.exists():
        print("ℹ️  No processed_sensor_data/ folder found — skipping local files")
        return features, trip_ids

    files = sorted(PROCESSED_ROOT.rglob("*_processed.geojson"))
    print(f"📂 Loading {len(files)} local processed file(s)…")

    for path in files:
        try:
            data = json.loads(path.read_text())
            for f in data.get("features", []):
                tid = f.get("properties", {}).get("trip_id")
                if tid:
                    trip_ids.add(tid)
                features.append(f)
        except Exception as e:
            print(f"  ⚠️  Could not read {path.name}: {e}")

    print(f"✅ Local: {len(features)} segments from {len(trip_ids)} trips")
    return features, trip_ids

# ── Step 2: fetch new trips from Supabase ────────────────────────────────────

def load_remote_trips(existing_trip_ids):
    """Fetch trips from Supabase, skip any whose trip_id already exists locally."""
    print("\n🔌 Connecting to Supabase…")
    try:
        conn = get_connection()
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return []

    features = []
    skipped  = []
    cur      = conn.cursor()
    trips    = fetch_trips(cur)
    print(f"📋 Found {len(trips)} trips in Supabase")

    for trip_row in trips:
        db_id, trip_start, trip_end, system_id, wheel_diam = trip_row
        trip_id = make_trip_id(system_id, db_id)

        # Convert wheel diameter: DB stores inches, we need mm
        try:
            wheel_diam_mm = float(wheel_diam) * 25.4 if wheel_diam else DEFAULT_WHEEL_DIAM_INCH * 25.4
        except (TypeError, ValueError):
            wheel_diam_mm = DEFAULT_WHEEL_DIAM_INCH * 25.4

        if trip_id in existing_trip_ids:
            print(f"  ⏭️  {trip_id} already in local files — skipping")
            continue

        try:
            rows, cols = reconstruct_trip(cur, db_id)
            if not rows:
                print(f"  ⚠️  Trip {db_id} ({trip_id}): no rows — skipping")
                continue
            dicts    = rows_to_dicts(rows, cols)
            new_feats = rows_to_features(dicts, trip_id, db_id, wheel_diam_mm)
            features.extend(new_feats)
            print(f"  ✅ {trip_id}: {len(rows)} samples → {len(new_feats)} segments")
        except Exception as e:
            print(f"  ❌ {trip_id}: {e}")
            skipped.append(trip_id)

    cur.close()
    conn.close()

    if skipped:
        print(f"\n⚠️  {len(skipped)} trip(s) skipped due to timeout/error:")
        for t in skipped:
            print(f"   {t}")

    return features

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    # 1. Load local processed files first (best data quality)
    local_features, local_trip_ids = load_local_processed()

    # 2. Fetch remote trips not already covered locally
    remote_features = load_remote_trips(local_trip_ids)

    # 3. Merge — local takes priority (already deduplicated above)
    all_features = local_features + remote_features

    # 4. Write
    geojson = {"type": "FeatureCollection", "features": all_features}
    with open(OUTPUT_FILE, "w") as f:
        json.dump(geojson, f, separators=(",", ":"))

    size_kb = os.path.getsize(OUTPUT_FILE) / 1024
    print(f"\n✅ Written {OUTPUT_FILE}")
    print(f"   Local trips  : {len(local_trip_ids)}")
    print(f"   Remote trips : {len(set(f['properties']['trip_id'] for f in remote_features))}")
    print(f"   Total segments: {len(all_features)} ({size_kb:.0f} KB)")

if __name__ == "__main__":
    main()
