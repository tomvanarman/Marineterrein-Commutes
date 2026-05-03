#!/usr/bin/env python3
"""
generate_trips_geojson.py

Builds trips.geojson by merging two sources:
  1. Existing processed_sensor_data/ files (wheel-rotation speed, best quality)
  2. New trips fetched from Supabase (gnss.speed in km/h, ~1700 pts/trip)

Local processed files take priority — if a trip_id exists in both,
the local version wins.

Supabase trips use gnss.speed directly (already in km/h) with a 5-point
rolling average to smooth GPS noise. Road quality is calculated from
decoded acc_y via road_quality_calculator.py.

Run: python generate_trips_geojson.py
"""

import bisect
import json
import math
import os
from datetime import timedelta
from pathlib import Path

import numpy as np
import psycopg2
from dotenv import load_dotenv

load_dotenv()

try:
    from road_quality_calculator import calculate_road_quality
    ROAD_QUALITY_AVAILABLE = True
except ImportError:
    print("⚠️  road_quality_calculator.py not found — road_quality will be 0")
    ROAD_QUALITY_AVAILABLE = False

# ── Config ────────────────────────────────────────────────────────────────────

OUTPUT_FILE        = "trips.geojson"
PROCESSED_ROOT     = Path("processed_sensor_data")
MAX_SPEED_KMH      = 40
MAX_GPS_JUMP_M     = 1000
TRIM_M             = 100
INITIAL_DAYS       = None   # None = all trips; set e.g. 90 to limit
STATEMENT_TIMEOUT  = "30s"
SPEED_SMOOTH_WIN   = 5      # rolling average window for gnss speed

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

DEFAULT_WHEEL_DIAM_INCH = 28.0

def fetch_trips(cur):
    where = ""
    if INITIAL_DAYS:
        where = f"where trip_start >= now() - interval '{INITIAL_DAYS} days'"
    cur.execute(TRIPS_QUERY.format(where=where))
    return cur.fetchall()

# ── Queries ───────────────────────────────────────────────────────────────────

# 1. Decoded acc_y from raw_data blobs (for road quality)
RAW_QUERY = """
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
        rd.samples - 9 + gs.i as output_samples,
        trim(vals[gs.i * 4 + 1])::integer as acc_low,
        trim(vals[gs.i * 4 + 2])::integer as acc_high
    from (
        select rd.samples,
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
)
select
    x.output_samples as samples,
    round((
        case
            when (x.acc_low + x.acc_high * 256) >= 32768
                then (x.acc_low + x.acc_high * 256) - 65536
            else (x.acc_low + x.acc_high * 256)
        end
    ) / 1024.0, 3) as acc_y
from x
join marker_bounds mb on true
where x.output_samples >= mb.start_sample
  and x.output_samples <= mb.end_sample
order by x.output_samples
"""

# 2. GNSS points with speed (already in km/h)
GNSS_QUERY = """
select latitude, longitude, speed, "timestamp"
from public.gnss
where trip_id = %(trip_id)s
  and latitude  is not null
  and longitude is not null
order by "timestamp"
"""

# 3. data1 anchors for sample→timestamp mapping (road quality lookup)
DATA1_QUERY = """
select samples, "timestamp"
from public.data1
where trip_id = %(trip_id)s
order by samples
"""

def fetch_trip_data(cur, trip_id):
    cur.execute(f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT}'")

    cur.execute(RAW_QUERY, {"trip_id": trip_id})
    raw_rows = cur.fetchall()
    raw_cols = [d[0] for d in cur.description]

    cur.execute(GNSS_QUERY, {"trip_id": trip_id})
    gnss_rows = cur.fetchall()
    gnss_cols = [d[0] for d in cur.description]

    cur.execute(DATA1_QUERY, {"trip_id": trip_id})
    d1_rows = cur.fetchall()
    d1_cols = [d[0] for d in cur.description]

    return raw_rows, raw_cols, gnss_rows, gnss_cols, d1_rows, d1_cols

# ── Road quality ──────────────────────────────────────────────────────────────

def compute_road_quality_lookup(raw_rows, raw_cols, d1_rows, d1_cols):
    """
    Build a timestamp → road_quality lookup using decoded acc_y samples.
    Maps each raw sample to a timestamp via data1 anchors, then returns
    a function: timestamp → quality score (1-5) or 0 if unavailable.
    """
    if not ROAD_QUALITY_AVAILABLE or not raw_rows or len(raw_rows) < 200:
        return None

    raw = [dict(zip(raw_cols, r)) for r in raw_rows]
    d1  = [dict(zip(d1_cols,  r)) for r in d1_rows]

    if not d1:
        return None

    d1_samples = [r["samples"]   for r in d1]
    d1_ts      = [r["timestamp"] for r in d1]

    def interp_ts(sample_idx):
        pos = bisect.bisect_left(d1_samples, sample_idx)
        if pos == 0:
            anchor = 0
        elif pos >= len(d1_samples):
            anchor = len(d1_samples) - 1
        else:
            before, after = pos - 1, pos
            anchor = before if abs(d1_samples[before] - sample_idx) <= abs(d1_samples[after] - sample_idx) else after
        diff = sample_idx - d1_samples[anchor]
        return d1_ts[anchor] + timedelta(milliseconds=diff * 20)

    acc_y = np.array([float(r["acc_y"] or 0) for r in raw])
    try:
        rq_data = calculate_road_quality(acc_y, window_size=100, overlap=0.5)
    except Exception as e:
        print(f"    ⚠️  Road quality failed: {e}")
        return None

    quality_scores = rq_data["road_quality"]
    time_windows   = rq_data["time_windows"]
    window_ts      = [interp_ts(int(w)) for w in time_windows]

    def lookup(ts):
        if not window_ts:
            return 0
        diffs = [abs((t - ts).total_seconds()) for t in window_ts]
        return int(quality_scores[diffs.index(min(diffs))])

    return lookup

# ── Geometry helpers ──────────────────────────────────────────────────────────

def haversine(a, b):
    R = 6_371_000
    lat1 = math.radians(float(a["latitude"]))
    lat2 = math.radians(float(b["latitude"]))
    dlon = math.radians(float(b["longitude"]) - float(a["longitude"]))
    dlat = lat2 - lat1
    x = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(x), math.sqrt(1 - x))

def privacy_trim(rows):
    if len(rows) < 2:
        return rows
    cum, start_idx = 0, 0
    for k in range(1, len(rows)):
        d = haversine(rows[k - 1], rows[k])
        if d > 500:
            continue
        cum += d
        if cum >= TRIM_M:
            start_idx = k
            break
    cum, end_idx = 0, len(rows) - 1
    for k in range(len(rows) - 1, 0, -1):
        d = haversine(rows[k], rows[k - 1])
        if d > 500:
            continue
        cum += d
        if cum >= TRIM_M:
            end_idx = k
            break
    return rows[start_idx:end_idx] if start_idx < end_idx else rows

# ── Feature builder ───────────────────────────────────────────────────────────

def rows_to_features(gnss_rows, gnss_cols, raw_rows, raw_cols,
                     d1_rows, d1_cols, trip_id, db_trip_id, wheel_diam_mm):
    """
    One LineString per consecutive gnss point pair.
    Speed = smoothed gnss.speed (already in km/h), capped at MAX_SPEED_KMH.
    Road quality from decoded acc_y via timestamp lookup.
    """
    if not gnss_rows:
        return []

    gnss    = [dict(zip(gnss_cols, r)) for r in gnss_rows]
    trimmed = privacy_trim(gnss)
    if len(trimmed) < 2:
        return []

    # 5-point rolling average to smooth GPS speed noise
    raw_speeds = [min(float(r["speed"] or 0), MAX_SPEED_KMH) for r in trimmed]
    smoothed   = []
    w          = SPEED_SMOOTH_WIN
    for k in range(len(raw_speeds)):
        start = max(0, k - w // 2)
        end   = min(len(raw_speeds), k + w // 2 + 1)
        smoothed.append(sum(raw_speeds[start:end]) / (end - start))

    quality_lookup = compute_road_quality_lookup(raw_rows, raw_cols, d1_rows, d1_cols)

    features = []
    for i in range(len(trimmed) - 1):
        a = trimmed[i]
        b = trimmed[i + 1]

        dist = haversine(a, b)
        if dist > MAX_GPS_JUMP_M or dist == 0:
            continue

        speed_kmh    = min((smoothed[i] + smoothed[i + 1]) / 2, MAX_SPEED_KMH)
        time_diff_s  = (b["timestamp"] - a["timestamp"]).total_seconds() if a["timestamp"] and b["timestamp"] else 0
        road_quality = quality_lookup(a["timestamp"]) if quality_lookup and a["timestamp"] else 0

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
                "trip_id":           trip_id,
                "db_trip_id":        db_trip_id,
                "Speed":             round(speed_kmh, 1),
                "marker":            0,
                "Acc Y (g)":         0,
                "road_quality":      road_quality,
                "hrot_diff":         0,
                "sample_diff":       0,
                "time_diff_s":       round(time_diff_s, 3),
                "gps_distance_m":    round(dist, 1),
                "wheel_diameter_mm": wheel_diam_mm,
            },
        })

    return features

def make_trip_id(system_id, db_trip_id):
    hex_slug = format(int(system_id) & 0xFFFFFFFFFFFFFFFF, 'X')[-5:]
    return f"{hex_slug}_Trip{db_trip_id}"

# ── Step 1: load existing processed files ─────────────────────────────────────

def load_local_processed():
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

# ── Step 2: fetch new trips from Supabase ─────────────────────────────────────

def load_remote_trips(existing_trip_ids):
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

        try:
            wheel_diam_mm = float(wheel_diam) * 25.4 if wheel_diam else DEFAULT_WHEEL_DIAM_INCH * 25.4
        except (TypeError, ValueError):
            wheel_diam_mm = DEFAULT_WHEEL_DIAM_INCH * 25.4

        if trip_id in existing_trip_ids:
            print(f"  ⏭️  {trip_id} already in local files — skipping")
            continue

        try:
            raw_rows, raw_cols, gnss_rows, gnss_cols, d1_rows, d1_cols = fetch_trip_data(cur, db_id)

            if not gnss_rows:
                print(f"  ⚠️  Trip {db_id} ({trip_id}): no gnss rows — skipping")
                continue

            new_feats = rows_to_features(
                gnss_rows, gnss_cols,
                raw_rows,  raw_cols,
                d1_rows,   d1_cols,
                trip_id, db_id, wheel_diam_mm
            )
            features.extend(new_feats)
            print(f"  ✅ {trip_id}: {len(gnss_rows)} gnss pts → {len(new_feats)} segments")

        except Exception as e:
            print(f"  ❌ {trip_id}: {e}")
            skipped.append(trip_id)

    cur.close()
    conn.close()

    if skipped:
        print(f"\n⚠️  {len(skipped)} trip(s) skipped:")
        for t in skipped:
            print(f"   {t}")

    return features

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    local_features, local_trip_ids = load_local_processed()
    remote_features = load_remote_trips(local_trip_ids)
    all_features = local_features + remote_features

    geojson = {"type": "FeatureCollection", "features": all_features}
    with open(OUTPUT_FILE, "w") as f:
        json.dump(geojson, f, separators=(",", ":"))

    size_kb = os.path.getsize(OUTPUT_FILE) / 1024
    print(f"\n✅ Written {OUTPUT_FILE}")
    print(f"   Local trips  : {len(local_trip_ids)}")
    remote_ids = set(f['properties']['trip_id'] for f in remote_features)
    print(f"   Remote trips : {len(remote_ids)}")
    print(f"   Total segments: {len(all_features)} ({size_kb:.0f} KB)")

if __name__ == "__main__":
    main()