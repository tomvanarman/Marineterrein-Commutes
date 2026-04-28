#!/usr/bin/env python3
"""
generate_trips_geojson.py

Connects to Supabase via direct Postgres credentials, reconstructs every trip
using the same CTE logic as the pipeline, and writes trips.geojson.

Run locally:  python generate_trips_geojson.py
Run via CI:   env vars are injected by the GitHub Actions workflow.
"""

import json
import math
import os
import sys

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────

OUTPUT_FILE    = "trips.geojson"
MAX_SPEED_KMH  = 40
MAX_GPS_JUMP_M = 1000
TRIM_M         = 100          # privacy trim: drop first/last ~100 m
INITIAL_DAYS   = 90           # only export trips from the last N days (set None for all)

# ── DB connection ─────────────────────────────────────────────────────────────

def get_connection():
    return psycopg2.connect(
        host     = os.environ["SUPABASE_HOST"],
        port     = int(os.environ.get("SUPABASE_PORT", 6543)),
        dbname   = os.environ["SUPABASE_DB"],
        user     = os.environ["SUPABASE_USER"],
        password = os.environ["SUPABASE_PASSWORD"],
        sslmode  = "require",
        connect_timeout = 30,
    )

# ── Trip list ─────────────────────────────────────────────────────────────────

TRIPS_QUERY = """
select id, trip_start, trip_end, system_id
from public.trips
{where}
order by trip_start desc
"""

def fetch_trips(cur):
    where = ""
    if INITIAL_DAYS:
        where = f"where trip_start >= now() - interval '{INITIAL_DAYS} days'"
    cur.execute(TRIPS_QUERY.format(where=where))
    return cur.fetchall()

# ── Per-trip reconstruction ───────────────────────────────────────────────────
# Identical CTE logic to the original pipeline / Edge Function.
# Returns one row per accelerometer sample with matched GNSS position.

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
    g.speed    as speed_gps,
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
    cur.execute(RECONSTRUCTION_QUERY, {"trip_id": trip_id})
    return cur.fetchall(), [desc[0] for desc in cur.description]

# ── Geometry helpers ──────────────────────────────────────────────────────────

def haversine(a, b):
    R = 6_371_000
    lat1, lat2 = math.radians(float(a["latitude"])), math.radians(float(b["latitude"]))
    dlon = math.radians(float(b["longitude"]) - float(a["longitude"]))
    dlat = lat2 - lat1
    x = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(x), math.sqrt(1 - x))

def rows_to_dicts(rows, cols):
    return [dict(zip(cols, row)) for row in rows]

def privacy_trim(rows):
    """Drop the first and last ~TRIM_M metres."""
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

def rows_to_features(rows, trip_id, db_trip_id):
    features = []
    trimmed = privacy_trim(rows)

    for i in range(len(trimmed) - 1):
        a, b = trimmed[i], trimmed[i + 1]

        if not all([a["latitude"], a["longitude"], b["latitude"], b["longitude"]]):
            continue

        dist = haversine(a, b)
        if dist > MAX_GPS_JUMP_M:
            continue
        if a["longitude"] == b["longitude"] and a["latitude"] == b["latitude"]:
            continue

        speed_a = (float(a["speed_gps"] or 0)) * 3.6
        speed_b = (float(b["speed_gps"] or 0)) * 3.6
        speed   = min((speed_a + speed_b) / 2, MAX_SPEED_KMH)

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
                "trip_id":    trip_id,
                "db_trip_id": db_trip_id,
                "Speed":      round(speed, 1),
                "marker":     a["marker"] or 0,
                "Acc Y (g)":  float(a["acc_y"] or 0),
                "road_quality": 0,  # computed client-side if needed
            },
        })

    return features

# ── Trip ID slug ──────────────────────────────────────────────────────────────

def make_trip_id(system_id, db_trip_id):
    hex_slug = format(int(system_id) & 0xFFFFFFFFFFFFFFFF, 'X')[-5:]
    return f"{hex_slug}_Trip{db_trip_id}"

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("🔌 Connecting to database...")
    try:
        conn = get_connection()
    except Exception as e:
        print(f"❌ Connection failed: {e}", file=sys.stderr)
        sys.exit(1)

    all_features = []

    with conn:
        with conn.cursor() as cur:
            trips = fetch_trips(cur)
            print(f"📋 Found {len(trips)} trips to process")

            for trip_row in trips:
                db_id, trip_start, trip_end, system_id = trip_row
                trip_id = make_trip_id(system_id, db_id)

                try:
                    rows, cols = reconstruct_trip(cur, db_id)
                    if not rows:
                        print(f"  ⚠️  Trip {db_id} ({trip_id}): no rows — skipping")
                        continue

                    dicts    = rows_to_dicts(rows, cols)
                    features = rows_to_features(dicts, trip_id, db_id)
                    all_features.extend(features)
                    print(f"  ✅ Trip {db_id} ({trip_id}): {len(rows)} samples → {len(features)} segments")

                except Exception as e:
                    print(f"  ❌ Trip {db_id} ({trip_id}): {e}", file=sys.stderr)
                    continue

    conn.close()

    geojson = {"type": "FeatureCollection", "features": all_features}

    with open(OUTPUT_FILE, "w") as f:
        json.dump(geojson, f, separators=(",", ":"))  # compact — smaller file

    size_kb = os.path.getsize(OUTPUT_FILE) / 1024
    print(f"\n✅ Written {OUTPUT_FILE} — {len(all_features)} segments, {size_kb:.0f} KB")

if __name__ == "__main__":
    main()
