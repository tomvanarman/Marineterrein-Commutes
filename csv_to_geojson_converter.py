import csv
import json
import os
import re
import sys
from dotenv import load_dotenv

load_dotenv()

INPUT_ROOT = "csv_data"       # where CSVs live
OUTPUT_ROOT = "sensor_data"   # where cleaned GeoJSONs + metadata go

# Speed spike filtering
MAX_BIKE_SPEED_KMH = 60       # hard cap — anything above this is physically implausible
MAX_NEIGHBOUR_RATIO = 2.5     # a point is a spike if it's >2.5x both its neighbours

# Supabase connection details — loaded from .env
SUPABASE_HOST     = os.getenv("SUPABASE_HOST")
SUPABASE_PORT     = int(os.getenv("SUPABASE_PORT", 6543))
SUPABASE_DB       = os.getenv("SUPABASE_DB")
SUPABASE_USER     = os.getenv("SUPABASE_USER")
SUPABASE_PASSWORD = os.getenv("SUPABASE_PASSWORD")

# ─────────────────────────────────────────────────────────────────────────────
# Supabase fetch
# ─────────────────────────────────────────────────────────────────────────────

RECONSTRUCTION_QUERY = """
with params as (
    select {trip_id}::int as trip_id
),
marker_bounds as (
    select
        p.trip_id,
        (
            select d1.samples
            from public.data1 d1
            where d1.trip_id = p.trip_id and d1.marker = 9
            order by d1.samples limit 1
        ) as start_sample,
        (
            select d1.samples
            from public.data1 d1
            where d1.trip_id = p.trip_id and d1.marker = 10
            order by d1.samples limit 1
        ) as end_sample
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
        select
            rd.trip_id,
            rd.samples,
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
    select x.*
    from x
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
        (select d1.h_rot from public.data1 d1
         where d1.trip_id = x.trip_id and d1.samples = x.output_samples
           and d1.marker != 1 and d1.marker != 3 limit 1) as hrot,
        (select d1.speed from public.data1 d1
         where d1.trip_id = x.trip_id and d1.samples = x.output_samples
           and d1.marker != 1 and d1.marker != 3 limit 1) as speed,
        (select d1."timestamp" from public.data1 d1
         where d1.trip_id = x.trip_id and d1.samples = x.output_samples
           and d1.marker != 1 and d1.marker != 3 limit 1) as d1_ts
    from x_filtered x
)
select
    g.latitude                          as latitude,
    g.longitude                         as longitude,
    b.marker                            as marker,
    round((
        case
            when (b.acc_low + b.acc_high * 256) >= 32768
                then (b.acc_low + b.acc_high * 256) - 65536
            else (b.acc_low + b.acc_high * 256)
        end
    ) / 1024.0, 3)                      as "Acc Y (g)",
    b.hrot                              as "HRot Count",
    b.speed                             as "Speed",
    b.output_samples                    as "Samples",
    g.heading                           as "Heading GPS (dg)",
    g.speed                             as "Speed GPS",
    g.accuracy                          as "Accuracy GPS",
    g.altitude                          as "Altitude GPS",
    g."timestamp"                       as "GNSS Timestamp",
    to_char(b.d1_ts at time zone 'Europe/Amsterdam', 'HH24:MI:SS') as "HH:mm:ss",
    to_char(b.d1_ts, 'MS')             as "SSS"
from base b
left join lateral (
    select g.latitude, g.longitude, g.heading, g.speed,
           g.accuracy, g.altitude, g."timestamp"
    from public.gnss g
    where g.trip_id = b.trip_id and b.d1_ts is not null
    order by abs(extract(epoch from (g."timestamp" - b.d1_ts)))
    limit 1
) g on true
order by b.raw_samples, b.output_samples;
"""


def get_supabase_connection():
    """Return a psycopg2 connection to Supabase, or raise with a clear message."""
    try:
        import psycopg2
    except ImportError:
        print("  ❌ psycopg2 is not installed. Run: pip install psycopg2-binary")
        sys.exit(1)

    return psycopg2.connect(
        host=SUPABASE_HOST,
        port=SUPABASE_PORT,
        dbname=SUPABASE_DB,
        user=SUPABASE_USER,
        password=SUPABASE_PASSWORD,
        sslmode="require",
    )


def _normalise_ts(ts):
    """Return a stripped string for timestamp comparison (strips microseconds noise)."""
    if ts is None:
        return None
    return str(ts).strip()


def build_existing_ts_index(all_metadata):
    """
    Build a set of (trip_start, trip_end) strings from existing metadata so we
    can quickly check whether a Supabase trip has already been processed.
    """
    index = set()
    for entry in all_metadata.values():
        raw = entry.get("Trip start/end", "")
        # Format stored in metadata: ", 2026-04-09 16:43:08.998, 2026-04-09 16:49:17.797"
        parts = [p.strip() for p in raw.split(",") if p.strip()]
        if len(parts) >= 2:
            index.add((parts[0], parts[1]))
    return index


def fetch_trips_from_supabase(all_metadata):
    """
    Query Supabase for all trips, skip ones already in metadata (by
    trip_start / trip_end), reconstruct CSV rows for new trips, write
    them to csv_data/, and return a dict of {filename: trip_metadata}.
    """
    print("\n🌐 Connecting to Supabase…")
    conn = get_supabase_connection()
    cur = conn.cursor()

    # ── 1. List all trips ────────────────────────────────────────────────────
    cur.execute("""
        SELECT id, trip_start, trip_end, system_id
        FROM public.trips
        ORDER BY trip_start;
    """)
    trips = cur.fetchall()
    print(f"  Found {len(trips)} trip(s) in Supabase.")

    existing_ts = build_existing_ts_index(all_metadata)
    os.makedirs(INPUT_ROOT, exist_ok=True)

    new_files = {}   # filename → trip metadata dict for newly fetched trips

    for (trip_id, trip_start, trip_end, system_id) in trips:
        ts_key = (_normalise_ts(trip_start), _normalise_ts(trip_end))

        if ts_key in existing_ts:
            print(f"  ⏭️  Trip {trip_id} ({trip_start}) already processed — skipping.")
            continue

        print(f"  ⬇️  Fetching trip {trip_id} ({trip_start} → {trip_end})…")

        # ── 2. Reconstruct rows ──────────────────────────────────────────────
        cur.execute(RECONSTRUCTION_QUERY.format(trip_id=trip_id))
        rows = cur.fetchall()
        col_names = [desc[0] for desc in cur.description]

        if not rows:
            print(f"     ⚠️  No data rows returned — skipping.")
            continue

        # ── 3. Write temporary CSV ───────────────────────────────────────────
        # Convert signed int system_id to the same hex slug used by local CSVs:
        # e.g. -2553939011954146614 → 0xDC8E95FFFE7602D3 → last 5 chars → "602D3"
        if system_id is not None:
            sid_slug = hex(system_id & 0xFFFFFFFFFFFFFFFF).upper()[-5:]
        else:
            sid_slug = str(trip_id)
        filename = f"API_{sid_slug}_{trip_start.strftime('%Y%m%d_%H%M%S')}.csv"
        csv_path = os.path.join(INPUT_ROOT, filename)

        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(col_names)
            writer.writerows(rows)

        # ── 4. Build the metadata we'd normally parse from the footer ────────
        trip_meta = {
            "Trip start/end": f", {_normalise_ts(trip_start)}, {_normalise_ts(trip_end)}",
            "source": "api",
            "supabase_trip_id": trip_id,
        }
        new_files[filename] = trip_meta
        print(f"     ✅ Written {len(rows)} rows → {csv_path}")

    cur.close()
    conn.close()
    return new_files


# ─────────────────────────────────────────────────────────────────────────────
# Speed spike filtering (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

def filter_gnss_max_speed(gnss_value):
    """
    Parse the raw GNSS CSV line, replace MAX km/h with a spike-filtered value,
    and return the sanitised string.
    """
    parts = gnss_value.split(',')
    if len(parts) < 7:
        return gnss_value

    try:
        raw_max = float(parts[6])
    except (ValueError, TypeError):
        return gnss_value

    filtered_max = min(raw_max, MAX_BIKE_SPEED_KMH)

    try:
        avg_speed = float(parts[4])
        if avg_speed > 0 and filtered_max > avg_speed * MAX_NEIGHBOUR_RATIO:
            filtered_max = round(avg_speed * MAX_NEIGHBOUR_RATIO, 1)
    except (ValueError, TypeError):
        pass

    if filtered_max != raw_max:
        print(f"    ⚡ GPS spike filtered: {raw_max} km/h → {filtered_max} km/h")

    parts[6] = str(filtered_max)
    return ','.join(parts)


# ─────────────────────────────────────────────────────────────────────────────
# Trip numbering (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

def get_next_trip_number(sensor_id_folder):
    """Find the next available trip number inside a sensor_data/{sensor_id} folder."""
    if not os.path.exists(sensor_id_folder):
        return 1
    existing = [
        int(m.group(1))
        for f in os.listdir(sensor_id_folder)
        if (m := re.match(r".*_Trip(\d+)_clean\.geojson", f))
    ]
    return max(existing, default=0) + 1


# ─────────────────────────────────────────────────────────────────────────────
# CSV → GeoJSON (unchanged logic, source tag added)
# ─────────────────────────────────────────────────────────────────────────────

def process_csv(input_path, sensor_id, trip_num, extra_meta=None):
    """
    Convert a CSV file to GeoJSON features + metadata dict.
    extra_meta: optional dict merged into metadata (used for API-sourced trips).
    """
    features = []
    coords = []
    last_lat, last_lon = None, None

    with open(input_path, newline='') as csvfile:
        reader = list(csv.DictReader(csvfile))

        for row in reader:
            lat, lon = row.get('latitude'), row.get('longitude')
            try:
                lat_f, lon_f = float(lat), float(lon)
                last_lat, last_lon = lat_f, lon_f
                coords.append((lat_f, lon_f))
            except (ValueError, TypeError):
                coords.append((last_lat, last_lon) if last_lat and last_lon else None)

        for i in range(len(reader) - 1):
            row1, row2 = reader[i], reader[i + 1]
            coord1, coord2 = coords[i], coords[i + 1]
            if coord1 and coord2:
                props = {k: v for k, v in row1.items() if k not in ['latitude', 'longitude']}
                props["trip_id"] = f"{sensor_id}_Trip{trip_num}"
                feature = {
                    "type": "Feature",
                    "geometry": {
                        "type": "LineString",
                        "coordinates": [
                            [coord1[1], coord1[0]],
                            [coord2[1], coord2[0]]
                        ]
                    },
                    "properties": props
                }
                features.append(feature)

    metadata = {}

    # For API-sourced CSVs there is no footer — skip footer parsing
    is_api = (extra_meta or {}).get("source") == "api"

    if not is_api:
        with open(input_path, "r") as f:
            lines = f.readlines()

        last_gps_line = 0
        for i, line in enumerate(lines):
            parts = line.strip().split(',')
            if len(parts) >= 2:
                try:
                    float(parts[0]); float(parts[1])
                    last_gps_line = i
                except ValueError:
                    continue

        for line in lines[last_gps_line + 1:]:
            line = line.strip()
            if not line:
                continue
            if line.startswith(','):
                continue
            if ':' in line:
                key, val = line.split(':', 1)
                key = key.strip()
                if key and not key[0].isdigit():
                    metadata[key] = val.strip()
            elif line == "BLE Device Information Service":
                metadata[line] = line
            elif line.startswith('SENSOR,') or line.startswith('GNSS,'):
                parts = line.split(',', 1)
                if len(parts) == 2:
                    raw_value = ',' + parts[1]
                    if parts[0] == 'GNSS':
                        raw_value = filter_gnss_max_speed(raw_value)
                    metadata[parts[0]] = raw_value

        metadata["source"] = "local_csv"
    else:
        # Merge API metadata (trip_start/end, source tag, supabase_trip_id, etc.)
        metadata.update(extra_meta or {})

    return features, metadata


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main(use_api=False):
    os.makedirs(OUTPUT_ROOT, exist_ok=True)
    os.makedirs(INPUT_ROOT, exist_ok=True)

    metadata_index_file = "trips_metadata.json"
    if os.path.exists(metadata_index_file):
        with open(metadata_index_file, "r", encoding="utf-8") as f:
            all_metadata = json.load(f)
    else:
        all_metadata = {}

    # ── Optional: fetch new trips from Supabase ───────────────────────────────
    api_meta_by_file = {}   # filename → pre-built metadata from Supabase
    if use_api:
        api_meta_by_file = fetch_trips_from_supabase(all_metadata)
        if not api_meta_by_file:
            print("  ℹ️  No new trips to fetch from Supabase.")

    # ── Process all CSVs in csv_data/ ────────────────────────────────────────
    processed_any = False

    for entry in os.listdir(INPUT_ROOT):
        entry_path = os.path.join(INPUT_ROOT, entry)

        if os.path.isdir(entry_path):
            csv_files = [os.path.join(entry_path, f)
                         for f in os.listdir(entry_path) if f.lower().endswith(".csv")]
        elif entry.lower().endswith(".csv"):
            csv_files = [entry_path]
        else:
            continue

        for input_file in csv_files:
            file = os.path.basename(input_file)
            first_segment = file.split("_")[0]

            # API files are named API_<slug>_<date>.csv
            if first_segment == "API":
                sensor_id = file.split("_")[1]
            else:
                sensor_id = first_segment[-5:]

            # ── Deduplication for local CSVs ──────────────────────────────────
            # API trips were already deduplicated before writing; local CSVs
            # use source_file as a secondary guard (cheap, avoids re-processing
            # a file that was previously loaded locally).
            if first_segment != "API":
                already = any(
                    v.get("source_file") == file
                    for v in all_metadata.values()
                )
                if already:
                    print(f"⏭️  {file} already in metadata — skipping.")
                    continue

            # ── Assign trip number ────────────────────────────────────────────
            sensor_output = os.path.join(OUTPUT_ROOT, sensor_id)
            os.makedirs(sensor_output, exist_ok=True)
            trip_num = get_next_trip_number(sensor_output)
            trip_id  = f"{sensor_id}_Trip{trip_num}"

            # Pull pre-built API metadata if available
            extra_meta = api_meta_by_file.get(file)

            features, metadata = process_csv(input_file, sensor_id, trip_num, extra_meta)

            geojson = {"type": "FeatureCollection", "features": features}
            out_geojson = os.path.join(sensor_output, f"{trip_id}_clean.geojson")
            with open(out_geojson, "w", encoding="utf-8") as f:
                json.dump(geojson, f, indent=2)

            trip_metadata = {"source_file": file}
            trip_metadata.update(metadata)
            all_metadata[trip_id] = trip_metadata

            with open(metadata_index_file, "w", encoding="utf-8") as f:
                json.dump(all_metadata, f, indent=2)

            source_label = "🌐 API" if (extra_meta or {}).get("source") == "api" else "📄 CSV"
            print(f"✅ {source_label} {file} → {trip_id}_clean.geojson in {sensor_output}")
            processed_any = True

    if not processed_any:
        print("ℹ️  Nothing new to process.")


if __name__ == "__main__":
    # Pass --api flag to enable Supabase fetching
    use_api = "--api" in sys.argv
    main(use_api=use_api)
