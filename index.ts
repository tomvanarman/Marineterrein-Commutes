// supabase/functions/trips-geojson/index.ts
// Deploy with: supabase functions deploy trips-geojson
//
// Returns processed trip data as GeoJSON FeatureCollection.
// All reconstruction is done inline via raw SQL — no DB function or migration needed.
// Reads from: gnss, raw_data, data1, trips (read-only).
//
// Query params:
//   ?trip_id=123          — single trip (optional)
//   ?since=2026-01-01     — only trips starting after this date (optional)
//   ?limit=50             — max number of trips to return (default 100)

import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { Pool } from "https://deno.land/x/postgres@v0.17.0/mod.ts";

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

// ─── Inline reconstruction query ─────────────────────────────────────────────
// This replicates what the Python pipeline does:
//   1. Find start/end sample bounds from data1 (marker 9 = start, 10 = end)
//   2. Decode raw_data blobs → 10 accelerometer samples per row
//   3. Match each sample to the nearest GNSS point by timestamp
//   4. Return one row per sample with lat/lng, speed, acc_y, marker
//
// $1 = trip_id (integer)
const RECONSTRUCTION_QUERY = `
with params as (
    select $1::int as trip_id
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
        (select d1."timestamp" from public.data1 d1
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
    order by abs(extract(epoch from (g."timestamp" - b.d1_ts)))
    limit 1
) g on true
where g.latitude is not null and g.longitude is not null
order by b.output_samples;
`;

// ─── GeoJSON conversion ───────────────────────────────────────────────────────
function rowsToFeatures(rows: any[], tripId: string, dbTripId: number) {
  const features = [];
  const MAX_GPS_JUMP_M = 1000;
  const MAX_SPEED_KMH = 40;

  // Privacy trim: skip first and last ~100m
  const TRIM_M = 100;
  let cumStart = 0, startIdx = 0;
  for (let k = 1; k < rows.length; k++) {
    const d = haversine(rows[k - 1], rows[k]);
    if (d > 500) continue;
    cumStart += d;
    if (cumStart >= TRIM_M) { startIdx = k; break; }
  }

  let cumEnd = 0, endIdx = rows.length - 1;
  for (let k = rows.length - 1; k > 0; k--) {
    const d = haversine(rows[k], rows[k - 1]);
    if (d > 500) continue;
    cumEnd += d;
    if (cumEnd >= TRIM_M) { endIdx = k; break; }
  }

  const trimmed = startIdx < endIdx ? rows.slice(startIdx, endIdx) : rows;

  for (let i = 0; i < trimmed.length - 1; i++) {
    const a = trimmed[i];
    const b = trimmed[i + 1];

    if (!a.latitude || !a.longitude || !b.latitude || !b.longitude) continue;

    const dist = haversine(a, b);
    if (dist > MAX_GPS_JUMP_M) continue;
    if (a.longitude === b.longitude && a.latitude === b.latitude) continue;

    const speedA = (parseFloat(a.speed_gps) || 0) * 3.6;
    const speedB = (parseFloat(b.speed_gps) || 0) * 3.6;
    const speed = Math.min((speedA + speedB) / 2, MAX_SPEED_KMH);

    features.push({
      type: "Feature",
      geometry: {
        type: "LineString",
        coordinates: [
          [parseFloat(a.longitude), parseFloat(a.latitude)],
          [parseFloat(b.longitude), parseFloat(b.latitude)],
        ],
      },
      properties: {
        trip_id: tripId,
        db_trip_id: dbTripId,
        Speed: Math.round(speed * 10) / 10,
        marker: a.marker ?? 0,
        "Acc Y (g)": parseFloat(a.acc_y) || 0,
        road_quality: 0, // computed client-side if needed
      },
    });
  }

  return features;
}

function haversine(a: any, b: any): number {
  const R = 6371000;
  const lat1 = (parseFloat(a.latitude) * Math.PI) / 180;
  const lat2 = (parseFloat(b.latitude) * Math.PI) / 180;
  const dLat = lat2 - lat1;
  const dLon = ((parseFloat(b.longitude) - parseFloat(a.longitude)) * Math.PI) / 180;
  const x =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(lat1) * Math.cos(lat2) * Math.sin(dLon / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(x), Math.sqrt(1 - x));
}

// ─── Main handler ─────────────────────────────────────────────────────────────
serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response("ok", { headers: CORS_HEADERS });
  }

  // Create a Postgres pool using the direct DB URL (set automatically by Supabase
  // for Edge Functions — no env var you need to add).
  // SUPABASE_DB_URL format: postgres://postgres:password@host:5432/postgres
  const pool = new Pool(Deno.env.get("SUPABASE_DB_URL")!, 3, true);

  try {
    const url = new URL(req.url);
    const singleTripId = url.searchParams.get("trip_id");
    const since = url.searchParams.get("since");
    const limit = Math.min(parseInt(url.searchParams.get("limit") || "100"), 500);

    // ── 1. Fetch trip list via Supabase JS (convenient filter API) ──────────
    const supabase = createClient(
      Deno.env.get("SUPABASE_URL")!,
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
    );

    let tripsQuery = supabase
      .from("trips")
      .select("id, trip_start, trip_end, system_id")
      .order("trip_start", { ascending: false })
      .limit(limit);

    if (singleTripId) tripsQuery = tripsQuery.eq("id", parseInt(singleTripId));
    if (since)        tripsQuery = tripsQuery.gte("trip_start", since);

    const { data: trips, error: tripsError } = await tripsQuery;
    if (tripsError) throw tripsError;
    if (!trips || trips.length === 0) {
      return new Response(
        JSON.stringify({ type: "FeatureCollection", features: [] }),
        { headers: { ...CORS_HEADERS, "Content-Type": "application/json" } }
      );
    }

    // ── 2. Reconstruct each trip using raw SQL via the pg connection ────────
    const conn = await pool.connect();
    const allFeatures: any[] = [];

    try {
      for (const trip of trips) {
        const systemId = BigInt.asUintN(64, BigInt(trip.system_id));
        const hexSlug = systemId.toString(16).toUpperCase().slice(-5);
        const tripId = `${hexSlug}_Trip${trip.id}`;

        try {
          const result = await conn.queryObject(RECONSTRUCTION_QUERY, [trip.id]);
          const rows = result.rows;

          if (!rows || rows.length === 0) {
            console.warn(`Trip ${trip.id}: no rows reconstructed`);
            continue;
          }

          const features = rowsToFeatures(rows, tripId, trip.id);
          allFeatures.push(...features);
          console.log(`✅ Trip ${trip.id} (${tripId}): ${rows.length} samples → ${features.length} segments`);
        } catch (err) {
          // Log and continue — one bad trip shouldn't break the whole response
          console.error(`❌ Trip ${trip.id} reconstruction failed:`, err);
        }
      }
    } finally {
      conn.release();
    }

    const geojson = {
      type: "FeatureCollection",
      features: allFeatures,
    };

    return new Response(JSON.stringify(geojson), {
      headers: {
        ...CORS_HEADERS,
        "Content-Type": "application/json",
        "Cache-Control": "public, max-age=300",
      },
    });

  } catch (err) {
    console.error("Edge function error:", err);
    return new Response(JSON.stringify({ error: String(err) }), {
      status: 500,
      headers: { ...CORS_HEADERS, "Content-Type": "application/json" },
    });
  } finally {
    await pool.end();
  }
});
