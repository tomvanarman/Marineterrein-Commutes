# Reflector Ride Maps

A bike sensor data visualization tool that transforms GPS and accelerometer data into interactive route maps with speed coloring, road quality analysis, and braking event detection.

## Overview

This project ingests raw sensor data directly from a Supabase database, processes it into GeoJSON, and renders it as an interactive map.

- **Speed-colored route visualizations** showing cycling speeds across trips
- **Road quality mapping** to identify infrastructure conditions
- **Sudden braking detection** flagging deceleration events
- **Braking hotspots** accumulating repeated braking events across trips into location-based clusters
- **Single-file rendering** via `trips.geojson` — generated locally, committed to the repo, served statically

## How data gets to the map

```
Supabase DB ──► generate_trips_geojson.py ──► trips.geojson ──┐
                                                                 ├──► map
              generate_braking_hotspots.py ──► braking_hotspots.json ──┘
```

`generate_trips_geojson.py` fetches every trip from Supabase (GNSS speed, road quality from accelerometer, braking, crash/fall detection — all in one pass) and writes `trips.geojson`. Trips already present in the existing `trips.geojson` are skipped on subsequent runs rather than re-fetched, so the heavy per-trip raw-accelerometer decode only ever runs once per trip.

This used to also merge in a separate local CSV/manual-upload pipeline (`csv_to_geojson_converter.py` → `integrated_processor.py` → `processed_sensor_data/`), which took priority over Supabase when a trip existed in both. That pipeline only existed because an earlier version of this project bulk-downloaded everything locally first; it's now retired and moved to `archive/legacy-local-pipeline/` (see that folder's README for why). Supabase is the sole source of truth going forward, so there's no local/remote precedence to reason about anymore, and the trip_id scheme mismatch that used to exist between the two sources is no longer a concern.

## Features

### **Interactive Map**
- View all trips simultaneously or focus on individual routes
- Click any route segment to see speed, quality, and braking metrics
- Search for specific trips or sensors by name
- Toggle fullscreen mode for presentations

### **Speed Visualization**
- **Gradient mode**: Smooth color transitions between speeds
- **Category mode**: Distinct colors for speed ranges
- Speed range: 0–30+ km/h with 7 color categories

### **Road Quality Analysis**
- 5-level road quality rating system
- Color-coded segments: Perfect → Normal → Outdated → Bad → No Road

### **Sudden Braking**
- Flags segments where deceleration exceeds `BRAKING_DECEL_THRESHOLD_GPS_KMH_S` (currently 2.0 km/h/s, see Configuration below)
- Individual events colored by intensity: Gentle → Hard → Emergency
- **Accumulation hotspots**: larger circles where braking recurs across multiple trips, sized by event count — useful for identifying dangerous intersections or road features
- Click any event or hotspot for deceleration, severity, speed, and trip details

### **Trip Statistics**
- Total trips, distance, and riding time
- Average and maximum speeds
- Per-trip metrics on click
- Aggregate statistics across all rides

## Project Structure

```
Reflector-Ride-Maps/
├── trips.geojson                   # Map data (generated, commit this)
├── road_segments_averaged.json     # Averaged road segment scores (generated, commit this)
├── braking_hotspots.json           # Braking hotspot clusters (generated, commit this)
│
├── generate_trips_geojson.py       # Fetch from Supabase, process, write trips.geojson
├── road_quality_calculator.py      # Road quality scoring module
├── generate_braking_hotspots.py    # Aggregate braking events into hotspot clusters
│
├── index.html                      # Main visualization page
├── app.js                          # Map logic and interactions
├── config.js                       # Map configuration
├── styles.css                      # Styling
├── .env                            # Supabase credentials (never commit this)
│
└── archive/legacy-local-pipeline/  # Retired local CSV/manual-upload pipeline
    ├── csv_data/                   # Raw CSV files from sensors
    ├── sensor_data/                # Cleaned GeoJSON (intermediate)
    ├── processed_sensor_data/      # Speed + road quality + braking GeoJSON (final local output)
    ├── csv_to_geojson_converter.py # Old step 1: Convert CSVs / fetch from Supabase
    ├── integrated_processor.py     # Old step 2: Calculate speeds, road quality + braking
    └── master_pipeline.py          # Old orchestrator — see note below
```

> **Resolved: `master_pipeline.py`, `road_averaging.py`, and `trips_metadata.json`.** `road_averaging.py` only reads `trips.geojson` — it's unaffected by the local-pipeline archive and needs no changes; it stays at the repo root and runs unmodified after `generate_trips_geojson.py`. `master_pipeline.py` is a different story: it's an interactive CLI (it prompts for a `y/N` confirmation, twice) that shells out to the now-archived `csv_to_geojson_converter.py` and `integrated_processor.py` at their old top-level paths — it's currently broken, and even fixed, its `input()` prompts mean it was never runnable from CI anyway. It belongs in `archive/legacy-local-pipeline/` alongside the two scripts it called, which is where the tree above now shows it. `trips_metadata.json` and `processing_versions.json` are both confirmed gone — only the archived scripts ever wrote them, and nothing in the current pipeline reads or writes either.
>
> The GitHub Action (`.github/workflows/generate-trips-geojson.yml`, runs every 6 hours + manual dispatch) now runs `generate_trips_geojson.py` → `road_averaging.py` → `generate_braking_hotspots.py` directly — it never called `master_pipeline.py`, and previously never called `generate_braking_hotspots.py` either, so `braking_hotspots.json` was only ever produced by a manual local run. That gap is now closed.

## Quick Start

### Prerequisites

- **Python 3.x**
- **Python packages:**
  ```bash
  pip install psycopg2-binary python-dotenv numpy geojson
  ```

### Environment Setup

Create a `.env` file in the project root (gitignored — never commit it):

```
SUPABASE_HOST=aws-1-eu-west-1.pooler.supabase.com
SUPABASE_PORT=6543
SUPABASE_DB=postgres
SUPABASE_USER=your_username
SUPABASE_PASSWORD=your_password
```

### Adding new trips and updating the map

**Step 1 — Fetch and process new trips:**
```bash
python generate_trips_geojson.py
```
Fetches any trips from Supabase not already in `trips.geojson`, and computes speed, road quality, braking, and crash/fall detection for them in one pass. Writes `trips.geojson`.

**Step 2 — Average road segments:**
```bash
python road_averaging.py
```
Reads `trips.geojson` and writes `road_segments_averaged.json`.

**Step 3 — Generate braking hotspots:**
```bash
python generate_braking_hotspots.py
```
Reads `trips.geojson` and writes `braking_hotspots.json`.

**Step 4 — Commit and push:**
```bash
git add trips.geojson road_segments_averaged.json braking_hotspots.json
git commit -m "Update trip data"
git push
```

The map at `https://tomvanarman.github.io/Reflector-Ride-Maps/` updates automatically.

> This is also exactly what `.github/workflows/generate-trips-geojson.yml` runs on a 6-hour schedule (plus manual `workflow_dispatch`) — the four steps above are the verified, currently-running path, not a proposal.

## Detailed Workflow

### generate_trips_geojson.py

```bash
python generate_trips_geojson.py
```

Builds `trips.geojson` by:
1. Reading the existing `trips.geojson` (if any) and treating every trip already in it as processed — skipped on this run.
2. Fetching every remaining trip from Supabase: GNSS points (speed, position), decoded accelerometer data (road quality, crash detection), and wheel-rotation counts (used for crash-adjacent speed estimates).
3. Computing speed (GPS-smoothed for display), road quality, braking, and crash/fall events for each new trip, and writing everything to `trips.geojson`.

Trips that time out during reconstruction (>30s) are skipped and listed at the end. Increase `STATEMENT_TIMEOUT` in the script if needed.

### road_averaging.py

```bash
python road_averaging.py
```

Reads `trips.geojson`, groups nearby same-direction segments across all trips (within `DISTANCE_THRESHOLD_M` = 50m and `BEARING_THRESHOLD_DEG` = 15°) via a spatial grid, and writes `road_segments_averaged.json` with per-segment average/min/max speed, average road quality, and a composite score (0 = best, 100 = worst; 60% road quality weighted, 40% speed). Only reads `trips.geojson` — no Supabase connection, no dependency on the archived local pipeline.

### generate_braking_hotspots.py

```bash
python generate_braking_hotspots.py
```

Reads `trips.geojson` and clusters braking events into ~35 m grid cells. Each output point in `braking_hotspots.json` carries:
- `count` — number of braking events at this location
- `avg_intensity` / `max_intensity` — deceleration in km/h/s
- `trip_count` — how many distinct trips braked here
- `severity` — Gentle / Firm / Hard / Emergency

Grid cell size is controlled by `CELL_DEG` in the script (default `0.0003` ≈ 35 m at Amsterdam latitude).

> Note: `MIN_INTENSITY` in `generate_braking_hotspots.py` is currently `5.0` — a second, independent copy of a braking threshold, not read from `generate_trips_geojson.py`. That's worth a closer look: `generate_trips_geojson.py`'s live detection threshold (`BRAKING_DECEL_THRESHOLD_GPS_KMH_S`) is `2.0`, not `5.0` — so `is_braking` gets flagged starting at 2.0 km/h/s, but a braking event needs to clear 5.0 before it counts toward a hotspot. That gap may be intentional (a higher bar for hotspot-worthy braking than for flagging a single segment), or `MIN_INTENSITY` may just be a stale leftover from when `5.0` was the shared threshold in the now-archived `integrated_processor.py`. Worth confirming which before relying on hotspot counts.

### Automation

`.github/workflows/generate-trips-geojson.yml` runs the three scripts above, in order, every 6 hours (`cron: '0 */6 * * *'`) and on manual `workflow_dispatch`, then commits `trips.geojson`, `road_segments_averaged.json`, and `braking_hotspots.json` if anything changed.

## Web Visualization

Visit: **https://tomvanarman.github.io/Reflector-Ride-Maps/**

The map loads `trips.geojson` and `braking_hotspots.json` statically — fast, simple, no backend needed at runtime.

### Controls

**Trip Selection:**
- **Search**: Find trips by sensor ID (e.g. `602CA`) or full trip name
- **Click**: Select a route segment to see its stats
- **Reset**: Return to full view

**Visualization Modes:**
- **Speed**: Gradient or category color mode
- **Road Quality**: Infrastructure condition coloring
- **Averaged Road Segments**: Aggregated multi-trip view with composite score — sourced from `road_segments_averaged.json`, produced by `road_averaging.py`, which runs unchanged after `generate_trips_geojson.py` in both the manual steps and the scheduled GitHub Action
- **Sudden Braking**: Individual deceleration events; enable sub-toggle for accumulation hotspots

### Speed Legend

- 🔘 Gray: Stopped (0–2 km/h)
- 🔴 Red: Very Slow (2–5 km/h)
- 🟠 Orange: Slow (5–10 km/h)
- 🟡 Yellow: Moderate (10–15 km/h)
- 🟢 Green: Fast (15–20 km/h)
- 🔵 Blue: Very Fast (20–25 km/h)
- 🟣 Purple: Extreme (25+ km/h)

### Road Quality Legend

- 🟢 Green: Perfect (1)
- 🟢 Light Green: Normal (2)
- 🟡 Yellow: Outdated (3)
- 🟠 Orange: Bad (4)
- 🔴 Red: No Road (5)

### Braking Legend

- 🟡 Yellow: Gentle (5–10 km/h/s)
- 🟠 Orange: Hard (10–20 km/h/s)
- 🔴 Red: Emergency (20+ km/h/s)
- Circle size (hotspot mode): proportional to number of events at that location

## Configuration

### Map settings (`config.js`)

```javascript
MAP_CENTER: [4.9041, 52.3676],  // Amsterdam
MAP_ZOOM: 13,
MAP_STYLE: '...'                // CartoDB Dark Matter
```

### Braking detection (`generate_trips_geojson.py`)

```python
BRAKING_DECEL_THRESHOLD_GPS_KMH_S = 2.0  # km/h lost per second; tune up to 2.5 to reduce false positives
SPEED_JUMP_THRESHOLD_KMH          = 20   # implausible inter-segment speed change → treated as a GPS discontinuity, not braking
MIN_SEGMENT_TIME_S                = 0.5  # segments shorter than this are excluded — too close together to distinguish real braking from GPS position noise
```

> The file also still defines `BRAKING_DECEL_THRESHOLD_KMH_S = 40` and `BRAKING_INTENSITY_CAP_KMH_S = 50`, labeled for the CSV/wheel-rotation path. That path no longer runs (see the local-pipeline note above), so those two constants are currently dead code left over from before the archive — harmless, but worth removing next time you're in this file.

### GPS display smoothing (`generate_trips_geojson.py`)

```python
SPEED_SMOOTH_WIN = 5  # points averaged for gnss speed before it's used as map color
```

Higher = smoother color bands, less responsive to real speed changes.

### Crash/fall detection (`generate_trips_geojson.py`)

```python
CRASH_IMPACT_THRESHOLD_G      = 6.0  # peak accelerometer g to even consider an impact candidate
CRASH_BASELINE_DELTA_MIN_G    = 0.5  # how much the post-impact settled reading must differ from the pre-impact riding baseline
CRASH_GPS_STILL_MAX_SPEED_KMH = 3.0  # GPS speed ceiling to help confirm the bike actually stopped
```

A candidate only becomes a confirmed crash event if the accelerometer shows a genuine settle-after-impact (not just flatness — a real shift from the pre-impact riding baseline) **and** GPS confirms the bike came to a stop. See the full constant list and `detect_crash_events_api` in the script for the rest (clustering gap, cooldown, severity bands, etc.).

### Hotspot grid size (`generate_braking_hotspots.py`)

```python
CELL_DEG = 0.0003  # ~35 m cells at Amsterdam latitude; increase for coarser clusters
```

### Wheel diameter fallback (`generate_trips_geojson.py`)

```python
DEFAULT_WHEEL_DIAM_INCH = 28.0  # used when a trip's wheel_diam isn't set in Supabase
```

## Data quality notes

| Speed (raw, `Speed`)     | Road Quality               | Braking                          | Crash/fall detection |
| -------------------------- | --------------------------- | --------------------------------- | --------------------- |
| GPS (`gnss.speed`), 5-point rolling average smoothed | Real — from decoded accelerometer | Detected from GPS speed deltas (segments <0.5s excluded as noise) | Peak-g candidates confirmed by a pre/post-impact accelerometer baseline shift + GPS stop |

Every segment carries `Speed` — the smoothed GPS value, used for both the map color and braking/stats. **`Speed_display` does not currently exist as a separate value**: it's listed in `SEGMENT_PROPS_TO_KEEP` (implying the frontend expects it) but `rows_to_features` in `generate_trips_geojson.py` never actually sets it — only `Speed` is written. This predates today's local-pipeline archive; it's flagged here rather than described as working, since the previous version of this README documented `Speed_display` as a live, separate field. Worth checking whether `app.js` reads `Speed_display` and silently falls back, or whether this needs a fix.

## Troubleshooting

### "Braking hotspots show 0 events"
- Delete `trips.geojson` and rerun the pipeline from scratch:
  ```bash
  rm trips.geojson
  python generate_trips_geojson.py
  python generate_braking_hotspots.py
  ```
- Confirm braking data is present: `is_braking` and `braking_intensity` should appear on segment features in `trips.geojson`.

### "Map is blank"
- Check that `trips.geojson` exists in the repo root and has been pushed
- Open browser console and look for fetch errors

### "Speed shows 0 or capped at 40 for some trips"
- `MAX_SPEED_KMH = 40` in `generate_trips_geojson.py` is a hard cap; GPS speed above that is clamped, not dropped
- A value of exactly 0 usually means GPS speed was null for that fix — check the trip's `gnss` rows in Supabase

### "Trip skipped due to timeout"
- The reconstruction query for that trip takes >30s
- Increase `STATEMENT_TIMEOUT` in `generate_trips_geojson.py` and retry

### "No new trips fetched from Supabase"
- Check `.env` credentials
- Trips already present in the current `trips.geojson` are skipped by design — see `load_existing_output` in `generate_trips_geojson.py`

### "Connection error on pipeline run"
- Confirm `.env` is in the project root
- Run `pip install python-dotenv` if missing

## Use Cases

### Urban Planning
- Identify intersections where cyclists repeatedly brake — signals dangerous junctions, poor sightlines, or road surface problems
- Analyze road quality across cycling infrastructure
- Plan bike lane improvements based on actual usage patterns

### Cycling Safety
- Locate braking hotspots that may warrant infrastructure intervention
- Identify road quality problem areas
- Optimize routes to avoid hazards

### Research & Analytics
- Compare speed, road quality, and braking patterns across sensors and trips
- Export data for further analysis via the Supabase dashboard
