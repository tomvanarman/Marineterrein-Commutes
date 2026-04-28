# 🚴 Reflector Ride Maps

A bike sensor data visualization tool that transforms GPS and accelerometer data into interactive, speed-colored route maps with road quality analysis.

## Overview

This project ingests raw sensor data — either from local CSV files or directly from a Supabase database — and renders it as a live interactive map. No file downloads required: the map fetches trip data on demand from a Supabase Edge Function.

- **Speed-colored route visualizations** showing cycling speeds across trips
- **Road quality mapping** to identify infrastructure conditions
- **Live data rendering** via Supabase — no PMTiles or file generation needed

## Features

### **Interactive Map**
- View all trips simultaneously or focus on individual routes
- Click any route segment to see speed and quality metrics
- Search for specific trips or sensors by name
- Toggle fullscreen mode for presentations

### **Speed Visualization**
- **Gradient mode**: Smooth color transitions between speeds
- **Category mode**: Distinct colors for speed ranges
- Speed range: 0-30+ km/h with 7 color categories

### **Road Quality Analysis**
- 5-level road quality rating system
- Color-coded segments: Perfect → Normal → Outdated → Bad → No Road
- Helps identify infrastructure improvements needed

### **Trip Statistics**
- Total trips, distance, and riding time
- Average and maximum speeds
- Per-trip metrics on click
- Aggregate statistics across all rides

## Project Structure

```
Reflector-Ride-Maps/
├── csv_data/                          # Raw CSV files from sensors (optional)
├── sensor_data/                       # Cleaned GeoJSON files (generated)
├── processed_sensor_data/             # Speed-calculated trips (generated)
├── trips_metadata.json                # Trip statistics (generated)
│
├── master_pipeline.py                 # Run this to process everything
├── csv_to_geojson_converter.py        # Step 1: Convert CSVs / fetch from Supabase
├── integrated_processor.py            # Step 2: Calculate speeds from sensors
├── road_averaging.py                  # Step 3: Average road segments
│
├── supabase/
│   ├── functions/trips-geojson/       # Edge Function — serves live GeoJSON to map
│   │   └── index.ts
│   └── migrations/
│       └── 20260428_reconstruct_trip_rows.sql  # DB function for trip reconstruction
│
├── index.html                         # Main visualization page
├── app.js                             # Map logic and interactions
├── config.js                          # Configuration (API URLs etc.)
├── styles.css                         # Styling
└── .env                               # Supabase credentials (never commit this)
```

## Quick Start

### Prerequisites

- **Python 3.x**
- **Python packages:** `psycopg2-binary`, `python-dotenv`, `numpy`, `geojson`
  ```bash
  pip install psycopg2-binary python-dotenv numpy geojson
  ```
- **Supabase project** with the Amsterdam dataset access (see `.env` setup below)

### Environment Setup

Create a `.env` file in the project root (this is gitignored — never commit it):

```
SUPABASE_HOST=aws-1-eu-west-1.pooler.supabase.com
SUPABASE_PORT=6543
SUPABASE_DB=postgres
SUPABASE_USER=your_username
SUPABASE_PASSWORD=your_password
```

### One-Command Processing

Fetch new trips from Supabase and run the full pipeline:

```bash
python master_pipeline.py --api
```

Or process local CSV files only:

```bash
python master_pipeline.py
```

## Detailed Workflow

### Step 1: Fetch & Convert Data

**From Supabase (recommended):**
```bash
python csv_to_geojson_converter.py --api
```
Connects to Supabase, fetches all new trips (deduplicates by trip start/end timestamp), reconstructs sensor rows, and writes clean GeoJSON to `sensor_data/`.

**From local CSVs:**

Place CSV files in `csv_data/`, then run:
```bash
python csv_to_geojson_converter.py
```

Each entry in `trips_metadata.json` is tagged with `"source": "api"` or `"source": "local_csv"` so you always know where a trip came from.

**Output:** `sensor_data/{sensor_id}/{sensor_id}_Trip{N}_clean.geojson`

### Step 2: Calculate Speeds

```bash
python integrated_processor.py
```

**For local CSV trips** — speed is calculated from wheel rotation (HRot) data:
- Uses wheel diameter from metadata (default 711mm / 28 inch)
- Formula: `speed = (wheel_rotations × circumference) / time`
- Sample rate: 50 Hz

**For API trips** — speed is taken directly from GPS (`Speed GPS` field in km/h), since wheel rotation data is not available via the reconstruction query.

Both paths also calculate road quality from accelerometer (Y-axis) data.

**Properties added:**
- `Speed`: km/h
- `road_quality`: 1–5 rating (1 = perfect, 5 = no road)
- `hrot_diff`, `time_diff_s`, `gps_distance_m`: diagnostic fields

**Output:** `processed_sensor_data/{sensor_id}/{sensor_id}_Trip{N}_processed.geojson`

### Step 3: Average Road Segments

```bash
python road_averaging.py
```

Aggregates overlapping trip segments into averaged road quality and speed scores per road segment.

**Output:** `road_segments_averaged.json`

## Web Visualization

Visit: **https://tomvanarman.github.io/Reflector-Ride-Maps/**

The map loads trip data live from a Supabase Edge Function — no file downloads, no PMTiles. Data is always up to date.

### Controls

**Trip Selection:**
- **Search**: Find trips by sensor ID or trip name
- **Click**: Select individual route segments
- **Reset**: Return to full view

**Visualization Modes:**
- **Speed**: Gradient or category color mode
- **Road Quality**: Infrastructure condition coloring
- **Averaged Road Segments**: Aggregated multi-trip view

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

## Configuration

### Map & API Settings (`config.js`)

```javascript
TRIPS_API_URL: 'https://<PROJECT_REF>.supabase.co/functions/v1/trips-geojson',
INITIAL_DAYS: 90,        // Only load trips from the last N days on startup
MAP_CENTER: [4.9041, 52.3676],
MAP_ZOOM: 13,
```

Replace `<PROJECT_REF>` with your Supabase project ref (visible in your dashboard URL).

### Wheel Settings (`integrated_processor.py`)

```python
DEFAULT_WHEEL_DIAMETER_MM = 711  # fallback if not in metadata
```

Wheel diameter is read from trip metadata where available; this is only used as a fallback.

## Supabase Edge Function

The map fetches live GeoJSON from a Supabase Edge Function at `supabase/functions/trips-geojson/`. It accepts optional query parameters:

| Parameter | Description | Default |
|-----------|-------------|---------|
| `trip_id` | Fetch a single trip by DB ID | all trips |
| `since`   | Only trips starting after this date (`YYYY-MM-DD`) | none |
| `limit`   | Max number of trips to return | 100 |

**Deploy:**
```bash
supabase functions deploy trips-geojson
```

The function depends on a PostgreSQL helper function (`reconstruct_trip_rows`) — run the SQL migration in `supabase/migrations/` once in the Supabase SQL editor before deploying.

## Deduplication

The pipeline deduplicates by **trip start/end timestamp** — so the same physical trip is never processed twice, regardless of whether it came from a local CSV or the API. Each trip in `trips_metadata.json` also carries a `"source"` field (`"api"` or `"local_csv"`) for traceability.

## Troubleshooting

### "Map is blank / no trips visible"
- Check browser console for errors
- Verify `TRIPS_API_URL` in `config.js` points to your deployed edge function
- Confirm the edge function is deployed: `supabase functions list`

### "Speed shows as 0 or 40 for all API trips"
- GPS speed (`Speed GPS`) may be null for some trips — check the `_clean.geojson` properties
- Verify the `reconstruct_trip_rows` SQL function was deployed successfully

### "No new trips fetched from Supabase"
- Check `.env` credentials
- Trips already in `trips_metadata.json` are skipped by design — delete the entry to reprocess

### "API fetch fails with connection error"
- Confirm `.env` is in the project root directory
- Run `pip install python-dotenv` if not installed

## Use Cases

### Urban Planning
- Identify intersections where cyclists frequently slow down
- Analyze road quality across cycling infrastructure
- Plan bike lane improvements based on actual usage patterns

### Cycling Safety
- Locate road quality hotspots
- Optimize routes to avoid problem areas

### Research & Analytics
- Track speed patterns across sensors and time
- Compare road conditions across trips
- Export data for further analysis via the Supabase dashboard
