# 🚴 Reflector Ride Maps

A comprehensive bike sensor data visualization tool that transforms GPS and wheel rotation data into interactive, speed-colored route maps with traffic light analysis.

## Overview

This project processes raw CSV files from bike sensors and creates:
- **Speed-colored route visualizations** showing cycling speeds across trips
- **Road quality mapping** to identify infrastructure conditions
- **Interactive web visualization** powered by MapLibre GL JS and PMTiles

## Features

### **Interactive Map**
- View all trips simultaneously or focus on individual routes
- Click any route segment to see detailed speed and quality metrics
- Search for specific trips by name
- Toggle fullscreen mode for presentations

### **Speed Visualization**
- **Gradient mode**: Smooth color transitions between speeds
- **Category mode**: Distinct colors for speed ranges
- Real-time speed display on hover
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
├── csv_data/                          # Raw CSV files from sensors (you create this)
├── sensor_data/                       # Cleaned GeoJSON files (generated)
├── processed_sensor_data/             # Speed-calculated trips (generated)
├── trips.pmtiles                      # Compressed trip data for map (generated)
├── trips_metadata.json                # Trip statistics (generated)
│
├── master_pipeline.py                 # Run this to process everything
├── csv_to_geojson_converter.py        # Step 1: Convert CSVs to GeoJSON
├── integrated_processor.py            # Step 2: Calculate speeds from sensors
├── build_pmtiles.py                   # Step 4: Build PMTiles for web
│
├── index.html                         # Main visualization page
├── app.js                             # Map logic and interactions
├── config.js                          # Configuration
└── styles.css                         # Styling
```

## Quick Start

### Prerequisites

- **Python 3.x** for data processing
- **Tippecanoe** for PMTiles generation:
  ```bash
  brew install tippecanoe  # macOS
  # or see: https://github.com/felt/tippecanoe
  ```

### One-Command Processing

The easiest way to process your data:

```bash
python master_pipeline.py
```

This automated pipeline runs all processing steps:
1. ✅ Converts CSV files to GeoJSON
2. ✅ Calculates speeds from wheel rotation data
3. ✅ Averages road segments
4. ✅ Generates PMTiles for web visualization

## Detailed Workflow

### Step 1: Convert Raw CSVs to GeoJSON

Place your CSV files in a `csv_data/` folder, then run:

```bash
python csv_to_geojson_converter.py
```

**What it does:**
- Reads CSV files with GPS coordinates and sensor data
- Converts to GeoJSON format with LineString geometries
- Organizes by sensor ID (e.g., `602B3`, `604F0`)
- Extracts metadata from CSV footers
- **Output:** `sensor_data/{sensor_id}/{sensor_id}_Trip{N}_clean.geojson`

**Input CSV format:**
```csv
latitude,longitude,HH:mm:ss,SSS,marker,HRot Count,Samples,Speed
52.3644,4.9130,14:23:45,123,2,100,1000,
52.3645,4.9131,14:23:46,123,3,102,1050,234
...
Bike: Trek 820
Distance: 5.2 km
```

### Step 2: Calculate Speeds from Sensor Data

```bash
python integrated_processor.py
```

**What it does:**
- Reads cleaned GeoJSON files from `sensor_data/`
- Calculates speed using **wheel rotation (HRot)** data:
  - Uses 711mm wheel diameter (configurable)
  - Formula: `speed = (wheel_rotations × circumference) / time`
- Creates line segments only where the wheel actually moved
- Filters out stopped periods and anomalies
- Assesses road quality based on GPS accuracy and movement patterns
- **Output:** `processed_sensor_data/{sensor_id}_Trip{N}_processed.geojson`

**Key calculations:**
- Wheel circumference: ~2.073 meters
- Sample rate: 50 Hz (0.02 seconds per sample)
- Speed cap: 50 km/h (filters unrealistic values)

**Properties added:**
- `Speed`: km/h calculated from wheel rotation
- `road_quality`: 1-5 rating (1=perfect, 5=no road)
- `hrot_diff`: Wheel rotation difference
- `time_diff_s`: Time between points
- `gps_distance_m`: GPS distance for validation

### Step 3: Average Road Segments

```bash
python road_averaging.py
```

### Step 4: Build PMTiles for Web

```bash
python build_pmtiles.py
```

**What it does:**
- Uses Tippecanoe to compress processed GeoJSON into PMTiles format
- PMTiles = efficient vector tiles for web maps
- Preserves `Speed`, `road_quality`, `marker`, and `trip_id` properties
- **Output:** `trips.pmtiles` (~90% smaller than raw GeoJSON)

**Why PMTiles?**
- Efficient: Dramatically smaller file size
- Fast: Only loads visible tiles
- Standard: Works with MapLibre/Mapbox GL JS

## Web Visualization

Visit: **https://tomvanarman.github.io/Reflector-Ride-Maps/**

### Controls:

**Trip Selection:**
- **Search**: Find specific trips by name
- **Click**: Select individual routes
- **Reset**: Return to full view

**Visualization Modes:**
- **Speed**: Show speeds with gradient or categories
- **Road Quality**: Display infrastructure conditions
- **Averaged Road Segments**: Aggregated segment view

### Speed Legend:

- 🔘 Gray: Stopped (0-2 km/h)
- 🔴 Red: Very Slow (2-5 km/h)
- 🟠 Orange: Slow (5-10 km/h)
- 🟡 Yellow: Moderate (10-15 km/h)
- 🟢 Green: Fast (15-20 km/h)
- 🔵 Blue: Very Fast (20-25 km/h)
- 🟣 Purple: Extreme (25+ km/h)

### Road Quality Legend:

- 🟢 Green: Perfect (1)
- 🟢 Light Green: Normal (2)
- 🟡 Yellow: Outdated (3)
- 🟠 Orange: Bad (4)
- 🔴 Red: No Road (5)

## Configuration

### Wheel Settings (in `integrated_processor.py`):

```python
WHEEL_DIAMETER_MM = 711  
WHEEL_CIRCUMFERENCE_M = (711 / 1000) * math.pi  # ~2.234m
```

Adjust these for your specific bike wheel size.

### Map Settings (in `config.js`):

```javascript
MAP_CENTER: [4.9041, 52.3676],  // Amsterdam coordinates
MAP_ZOOM: 13,                    // Initial zoom level
MAP_STYLE: 'https://...'         // CartoDB Dark Matter
```

## Troubleshooting

### "PMTiles shows all gray/red"
- Check that `Speed` property exists in processed GeoJSON
- Verify wheel diameter matches your bike
- Rebuild: `python build_pmtiles.py`

### "Map is blank"
- Check browser console for errors
- Verify `trips.pmtiles` exists
- Confirm coordinates are in correct area

### "No trip data found"
- Ensure CSV files are in `csv_data/` folder
- Run complete pipeline: `python master_pipeline.py`
- Check that CSV format matches expected structure

## Use Cases

### Urban Planning
- Identify dangerous intersections requiring infrastructure improvements
- Analyze road quality across cycling routes
- Plan bike lane upgrades based on actual usage data

### Cycling Safety
- Locate areas where cyclists frequently slow down
- Optimize route planning to avoid problem areas

### Personal Analytics
- Track your cycling speed patterns
- Monitor road quality on regular routes
- Review trip statistics over time