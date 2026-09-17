#!/usr/bin/env python3
"""
Standalone Amsterdam bike-trip animations using the same CARTO Dark Matter
basemap and the same sensor colour mapping as the dashboard.

Outputs:
  amsterdam_trips_live.gif
  amsterdam_trips_chronological.gif

Install:
  pip install playwright pillow
  playwright install chromium

Run:
  python make_amsterdam_animation_osm_v6.py trips.geojson

The browser needs internet access while rendering because MapLibre loads the
CARTO/OSM basemap tiles. The resulting GIFs are standalone files.
"""

import io
import json
import os
import sys
import tempfile
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from PIL import Image
from playwright.sync_api import sync_playwright


# ---------------------------------------------------------------------------
# Visual / animation settings
# ---------------------------------------------------------------------------

WIDTH = 1100
HEIGHT = 720
FPS = 12

MAP_STYLE = "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json"
# Slightly tighter than the dashboard default to focus on central Amsterdam.
MAP_CENTER = [4.9041, 52.3676]
MAP_ZOOM = 13.55

# Same list and ordering as the actual dashboard's SENSOR_COLORS.
SENSOR_COLORS = [
    '#34CCCC','#FFCC33','#5B8FFF','#CC5BAA','#33CCAA',
    '#FF7A3D','#88DDFF','#FFE066','#CC3355','#66FF99',
    '#AA88FF','#FF9966','#00CCFF','#FFB3DE','#44FFDD',
    '#FFAA00','#7BFFB3','#FF6680','#B3EEFF','#D4FF66',
]
DEFAULT_COLOR = '#34CCCC'

# Live mode is driven by real trip durations. 90x compression means:
#   30 min real time -> ~20 sec animation time
#   60 min real time -> ~40 sec animation time
# The longest trip determines the GIF duration.
LIVE_TIME_COMPRESSION = 90.0
# Artificial live pacing: every ride stays visible for a meaningful amount of
# time, while longer real trips still remain longer than shorter ones.
# This keeps all rides starting together but spaces out when they arrive.
LIVE_MIN_ANIMATION_S = 14.0
LIVE_DURATION_SCALE = 1.0 / 150.0
# Very short trips make poor animated paths; omit only the tiny ones.
MIN_LIVE_TRIP_DURATION_S = 8.0

# Chronological draw-on animation. This is intentionally slower than before.
CHRONO_SECONDS_PER_TRIP = 0.65

# Keep enough geometry for the route to feel like the actual street trace.
MAX_POINTS_PER_TRIP = 420


# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------

def parse_time(ts):
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()


def load_trips(path):
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    grouped = defaultdict(list)

    for feature in data.get("features", []):
        props = feature.get("properties", {})
        trip_id = props.get("trip_id")
        geom = feature.get("geometry", {})

        if not trip_id or geom.get("type") != "LineString":
            continue

        coords = geom.get("coordinates", [])
        if len(coords) >= 2:
            grouped[trip_id].append(feature)

    trips = []

    for trip_id, features in grouped.items():
        samples = []

        for feature in features:
            props = feature.get("properties", {})
            ts = props.get("timestamp")
            coords = feature["geometry"]["coordinates"]
            if not ts:
                continue

            t0 = parse_time(ts)
            dt = max(float(props.get("time_diff_s") or 0), 0.02)
            speed = float(props.get("Speed") or 0)

            samples.append((t0, coords[0][0], coords[0][1], speed))
            samples.append((t0 + dt, coords[-1][0], coords[-1][1], speed))

        if len(samples) < 2:
            continue

        samples.sort(key=lambda p: p[0])

        # Keep the final point at each timestamp.  This avoids zero-time jumps
        # while preserving the actual segment timing.
        clean = []
        for point in samples:
            if clean and point[0] == clean[-1][0]:
                clean[-1] = point
            else:
                clean.append(point)

        if len(clean) < 2:
            continue

        start = clean[0][0]
        end = clean[-1][0]
        duration = max(end - start, 0.001)

        # Downsample by TIME, not by point count. That preserves traffic-light
        # stops and other slower sections much better than uniform point picks.
        if len(clean) > MAX_POINTS_PER_TRIP:
            selected = [clean[0]]
            step = (len(clean) - 1) / (MAX_POINTS_PER_TRIP - 1)
            for i in range(1, MAX_POINTS_PER_TRIP - 1):
                selected.append(clean[round(i * step)])
            selected.append(clean[-1])
            clean = selected

        normalized = [
            ((t - start) / duration, lon, lat, speed)
            for t, lon, lat, speed in clean
        ]

        trips.append({
            "id": trip_id,
            "sensor": trip_id.split("_")[0],
            "start": start,
            "duration": duration,
            "points": normalized,
        })

    trips.sort(key=lambda t: t["start"])

    # EXACTLY mirror the dashboard's sensor mapping:
    # sorted sensor IDs -> SENSOR_COLORS by index.
    sensors = sorted({trip["sensor"] for trip in trips})
    sensor_color_map = {
        sensor: SENSOR_COLORS[i % len(SENSOR_COLORS)]
        for i, sensor in enumerate(sensors)
    }

    for trip in trips:
        trip["color"] = sensor_color_map.get(trip["sensor"], DEFAULT_COLOR)

    print(f"Loaded {len(trips)} trips from {len(sensors)} sensors.")
    print("Sensor colour map:")
    for sensor in sensors:
        print(f"  {sensor}: {sensor_color_map[sensor]}")

    durations = [trip["duration"] for trip in trips]
    if durations:
        print(
            f"Trip durations: {min(durations)/60:.1f} min min, "
            f"{sorted(durations)[len(durations)//2]/60:.1f} min median, "
            f"{max(durations)/60:.1f} min max."
        )

    return trips


# ---------------------------------------------------------------------------
# Browser / MapLibre
# ---------------------------------------------------------------------------

def make_html(trips):
    trips_json = json.dumps(trips, separators=(",", ":"))

    return f"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<link rel="stylesheet" href="https://unpkg.com/maplibre-gl@3.6.2/dist/maplibre-gl.css">
<script src="https://unpkg.com/maplibre-gl@3.6.2/dist/maplibre-gl.js"></script>
<style>
html, body {{
  margin: 0;
  padding: 0;
  width: 100%;
  height: 100%;
  overflow: hidden;
  background: #081019;
}}
#map {{ position: absolute; inset: 0; }}
#attrib {{
  display:none;
}}
</style>
</head>
<body>
<div id="map"></div>
<script>
const TRIPS = {trips_json};

const map = new maplibregl.Map({{
  container: 'map',
  style: {json.dumps(MAP_STYLE)},
  center: {json.dumps(MAP_CENTER)},
  zoom: {MAP_ZOOM},
  attributionControl: false,
  interactive: false,
  preserveDrawingBuffer: true,
  fadeDuration: 0
}});

window.__ready = false;
window.__layersReady = false;

function emptyCollection() {{ return {{type:'FeatureCollection',features:[]}}; }}

map.on('load', () => {{
  // NORMAL ROUTES: deliberately crisp. No line blur.
  map.addSource('animated-trips', {{type:'geojson', data:emptyCollection()}});

  map.addLayer({{
    id:'route-underlay', type:'line', source:'animated-trips',
    layout:{{'line-cap':'round','line-join':'round'}},
    paint:{{
      'line-color':['get','color'],
      'line-width':['get','underlayWidth'],
      'line-opacity':['get','underlayOpacity']
    }}
  }});

  map.addLayer({{
    id:'route-core', type:'line', source:'animated-trips',
    layout:{{'line-cap':'round','line-join':'round'}},
    paint:{{
      'line-color':['get','color'],
      'line-width':['get','coreWidth'],
      'line-opacity':['get','coreOpacity']
    }}
  }});

  // Separate pulse source so pulses do NOT get rendered twice by the route layers.
  map.addSource('animated-pulse', {{type:'geojson', data:emptyCollection()}});
  map.addLayer({{
    id:'route-pulse-underlay', type:'line', source:'animated-pulse',
    layout:{{'line-cap':'round','line-join':'round'}},
    paint:{{
      'line-color':['get','color'],
      'line-width':['get','pulseUnderlayWidth'],
      'line-opacity':['get','pulseUnderlayOpacity']
    }}
  }});
  map.addLayer({{
    id:'route-pulse-core', type:'line', source:'animated-pulse',
    layout:{{'line-cap':'round','line-join':'round'}},
    paint:{{
      'line-color':['get','color'],
      'line-width':['get','pulseCoreWidth'],
      'line-opacity':['get','pulseCoreOpacity']
    }}
  }});

  map.addSource('animated-heads', {{type:'geojson', data:emptyCollection()}});
  map.addLayer({{
    id:'trip-head-ring', type:'circle', source:'animated-heads',
    paint:{{
      'circle-color':['get','color'],
      'circle-radius':['get','ringRadius'],
      'circle-opacity':['get','ringOpacity'],
      'circle-stroke-color':['get','color'],
      'circle-stroke-width':1
    }}
  }});
  map.addLayer({{
    id:'trip-head-core', type:'circle', source:'animated-heads',
    paint:{{
      'circle-color':['get','color'],
      'circle-radius':['get','radius'],
      'circle-opacity':1
    }}
  }});

  window.__ready = true;
  setTimeout(() => {{ window.__layersReady = true; }}, 500);
}});

window.setFrame = function(routes, pulse, heads) {{
  map.getSource('animated-trips').setData({{type:'FeatureCollection',features:routes}});
  map.getSource('animated-pulse').setData({{type:'FeatureCollection',features:pulse}});
  map.getSource('animated-heads').setData({{type:'FeatureCollection',features:heads}});
}};
window.isReady = () => window.__ready && window.__layersReady;
</script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Geometry helpers
# ---------------------------------------------------------------------------

def interpolate(points, u):
    if u <= points[0][0]:
        return points[0]
    if u >= points[-1][0]:
        return points[-1]

    lo, hi = 0, len(points) - 1
    while lo + 1 < hi:
        mid = (lo + hi) // 2
        if points[mid][0] < u:
            lo = mid
        else:
            hi = mid

    a = points[lo]
    b = points[hi]
    q = (u - a[0]) / max(b[0] - a[0], 1e-9)
    return (
        u,
        a[1] + q * (b[1] - a[1]),
        a[2] + q * (b[2] - a[2]),
        a[3] + q * (b[3] - a[3]),
    )


def partial_points(points, u):
    if u <= 0:
        return [points[0]]
    if u >= 1:
        return list(points)

    result = [points[0]]
    for i in range(1, len(points)):
        a = points[i - 1]
        b = points[i]
        if b[0] >= u:
            q = (u - a[0]) / max(b[0] - a[0], 1e-9)
            result.append((
                u,
                a[1] + q * (b[1] - a[1]),
                a[2] + q * (b[2] - a[2]),
                a[3] + q * (b[3] - a[3]),
            ))
            break
        result.append(b)
    return result


def pulse_segment(points, head_u, tail_width=0.045):
    # The moving pulse is a TRAIL behind the head, never ahead of it.
    # This keeps the dot visually anchored at the front of the active line.
    lo = max(points[0][0], head_u - tail_width)
    hi = min(points[-1][0], head_u)
    seg = [p for p in points if lo <= p[0] <= hi]

    if lo < head_u and lo > points[0][0]:
        seg.insert(0, interpolate(points, lo))
    if hi > points[0][0] and hi < points[-1][0]:
        seg.append(interpolate(points, hi))

    return seg if len(seg) >= 2 else []


def route_feature(trip, points, active=False, opacity=None):
    if len(points) < 2:
        return None

    return {
        'type':'Feature',
        'geometry':{
            'type':'LineString',
            'coordinates':[[p[1], p[2]] for p in points]
        },
        'properties':{
            'color':trip['color'],
            'underlayWidth':3.1 if active else 2.7,
            'underlayOpacity':0.20 if active else 0.12,
            'coreWidth':1.45 if active else 1.20,
            'coreOpacity':0.96 if opacity is None else opacity,
        }
    }


def pulse_feature(trip, points):
    if len(points) < 2:
        return None

    return {
        'type':'Feature',
        'geometry':{
            'type':'LineString',
            'coordinates':[[p[1], p[2]] for p in points]
        },
        'properties':{
            'color':trip['color'],
            'pulseUnderlayWidth':4.1,
            'pulseUnderlayOpacity':0.38,
            'pulseCoreWidth':1.9,
            'pulseCoreOpacity':1.0,
        }
    }


def head_feature(trip, p, stopped=False):
    return {
        'type':'Feature',
        'geometry':{'type':'Point','coordinates':[p[1],p[2]]},
        'properties':{
            'color':trip['color'],
            'radius':3.2 if not stopped else 3.8,
            'ringRadius':6.8 if not stopped else 9.2,
            'ringOpacity':0.28 if not stopped else 0.55,
        }
    }


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def save_gif(frames, path):
    # Adaptive palette keeps the basemap reasonably detailed while retaining
    # strong sensor colours.
    paletted = [
        frame.convert('P', palette=Image.Palette.ADAPTIVE, colors=192)
        for frame in frames
    ]
    paletted[0].save(
        path,
        save_all=True,
        append_images=paletted[1:],
        duration=int(1000 / FPS),
        loop=0,
        optimize=False,
        disposal=2,
    )


def render(output_dir, geojson_path):
    trips = load_trips(geojson_path)
    if not trips:
        raise SystemExit('No usable trips found in the GeoJSON.')

    html = make_html(trips)

    with tempfile.TemporaryDirectory() as tmp:
        html_path = Path(tmp) / 'animation.html'
        html_path.write_text(html, encoding='utf-8')

        with sync_playwright() as playwright:
            browser = playwright.chromium.launch()
            page = browser.new_page(
                viewport={'width':WIDTH,'height':HEIGHT},
                device_scale_factor=1,
            )
            page.goto(html_path.as_uri(), wait_until='networkidle')
            page.wait_for_function('window.isReady()', timeout=30000)

            def capture(path, total_frames, builder):
                frames = []
                for i in range(total_frames):
                    u = i / max(total_frames - 1, 1)
                    routes, pulse, heads = builder(u)

                    page.evaluate(
                        "(data) => window.setFrame(data.routes, data.pulse, data.heads)",
                        {'routes':routes, 'pulse':pulse, 'heads':heads},
                    )
                    page.wait_for_timeout(int(1000 / FPS))

                    png = page.screenshot(type='png')
                    frames.append(Image.open(io.BytesIO(png)).convert('RGB'))

                save_gif(frames, path)
                print(f"Wrote {path} ({os.path.getsize(path)/1e6:.1f} MB, {total_frames} frames)")

            # ---------------------------------------------------------------
            # LIVE: all rides start together, but arrival times are artificially
            # spaced so the map slowly populates instead of emptying early.
            # ---------------------------------------------------------------
            # Artificially space arrival times so the city populates gradually.
            # Every ride starts at t=0, but each gets a paced animation duration:
            # a 30-minute real trip is ~26s, a 60-minute trip ~38s, etc.
            # Omit only genuinely tiny trips; every other ride participates from frame 0.
            live_trips = [
                trip for trip in trips
                if trip['duration'] >= MIN_LIVE_TRIP_DURATION_S
            ]

            print(f"Live animation: {len(live_trips)} trips included; "
                  f"{len(trips) - len(live_trips)} very short trips omitted.")

            if not live_trips:
                raise RuntimeError('No trips long enough for the live animation.')

            for trip in live_trips:
                trip['live_duration'] = LIVE_MIN_ANIMATION_S + (trip['duration'] * LIVE_DURATION_SCALE)

            longest_live = max(trip['live_duration'] for trip in live_trips)
            live_frames = max(2, int(longest_live * FPS) + 1)

            def live_builder(u):
                routes, pulse, heads = [], [], []
                global_elapsed = u * longest_live

                for trip in live_trips:
                    trip_elapsed = global_elapsed
                    progress = trip_elapsed / trip['live_duration']

                    if progress < 0:
                        continue

                    if progress >= 1:
                        progress = 1.0
                        visible = trip['points']
                    else:
                        visible = partial_points(trip['points'], progress)

                    # Once a trip is complete, leave its line in place.
                    route = route_feature(trip, visible, active=(progress < 1), opacity=0.68 if progress >= 1 else 0.96)
                    if route:
                        routes.append(route)

                    if progress < 1:
                        p = interpolate(trip['points'], progress)
                        pulse_geom = pulse_segment(trip['points'], progress, tail_width=0.045)
                        pulse_feature_obj = pulse_feature(trip, pulse_geom)
                        if pulse_feature_obj:
                            pulse.append(pulse_feature_obj)
                        heads.append(head_feature(trip, p, stopped=False))

                return routes, pulse, heads

            # ---------------------------------------------------------------
            # CHRONO: sorted by trip start time, one trip draws on at a time.
            # ---------------------------------------------------------------
            chrono_total = max(1, len(trips)) * CHRONO_SECONDS_PER_TRIP
            chrono_frames = max(2, int(chrono_total * FPS) + 1)

            def chronological_builder(u):
                # CHRONOLOGICAL MODE IS INTENTIONALLY DIFFERENT FROM LIVE MODE:
                # no moving dots and no travelling pulse. Each recorded trip is
                # simply drawn onto the city in chronological order and remains
                # visible once completed.
                routes, pulse, heads = [], [], []
                exact = u * len(trips)
                complete = min(int(exact), len(trips))
                frac = exact - complete

                # Keep all previously completed trips on the map.
                for i in range(complete):
                    trip = trips[i]
                    route = route_feature(trip, trip['points'], active=False, opacity=0.76)
                    if route:
                        routes.append(route)

                # Draw the next trip progressively. This is deliberately clean:
                # no head dot, no animated pulse, just a crisp line extending
                # along the actual route.
                if complete < len(trips):
                    trip = trips[complete]
                    reveal = min(1.0, frac)
                    visible = partial_points(trip['points'], reveal)
                    route = route_feature(trip, visible, active=True, opacity=0.98)
                    if route:
                        routes.append(route)

                return routes, pulse, heads

            output_dir = Path(output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)

            capture(
                output_dir / 'amsterdam_trips_live.gif',
                live_frames,
                live_builder,
            )
            capture(
                output_dir / 'amsterdam_trips_chronological.gif',
                chrono_frames,
                chronological_builder,
            )

            browser.close()


if __name__ == '__main__':
    if len(sys.argv) != 2:
        raise SystemExit('Usage: python make_amsterdam_animation_osm_v7.py trips.geojson')

    # Write outputs beside the script when running locally on the Mac.
    render(Path(__file__).resolve().parent, sys.argv[1])
