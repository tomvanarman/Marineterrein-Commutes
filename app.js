// app.js
// Bike Sensor Data Visualization
// Loads live GeoJSON from Supabase Edge Function — no PMTiles download needed.

import { CONFIG } from './config.js';
import { buildLeaderboard, renderLeaderboard } from './leaderboard.js';

console.log('🚀 Starting bike visualization...');

// ─── FIX: Turf dependency safety ─────────────────────────────────────────────
// If using CDN: window.turf
// If using bundler: import * as turf from '@turf/turf'
const turfLib = (typeof turf !== 'undefined') ? turf : null;

if (!turfLib) {
  console.warn('⚠️ Turf.js not found — bbox zoom on search will be disabled');
}

// ─── API KEY ──────────────────────────────────────────────────────────────────
const ORS_API_KEY = 'eyJvcmciOiI1YjNjZTM1OTc4NTExMTAwMDFjZjYyNDgiLCJpZCI6ImZhODc1ZmQ3ODRmOTQ3MTNiNWRmMGY2NTcwYjM0YTVjIiwiaCI6Im11cm11cjY0In0=';

// Initialize map
const map = new mapboxgl.Map({
  container: 'map',
  style: CONFIG.MAP_STYLE,
  center: CONFIG.MAP_CENTER,
  zoom: CONFIG.MAP_ZOOM
});

window.map = map;

// ─── State ────────────────────────────────────────────────────────────────────
let tripIds = [];
let speedMode = 'gradient';
let showSpeedColors = false;
let showRoadQuality = false;
let selectedTrip = null;
let tripsMetadata = null;
let currentPopup = null;
let showAveragedSegments = false;
let averagedSegmentMode = 'composite';
let searchActive = false;
let activeFilter = null;
let showBraking = false;
let showBrakingHotspots = false;

// ─── Sensor colours ───────────────────────────────────────────────────────────
const SENSOR_COLORS = [
  '#34CCCC','#FFCC33','#5B8FFF','#CC5BAA','#33CCAA',
  '#FF7A3D','#88DDFF','#FFE066','#CC3355','#66FF99',
  '#AA88FF','#FF9966','#00CCFF','#FFB3DE','#44FFDD',
  '#FFAA00','#7BFFB3','#FF6680','#B3EEFF','#D4FF66',
];

const DEFAULT_COLOR = '#34CCCC';
const sensorColorMap = {};

function buildSensorColorMap(ids) {
  const sensors = [...new Set(ids.map(id => id.split('_')[0]))].sort();
  sensors.forEach((s, i) => {
    sensorColorMap[s] = SENSOR_COLORS[i % SENSOR_COLORS.length];
  });
  console.log('🎨 Sensor colour map:', sensorColorMap);
}

function getSensorColor(tripId) {
  const sensor = tripId.split('_')[0];
  return sensorColorMap[sensor] || DEFAULT_COLOR;
}

// ─── Colour expressions ───────────────────────────────────────────────────────
function getSpeedColorExpression(mode) {
  const v = ['to-number', ['coalesce', ['get', 'Speed'], ['get', 'speed'], 0]];
  if (mode === 'gradient') {
    return ['interpolate', ['linear'], v,
      0,'#808080', 2,'#DC2626', 5,'#F97316',
      10,'#FACC15', 15,'#22C55E', 20,'#3B82F6', 25,'#bb06d7'
    ];
  }
  return ['step', v,
    '#808080', 2,'#DC2626', 5,'#F97316',
    10,'#FACC15', 15,'#22C55E', 20,'#3B82F6', 25,'#bb06d7'
  ];
}

function getRoadQualityColorExpression() {
  return ['match', ['get', 'road_quality'],
    1,'#22C55E', 2,'#84CC16', 3,'#FACC15', 4,'#F97316', 5,'#DC2626',
    '#808080'
  ];
}

function getSensorColorExpression() {
  const fallback = DEFAULT_COLOR;
  const pairs = tripIds.flatMap(id => [id, getSensorColor(id)]);
  return ['match', ['get', 'trip_id'], ...pairs, fallback];
}

// ─── Braking hotspot fix-safe expression ──────────────────────────────────────
function getHotspotColorExpression() {
  return [
    'interpolate', ['linear'],
    ['to-number', ['get', 'count']],
    1, '#FFF176',
    3, '#FF9800',
    8, '#D32F2F',
  ];
}

// ─── Stats helpers ────────────────────────────────────────────────────────────
function parseDurationToSeconds(d) {
  if (!d) return 0;
  const parts = d.split(':').map(Number);
  if (parts.length === 3) return parts[0]*3600 + parts[1]*60 + parts[2];
  if (parts.length === 2) return parts[0]*60 + parts[1];
  return parts[0] || 0;
}

function formatDuration(s) {
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  return h > 0 ? `${h}h ${m}m` : `${m}m`;
}

// ─── RESET BUTTON ─────────────────────────────────────────────────────────────
function updateResetButtonVisibility() {
  const active =
    showSpeedColors ||
    showRoadQuality ||
    showAveragedSegments ||
    showBraking ||
    searchActive ||
    !!selectedTrip;

  const el = document.getElementById('resetButton');
  if (el) el.style.display = active ? 'block' : 'none';
}

// ─── FILTERING (FIXED SAFE BBOX ZOOM) ────────────────────────────────────────
function searchAndHighlightTrip(term) {
  if (!term) return;

  const q = term.toLowerCase().trim();
  const matches = tripIds.filter(id => id.toLowerCase().includes(q));

  if (matches.length === 0) {
    alert(`No trip found matching: ${term}`);
    return false;
  }

  searchActive = true;

  if (matches.length === 1) {
    selectedTrip = matches[0];
  }

  // ─── FIX: safe bbox calculation ───
  try {
    const source = map.getSource('trips');
    const features = source?._data?.features || [];

    const filtered = features.filter(f =>
      matches.includes(f.properties.trip_id)
    );

    if (filtered.length && turfLib?.bbox) {
      const bbox = turfLib.bbox({
        type: 'FeatureCollection',
        features: filtered
      });

      map.fitBounds(bbox, {
        padding: 50,
        duration: 1000
      });
    }
  } catch (err) {
    console.warn('BBox zoom skipped:', err);
  }

  return true;
}

// ─── MAP LOAD ────────────────────────────────────────────────────────────────
map.on('load', async () => {
  console.log('✅ Map loaded');

  const geojson = await (await fetch('./trips.geojson')).json();

  tripIds = [...new Set(
    geojson.features.map(f => f.properties.trip_id)
  )].sort();

  buildSensorColorMap(tripIds);

  map.addSource('trips', {
    type: 'geojson',
    data: geojson,
    tolerance: 0,          // 👈 disables simplification
    buffer: 0,             // 👈 reduces tile clipping artifacts
    lineMetrics: true      // optional but improves rendering stability
  });

  map.addLayer({
    id: 'trips-layer',
    type: 'line',
    source: 'trips',
    paint: {
      'line-color': getSensorColorExpression(),
      'line-width': 3,
      'line-opacity': 0.7
    }
  });

  map.on('click', 'trips-layer', (e) => {
    const p = e.features[0].properties;

    new mapboxgl.Popup()
      .setLngLat(e.lngLat)
      .setHTML(`
        <strong>${p.trip_id}</strong><br>
        Speed: ${p.Speed ?? '—'} km/h
      `)
      .addTo(map);
  });

  map.on('mouseenter', 'trips-layer', () => {
    map.getCanvas().style.cursor = 'pointer';
  });

  map.on('mouseleave', 'trips-layer', () => {
    map.getCanvas().style.cursor = '';
  });
});

window.searchTrip = searchAndHighlightTrip;