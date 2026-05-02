// app.js
// Bike Sensor Data Visualization
// Loads live GeoJSON from Supabase Edge Function — no PMTiles download needed.

import { CONFIG } from './config.js';
import { buildLeaderboard, renderLeaderboard } from './leaderboard.js';

console.log('🚀 Starting bike visualization...');
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
let tripIds = [];                       // All unique trip_id strings in the loaded GeoJSON
let speedMode = 'gradient';
let showSpeedColors = false;
let showRoadQuality = false;
let selectedTrip = null;
let tripsMetadata = null;
let currentPopup = null;
let showAveragedSegments = false;
let averagedSegmentMode = 'composite';
let searchActive = false;
let activeFilter = null;                // trip_id string currently highlighted, or null

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
  sensors.forEach((s, i) => { sensorColorMap[s] = SENSOR_COLORS[i % SENSOR_COLORS.length]; });
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
    return ['interpolate', ['linear'], v, 0,'#808080', 2,'#DC2626', 5,'#F97316', 10,'#FACC15', 15,'#22C55E', 20,'#3B82F6', 25,'#6366F1'];
  }
  return ['step', v, '#808080', 2,'#DC2626', 5,'#F97316', 10,'#FACC15', 15,'#22C55E', 20,'#3B82F6', 25,'#6366F1'];
}

function getRoadQualityColorExpression() {
  return ['match', ['get', 'road_quality'], 1,'#22C55E', 2,'#84CC16', 3,'#FACC15', 4,'#F97316', 5,'#DC2626', '#808080'];
}

// Sensor-colour expression based on trip_id property in each feature
function getSensorColorExpression() {
  const fallback = DEFAULT_COLOR;
  // Build a match expression: ['match', ['get', 'trip_id'], 'A', '#color', 'B', '#color', fallback]
  const pairs = tripIds.flatMap(id => [id, getSensorColor(id)]);
  if (pairs.length === 0) return fallback;
  return ['match', ['get', 'trip_id'], ...pairs, fallback];
}

function getAveragedSpeedColorExpression() {
  return ['interpolate', ['linear'], ['get', 'avg_speed'], 0,'#DC2626', 5,'#F97316', 10,'#FACC15', 15,'#22C55E', 20,'#3B82F6', 25,'#6366F1'];
}
function getAveragedQualityColorExpression() {
  return ['interpolate', ['linear'], ['get', 'avg_quality'], 1,'#22C55E', 2,'#84CC16', 3,'#FACC15', 4,'#F97316', 5,'#DC2626'];
}
function getCompositeScoreColorExpression() {
  return ['interpolate', ['linear'], ['get', 'composite_score'], 0,'#22C55E', 25,'#84CC16', 50,'#FACC15', 75,'#F97316', 100,'#DC2626'];
}

function getQualityLabel(q) {
  if (q <= 1.5) return 'Perfect';
  if (q <= 2.5) return 'Normal';
  if (q <= 3.5) return 'Outdated';
  if (q <= 4.5) return 'Bad';
  return 'No road';
}
function getCompositeLabel(s) {
  if (s < 20) return 'Excellent';
  if (s < 40) return 'Good';
  if (s < 60) return 'Moderate';
  if (s < 80) return 'Poor';
  return 'Critical';
}

// ─── Data loading ─────────────────────────────────────────────────────────────

async function loadMetadata() {
  const paths = [`${CONFIG.DATA_URL}trips_metadata.json`, './trips_metadata.json', 'trips_metadata.json'];
  for (const path of paths) {
    try {
      const r = await fetch(path);
      if (r.ok) {
        tripsMetadata = await r.json();
        console.log('✅ Metadata loaded for', Object.keys(tripsMetadata).length, 'trips');
        return tripsMetadata;
      }
    } catch {}
  }
  console.warn('⚠️ Could not load metadata');
  return null;
}

async function loadTripsGeoJSON() {
  // trips.geojson is pre-built by generate_trips_geojson.py which merges:
  //   - processed_sensor_data/ (real road quality + wheel speed)
  //   - Supabase API trips (GPS speed, road_quality=0)
  // Regenerate it locally whenever you have new data, then commit + push.
  const loadingEl = document.getElementById('loadingIndicator');
  if (loadingEl) loadingEl.style.display = 'block';

  try {
    const r = await fetch('./trips.geojson');
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const geojson = await r.json();
    console.log(`✅ Loaded trips.geojson — ${geojson.features?.length ?? 0} segments`);
    return geojson;
  } catch (err) {
    console.error('❌ Could not load trips.geojson:', err);
    return { type: 'FeatureCollection', features: [] };
  } finally {
    if (loadingEl) loadingEl.style.display = 'none';
  }
}

async function loadAveragedSegments() {
  const paths = ['./road_segments_averaged.json', `${CONFIG.DATA_URL}road_segments_averaged.json`];
  for (const path of paths) {
    try {
      const r = await fetch(path);
      if (r.ok) {
        const data = await r.json();
        console.log(`✅ Loaded ${data.features.length} averaged segments`);
        return data;
      }
    } catch {}
  }
  console.error('❌ Could not load averaged segments');
  return null;
}

// ─── Stats helpers ────────────────────────────────────────────────────────────
function getTripStats(tripId) {
  if (!tripsMetadata) return null;

  const variants = [
    tripId,
    tripId.replace(/_clean_processed$/i, ''),
    tripId.replace(/_clean$/i, ''),
    tripId.replace(/_processed$/i, ''),
    tripId.split('_clean')[0],
    tripId.split('_processed')[0],
  ];
  const m = tripId.match(/^(.+_Trip\d+)/i);
  if (m) variants.push(m[1]);

  for (const v of variants) {
    if (tripsMetadata[v]) {
      const meta = tripsMetadata[v].metadata || tripsMetadata[v];
      const gnss = meta['GNSS'];
      if (!gnss) return null;
      const parts = gnss.split(',');
      return {
        duration: parts[1], stops: parts[2],
        distance: parseFloat(parts[3]) || 0,
        avgSpeed: parseFloat(parts[4]) || 0,
        avgSpeedWOS: parseFloat(parts[5]) || 0,
        maxSpeed: parseFloat(parts[6]) || 0,
        elevation: parseFloat(parts[11]) || 0,
      };
    }
  }
  return null;
}

function parseDurationToSeconds(d) {
  if (!d) return 0;
  const parts = d.split(':').map(Number);
  if (parts.length === 3) return parts[0] * 3600 + parts[1] * 60 + parts[2];
  if (parts.length === 2) return parts[0] * 60 + parts[1];
  return parts[0] || 0;
}

function formatDuration(s) {
  const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60);
  return h > 0 ? `${h}h ${m}m` : `${m}m`;
}

function calculateAggregateStats() {
  if (!tripsMetadata) return null;
  let totalDist = 0, totalTime = 0, totalAvg = 0, count = 0;
  Object.keys(tripsMetadata).forEach(id => {
    const s = getTripStats(id);
    if (s) { totalDist += s.distance; totalTime += parseDurationToSeconds(s.duration); totalAvg += s.avgSpeed; count++; }
  });
  return count > 0 ? { tripCount: count, totalDistance: totalDist.toFixed(1), totalTime: formatDuration(totalTime), avgSpeed: (totalAvg / count).toFixed(1) } : null;
}

// ─── Layer paint helpers ──────────────────────────────────────────────────────

// Return the correct line-color expression given current UI state and optional trip filter
function currentColorExpression(forTripId = null) {
  if (showSpeedColors)  return getSpeedColorExpression(speedMode);
  if (showRoadQuality)  return getRoadQualityColorExpression();
  return getSensorColorExpression();
}

function applyTripFilter(filterTripId) {
  activeFilter = filterTripId;
  if (!map.getLayer('trips-layer')) return;

  if (filterTripId) {
    // Highlight matching trips, fade the rest
    map.setPaintProperty('trips-layer', 'line-color', [
      'case',
      ['==', ['get', 'trip_id'], filterTripId], '#FF69B4',
      'rgba(255,255,255,0.08)'
    ]);
    map.setPaintProperty('trips-layer', 'line-opacity', 1);
    map.setPaintProperty('trips-layer', 'line-width', [
      'case', ['==', ['get', 'trip_id'], filterTripId], 4, 1
    ]);
  } else {
    map.setPaintProperty('trips-layer', 'line-color', currentColorExpression());
    map.setPaintProperty('trips-layer', 'line-opacity', 0.7);
    map.setPaintProperty('trips-layer', 'line-width', 3);
  }
}

function applyGroupFilter(matchingIds) {
  if (!map.getLayer('trips-layer')) return;
  const set = new Set(matchingIds);
  map.setPaintProperty('trips-layer', 'line-color', [
    'case',
    ['in', ['get', 'trip_id'], ['literal', [...set]]], '#FF69B4',
    'rgba(255,255,255,0.08)'
  ]);
  map.setPaintProperty('trips-layer', 'line-opacity', 1);
  map.setPaintProperty('trips-layer', 'line-width', [
    'case', ['in', ['get', 'trip_id'], ['literal', [...set]]], 4, 1
  ]);
}

// ─── Selection / search ───────────────────────────────────────────────────────
function resetSelection() {
  selectedTrip = null;
  activeFilter = null;
  searchActive = false;

  if (currentPopup) { currentPopup.remove(); currentPopup = null; }
  applyTripFilter(null);

  document.getElementById('resetButton').style.display = 'none';
  document.getElementById('selectedTripRow').style.display = 'none';
  document.getElementById('statTripRow').style.display = 'flex';
  document.getElementById('statDistanceRow').style.display = 'flex';
  document.getElementById('statAvgSpeedRow').style.display = 'flex';
  document.getElementById('statTotalTimeRow').style.display = 'flex';
}

function clearSearch() {
  searchActive = false;
  selectedTrip = null;
  const input = document.getElementById('tripSearchInput');
  const clearBtn = document.getElementById('tripClearButton');
  if (input) input.value = '';
  if (clearBtn) clearBtn.style.display = 'none';
  if (currentPopup) { currentPopup.remove(); currentPopup = null; }
  applyTripFilter(null);
  document.getElementById('resetButton').style.display = 'none';
  document.getElementById('selectedTripRow').style.display = 'none';
  document.getElementById('statTripRow').style.display = 'flex';
  document.getElementById('statDistanceRow').style.display = 'flex';
  document.getElementById('statAvgSpeedRow').style.display = 'flex';
  document.getElementById('statTotalTimeRow').style.display = 'flex';
}

function showSelection(tripId) {
  document.getElementById('resetButton').style.display = 'block';
  document.getElementById('statTripRow').style.display = 'none';
  document.getElementById('statDistanceRow').style.display = 'none';
  document.getElementById('statAvgSpeedRow').style.display = 'none';
  document.getElementById('statTotalTimeRow').style.display = 'none';
  document.getElementById('selectedTripRow').style.display = 'flex';
  const name = tripId.replace(/_/g, ' ').replace(/processed/gi, '').replace(/clean/gi, '').trim();
  document.getElementById('selectedTrip').textContent = name;
}

function searchAndHighlightTrip(term) {
  if (!term) { resetSelection(); return; }

  const q = term.toLowerCase().trim();
  const matches = tripIds.filter(id => id.toLowerCase().includes(q));

  if (matches.length === 0) {
    alert(`No trip found matching: ${term}`);
    return false;
  }

  searchActive = true;
  const clearBtn = document.getElementById('tripClearButton');
  if (clearBtn) clearBtn.style.display = 'inline-block';

  if (matches.length === 1) {
    selectedTrip = matches[0];
    applyTripFilter(matches[0]);
    showSelection(matches[0]);
  } else {
    selectedTrip = null;
    applyGroupFilter(matches);
    document.getElementById('resetButton').style.display = 'block';
    document.getElementById('statTripRow').style.display = 'none';
    document.getElementById('statDistanceRow').style.display = 'none';
    document.getElementById('statAvgSpeedRow').style.display = 'none';
    document.getElementById('statTotalTimeRow').style.display = 'none';
    document.getElementById('selectedTripRow').style.display = 'flex';
    document.getElementById('selectedTrip').textContent = `${term.toUpperCase()} — ${matches.length} trips`;
  }

  // Zoom to matched features
  try {
    const features = map.querySourceFeatures('trips', {
      filter: ['in', ['get', 'trip_id'], ['literal', matches]]
    });
    if (features.length > 0) {
      const bbox = turf.bbox({ type: 'FeatureCollection', features });
      map.fitBounds(bbox, { padding: 50, duration: 1000 });
    }
  } catch (err) {
    console.error('Zoom error:', err);
  }

  return true;
}

// ─── Averaged segments ────────────────────────────────────────────────────────
function updateAveragedSegmentColors() {
  if (!map.getLayer('averaged-segments')) return;
  const exprs = { speed: getAveragedSpeedColorExpression(), quality: getAveragedQualityColorExpression(), composite: getCompositeScoreColorExpression() };
  map.setPaintProperty('averaged-segments', 'circle-color', exprs[averagedSegmentMode]);
}

async function setupAveragedSegments() {
  const data = await loadAveragedSegments();
  if (!data) return;

  const pointFeatures = data.features.map(f => {
    const coords = f.geometry.coordinates;
    const midLng = coords.reduce((s, c) => s + c[0], 0) / coords.length;
    const midLat = coords.reduce((s, c) => s + c[1], 0) / coords.length;
    return { type: 'Feature', geometry: { type: 'Point', coordinates: [midLng, midLat] }, properties: f.properties };
  });

  map.addSource('averaged-segments', { type: 'geojson', data: { type: 'FeatureCollection', features: pointFeatures } });
  map.addLayer({
    id: 'averaged-segments', type: 'circle', source: 'averaged-segments',
    layout: { visibility: 'none' },
    paint: {
      'circle-color': getCompositeScoreColorExpression(),
      'circle-radius': ['interpolate', ['linear'], ['zoom'], 10, 18, 13, 28, 16, 45],
      'circle-blur': 1.2, 'circle-opacity': 0.6, 'circle-pitch-alignment': 'map'
    }
  });

  map.on('click', 'averaged-segments', (e) => {
    e.preventDefault();
    if (e.originalEvent) e.originalEvent.stopPropagation();
    const p = e.features[0].properties;
    const qualityText = p.avg_quality ? `🛣️ Avg Quality: ${p.avg_quality} (${getQualityLabel(p.avg_quality)})` : '🛣️ Quality: No data';
    new mapboxgl.Popup().setLngLat(e.lngLat).setHTML(`
      <strong>📊 Averaged Road Segment</strong><br>
      🚴 Avg Speed: ${p.avg_speed} km/h<br>
      📈 Speed Range: ${p.min_speed} - ${p.max_speed} km/h<br>
      ${qualityText}<br>
      📏 Distance: ${p.distance_m}m<br>
      🎯 Composite Score: ${p.composite_score} (${getCompositeLabel(p.composite_score)})<br>
      📍 Observations: ${p.observation_count}<br>
      🚲 From ${p.trip_count} trips
    `).addTo(map);
  });
  map.on('mouseenter', 'averaged-segments', () => { map.getCanvas().style.cursor = 'pointer'; });
  map.on('mouseleave', 'averaged-segments', () => { map.getCanvas().style.cursor = ''; });

  console.log('✅ Averaged segments layer added');
}

// ─── Isochrone ────────────────────────────────────────────────────────────────
async function updateIsochrone(active) {
  const spinner = document.getElementById('isoSpinner');
  if (!active) {
    if (map.getLayer('isoLayer')) map.setLayoutProperty('isoLayer', 'visibility', 'none');
    return;
  }
  if (map.getSource('isoSource')) {
    map.setLayoutProperty('isoLayer', 'visibility', 'visible');
    return;
  }
  if (spinner) spinner.style.display = 'inline-block';
  try {
    const r = await fetch('https://api.openrouteservice.org/v2/isochrones/cycling-regular', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': ORS_API_KEY },
      body: JSON.stringify({ locations: [CONFIG.MAP_CENTER], range: [300, 600, 1800], range_type: 'time' })
    });
    const data = await r.json();
    map.addSource('isoSource', { type: 'geojson', data });
    map.addLayer({
      id: 'isoLayer', type: 'fill', source: 'isoSource',
      paint: {
        'fill-color': ['interpolate', ['linear'], ['get', 'value'], 300,'#34CCCC', 600,'#FFCC33', 1800,'#FF4444'],
        'fill-opacity': 0.15
      }
    }, 'trips-layer');
  } catch (err) {
    console.error('Isochrone error:', err);
  } finally {
    if (spinner) spinner.style.display = 'none';
  }
}

// ─── Map load ─────────────────────────────────────────────────────────────────
map.on('error', e => console.error('❌ Map error:', e));

map.on('load', async () => {
  console.log('✅ Map loaded');
  await loadMetadata();

  try {
    const geojson = await loadTripsGeoJSON();

    // Build leaderboard from loaded GeoJSON data
    const sensors = buildLeaderboard(geojson.features);
    renderLeaderboard(sensors);

    // Extract unique trip IDs
    tripIds = [...new Set((geojson.features || []).map(f => f.properties.trip_id).filter(Boolean))].sort();
    console.log(`📊 ${tripIds.length} unique trips loaded`);

    buildSensorColorMap(tripIds);

    // Single GeoJSON source + single line layer (filtered by expressions)
    map.addSource('trips', { type: 'geojson', data: geojson, attribution: 'Bike sensor data' });

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

    // Click handler on the single layer
    map.on('click', 'trips-layer', async (e) => {
      e.preventDefault();
      if (e.originalEvent) e.originalEvent.stopPropagation();

      if (currentPopup) { currentPopup.remove(); }

      const props = e.features[0].properties;
      const tripId = props.trip_id;
      const speed = parseFloat(props.Speed || props.speed || 0);
      const roadQuality = parseInt(props.road_quality || 0);

      selectedTrip = tripId;
      applyTripFilter(tripId);
      showSelection(tripId);

      const stats = getTripStats(tripId);
      const distanceKm = stats ? stats.distance.toFixed(2) : '—';
      const avgSpeed   = stats ? stats.avgSpeed.toFixed(1)  : '—';
      const maxSpeed   = stats ? stats.maxSpeed.toFixed(1)  : '—';
      const duration   = stats ? stats.duration             : '—';

      const qualityLabels = { 0:'Unknown', 1:'Perfect', 2:'Normal', 3:'Outdated', 4:'Bad', 5:'No road' };
      const popupName = tripId.replace(/_/g, ' ').trim();

      currentPopup = new mapboxgl.Popup()
        .setLngLat(e.lngLat)
        .setHTML(`
          <strong>${popupName}</strong><br>
          🚴 Speed at point: ${speed} km/h<br>
          🛣️ Road quality: ${roadQuality} (${qualityLabels[roadQuality] || 'Unknown'})<br>
          📊 Average speed: ${avgSpeed} km/h<br>
          🏁 Max speed: ${maxSpeed} km/h<br>
          📍 Total distance: ${distanceKm} km<br>
          ⏱️ Duration: ${duration}
        `)
        .addTo(map);
    });

    map.on('mouseenter', 'trips-layer', () => { map.getCanvas().style.cursor = 'pointer'; });
    map.on('mouseleave', 'trips-layer', () => { map.getCanvas().style.cursor = ''; });

    // Click anywhere else to deselect
    map.on('click', e => {
      if (!e.defaultPrevented) {
        if (searchActive) clearSearch();
        else if (selectedTrip) resetSelection();
      }
    });

    await setupAveragedSegments();

    setupControls();
    updateStatsFromMetadata();
    renderSensorLegend();
    updateStatsVisibility();

  } catch (err) {
    console.error('❌ Error loading trips:', err);
  }

  // Marineterrein boundary
  const boundary = [
    [4.914554,52.375853],[4.913224,52.374972],[4.914403,52.373225],
    [4.915884,52.373577],[4.916600,52.373163],[4.915520,52.372566],
    [4.915402,52.372642],[4.914957,52.372472],[4.915706,52.371622],
    [4.917090,52.372080],[4.920670,52.374342],[4.920837,52.374886],
    [4.914554,52.375853]
  ];
  map.addSource('marineterrein-outline', { type:'geojson', data:{ type:'Feature', geometry:{ type:'LineString', coordinates: boundary } } });
  map.addLayer({ id:'marineterrein-outline-layer', type:'line', source:'marineterrein-outline', layout:{ 'line-join':'round','line-cap':'round' }, paint:{ 'line-color':'#ffffff','line-width':2,'line-dasharray':[3,2],'line-opacity':0.8 } });
});

// ─── UI helpers ───────────────────────────────────────────────────────────────
function isFilteredMode() { return showSpeedColors || showRoadQuality || showAveragedSegments || searchActive; }

function updateStatsVisibility() {
  const statsEl = document.getElementById('stats');
  if (statsEl) statsEl.style.display = (window.innerWidth <= 768 && isFilteredMode()) ? 'none' : 'block';
  const sensorLegend = document.getElementById('sensorLegend');
  if (sensorLegend) sensorLegend.style.display = isFilteredMode() ? 'none' : 'block';
}

window.addEventListener('resize', updateStatsVisibility);

function updateLegendPositions() {
  const order = ['averagedSegmentsLegend','speedLegend','roadQualityLegend','sensorLegend'];
  const visible = order.map(id => document.getElementById(id)).filter(el => el && el.style.display === 'block');
  const mobile = window.matchMedia('(max-width: 768px)').matches;
  updateStatsVisibility();

  if (mobile) {
    let b = 10;
    visible.forEach(el => { el.style.right = '10px'; el.style.bottom = `${b}px`; b += (el.offsetHeight || 150) + 8; });
  } else {
    let r = 10;
    visible.forEach(el => { el.style.bottom = '10px'; el.style.right = `${r}px`; r += (el.offsetWidth || 220) + 10; });
  }
}

function setupAveragedSegmentControls() {
  const cb = document.getElementById('averagedSegmentsCheckbox');
  if (cb) {
    cb.addEventListener('change', e => {
      showAveragedSegments = e.target.checked;
      const modeGroup = document.getElementById('averagedModeGroup');
      const legend = document.getElementById('averagedSegmentsLegend');
      const sensorLegend = document.getElementById('sensorLegend');

      if (showAveragedSegments) {
        if (map.getLayer('averaged-segments')) map.setLayoutProperty('averaged-segments', 'visibility', 'visible');
        if (modeGroup) modeGroup.style.display = 'flex';
        if (legend) legend.style.display = 'block';
        if (sensorLegend) sensorLegend.style.display = 'none';
        if (map.getLayer('trips-layer')) map.setLayoutProperty('trips-layer', 'visibility', 'none');
        updateAveragedSegmentColors();
      } else {
        if (map.getLayer('averaged-segments')) map.setLayoutProperty('averaged-segments', 'visibility', 'none');
        if (modeGroup) modeGroup.style.display = 'none';
        if (legend) legend.style.display = 'none';
        if (sensorLegend) sensorLegend.style.display = 'block';
        if (map.getLayer('trips-layer')) map.setLayoutProperty('trips-layer', 'visibility', 'visible');
      }
      setTimeout(updateLegendPositions, 50);
      updateStatsVisibility();
    });
  }
  document.querySelectorAll('input[name="averagedMode"]').forEach(r => {
    r.addEventListener('change', e => { averagedSegmentMode = e.target.value; if (showAveragedSegments) updateAveragedSegmentColors(); });
  });
}

function setupControls() {
  const resetBtn = document.getElementById('resetButton');
  if (resetBtn) resetBtn.addEventListener('click', resetSelection);

  // Search
  const searchInput = document.getElementById('tripSearchInput');
  const searchButton = document.getElementById('tripSearchButton');
  const suggestionBox = document.getElementById('searchSuggestions');

  if (searchInput && searchButton && suggestionBox) {
    function getSensorNames() { return [...new Set(tripIds.map(id => id.split('_')[0]))].sort(); }
    function hideSuggestions() { suggestionBox.style.display = 'none'; suggestionBox.innerHTML = ''; }
    function showSuggestions(query) {
      const q = query.trim().toLowerCase();
      suggestionBox.innerHTML = '';
      if (!q) { hideSuggestions(); return; }
      const sensors = getSensorNames().filter(s => s.toLowerCase().startsWith(q));
      const trips = tripIds.filter(id => id.toLowerCase().startsWith(q) && !sensors.some(s => id.startsWith(s)));
      if (!sensors.length && !trips.length) { hideSuggestions(); return; }
      sensors.forEach(sensor => {
        const count = tripIds.filter(id => id.startsWith(sensor)).length;
        const li = document.createElement('li');
        li.textContent = `📡 ${sensor}  (${count} trip${count !== 1 ? 's' : ''})`;
        li.className = 'suggestion-sensor';
        li.addEventListener('mousedown', () => { searchInput.value = sensor; hideSuggestions(); searchAndHighlightTrip(sensor); });
        suggestionBox.appendChild(li);
      });
      trips.forEach(tripId => {
        const li = document.createElement('li');
        li.textContent = `🚴 ${tripId}`;
        li.className = 'suggestion-trip';
        li.addEventListener('mousedown', () => { searchInput.value = tripId; hideSuggestions(); searchAndHighlightTrip(tripId); });
        suggestionBox.appendChild(li);
      });
      suggestionBox.style.display = 'block';
    }

    searchButton.addEventListener('click', () => { hideSuggestions(); searchAndHighlightTrip(searchInput.value); });
    const clearBtn = document.getElementById('tripClearButton');
    if (clearBtn) clearBtn.addEventListener('click', () => { hideSuggestions(); clearSearch(); });
    searchInput.addEventListener('keypress', e => { if (e.key === 'Enter') { hideSuggestions(); searchAndHighlightTrip(searchInput.value); } });
    searchInput.addEventListener('input', e => showSuggestions(e.target.value));
    searchInput.addEventListener('focus', e => { if (e.target.value) showSuggestions(e.target.value); });
    searchInput.addEventListener('blur', () => setTimeout(hideSuggestions, 150));
  }

  // Speed colours
  const speedCb = document.getElementById('speedColorsCheckbox');
  if (speedCb) {
    speedCb.addEventListener('change', e => {
      showSpeedColors = e.target.checked;
      if (showSpeedColors && showRoadQuality) {
        showRoadQuality = false;
        document.getElementById('roadQualityCheckbox').checked = false;
        document.getElementById('roadQualityLegend').style.display = 'none';
      }
      const legend = document.getElementById('speedLegend');
      const modeGroup = document.getElementById('speedModeGroup');
      if (showSpeedColors) {
        map.setPaintProperty('trips-layer', 'line-color', getSpeedColorExpression(speedMode));
        if (legend) legend.style.display = 'block';
        if (modeGroup) modeGroup.style.display = 'flex';
      } else {
        map.setPaintProperty('trips-layer', 'line-color', getSensorColorExpression());
        if (legend) legend.style.display = 'none';
        if (modeGroup) modeGroup.style.display = 'none';
      }
      setTimeout(updateLegendPositions, 50);
      updateStatsVisibility();
    });
  }

  // Road quality
  const qualityCb = document.getElementById('roadQualityCheckbox');
  if (qualityCb) {
    qualityCb.addEventListener('change', e => {
      showRoadQuality = e.target.checked;
      if (showRoadQuality && showSpeedColors) {
        showSpeedColors = false;
        document.getElementById('speedColorsCheckbox').checked = false;
        document.getElementById('speedLegend').style.display = 'none';
        document.getElementById('speedModeGroup').style.display = 'none';
      }
      const legend = document.getElementById('roadQualityLegend');
      if (showRoadQuality) {
        map.setPaintProperty('trips-layer', 'line-color', getRoadQualityColorExpression());
        if (legend) legend.style.display = 'block';
      } else {
        map.setPaintProperty('trips-layer', 'line-color', getSensorColorExpression());
        if (legend) legend.style.display = 'none';
      }
      updateLegendPositions();
      updateStatsVisibility();
    });
  }

  document.querySelectorAll('input[name="speedMode"]').forEach(r => {
    r.addEventListener('change', e => {
      speedMode = e.target.value;
      if (showSpeedColors) map.setPaintProperty('trips-layer', 'line-color', getSpeedColorExpression(speedMode));
    });
  });

  setupAveragedSegmentControls();

  const isoToggle = document.getElementById('isoToggle');
  if (isoToggle) isoToggle.addEventListener('change', e => updateIsochrone(e.target.checked));
}

function updateStatsFromMetadata() {
  document.getElementById('statTrips').textContent = tripIds.length;
  if (!tripsMetadata) return;
  const agg = calculateAggregateStats();
  if (agg) {
    document.getElementById('statTrips').textContent = tripIds.length;
    document.getElementById('statDistance').textContent = `${agg.totalDistance} km`;
    document.getElementById('statAvgSpeed').textContent = `${agg.avgSpeed} km/h`;
    document.getElementById('statTotalTime').textContent = agg.totalTime;
  }
}

function renderSensorLegend() {
  const legend = document.getElementById('sensorLegend');
  if (!legend) return;
  legend.innerHTML = `<h4>Sensors</h4>` + Object.entries(sensorColorMap).map(([s, c]) => `
    <div class="speed-legend-item sensor-legend-item" data-sensor="${s}" style="cursor:pointer;" title="Click to highlight ${s}">
      <div class="speed-color-box" style="background:${c};"></div>
      <span>${s}</span>
    </div>`).join('');

  // Add click handlers
  legend.querySelectorAll('.sensor-legend-item').forEach(item => {
    item.addEventListener('click', () => {
      const sensor = item.dataset.sensor;
      const input = document.getElementById('tripSearchInput');
      if (input) input.value = sensor;
      searchAndHighlightTrip(sensor);
    });
  });

  legend.style.display = 'block';
  updateLegendPositions();
}

window.searchTrip = searchAndHighlightTrip;
