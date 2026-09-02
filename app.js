// app.js
// Bike Sensor Data Visualization

import { CONFIG } from './config.js';
import { buildLeaderboard, renderLeaderboard } from './leaderboard.js';

console.log('🚀 Starting bike visualization...');
const ORS_API_KEY = 'eyJvcmciOiI1YjNjZTM1OTc4NTExMTAwMDFjZjYyNDgiLCJpZCI6ImZhODc1ZmQ3ODRmOTQ3MTNiNWRmMGY2NTcwYjM0YTVjIiwiaCI6Im11cm11cjY0In0=';

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
let showCrashes = false;

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

function getFirstLabelLayerId() {
  const layers = map.getStyle().layers;
  for (const layer of layers) {
    if (layer.type === 'symbol') return layer.id;
  }
  return undefined;
}

// ─── Colour expressions ───────────────────────────────────────────────────────
function getSpeedColorExpression(mode) {
  // Speed_display is a smoothed value used only for map color — it cancels
  // out GPS point-to-point noise on API-sourced trips. It equals Speed on
  // CSV/wheel-rotation trips (already a clean measurement), so this is safe
  // for every trip. Falls back to Speed/speed for any older cached features
  // that predate Speed_display.
  const v = ['to-number', ['coalesce', ['get', 'Speed_display'], ['get', 'Speed'], ['get', 'speed'], 0]];
  if (mode === 'gradient') {
    return ['interpolate', ['linear'], v, 0,'#808080', 2,'#DC2626', 5,'#F97316', 10,'#FACC15', 15,'#22C55E', 20,'#3B82F6', 25,'#bb06d7'];
  }
  return ['step', v, '#808080', 2,'#DC2626', 5,'#F97316', 10,'#FACC15', 15,'#22C55E', 20,'#3B82F6', 25,'#bb06d7'];
}

function getRoadQualityColorExpression() {
  return ['match', ['get', 'road_quality'], 1,'#22C55E', 2,'#84CC16', 3,'#FACC15', 4,'#F97316', 5,'#DC2626', '#808080'];
}

function getSensorColorExpression() {
  const fallback = DEFAULT_COLOR;
  const pairs = tripIds.flatMap(id => [id, getSensorColor(id)]);
  if (pairs.length === 0) return fallback;
  return ['match', ['get', 'trip_id'], ...pairs, fallback];
}

function getAveragedSpeedColorExpression() {
  return ['interpolate', ['linear'], ['get', 'avg_speed'], 0,'#DC2626', 5,'#F97316', 10,'#FACC15', 15,'#22C55E', 20,'#3B82F6', 25,'#bb06d7'];
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

// ─── Hotspot colour (concentration only) ─────────────────────────────────────
function getHotspotColorExpression() {
  return [
    'interpolate', ['linear'],
    ['to-number', ['get', 'count']],
    1,  '#FFF176',
    3,  '#FF9800',
    8,  '#D32F2F',
    20, '#9C27B0',
  ];
}

// ─── Braking filter helpers ───────────────────────────────────────────────────
// When a trip is selected while braking is active, filter hotspots to that trip.
// When deselected, show all hotspots again.
function applyBrakingTripFilter(tripId) {
  if (!map.getSource('braking-hotspots')) return;

  const source = map.getSource('trips');
  const allFeatures = source?._data?.features || [];

  if (tripId) {
    // Rebuild hotspots for just this trip
    const tripFeatures = allFeatures.filter(f => f.properties.trip_id === tripId);
    const filtered = buildBrakingHotspots(tripFeatures);
    map.getSource('braking-hotspots').setData(filtered);
  } else {
    // Restore all hotspots
    const all = buildBrakingHotspots(allFeatures);
    map.getSource('braking-hotspots').setData(all);
  }
}

// ─── Trip dates ───────────────────────────────────────────────────────────────
let tripDateMap = {};

function buildTripDateMap(features) {
  tripDateMap = {};
  for (const f of features) {
    const tid = f.properties?.trip_id;
    const ts  = f.properties?.timestamp;
    if (!tid || !ts || tripDateMap[tid]) continue;
    tripDateMap[tid] = ts;
  }
}

// Returns the raw date/timestamp value for a trip, or null if unknown.
// Prefers the per-feature "timestamp" property (from trips.geojson); falls
// back to the "Trip start/end" metadata field for trips that predate it.
function getTripDate(tripId) {
  if (tripDateMap[tripId]) return tripDateMap[tripId];
  const meta = tripsMetadata?.[tripId];
  const raw  = meta?.['Trip start/end'] || meta?.metadata?.['Trip start/end'];
  if (raw) {
    const start = raw.split(',').map(p => p.trim()).filter(Boolean)[0];
    if (start) return start;
  }
  return null;
}

// Returns YYYY-MM-DD for a trip, for comparison against an <input type="date"> value.
function getTripDateOnly(tripId) {
  const raw = getTripDate(tripId);
  if (!raw) return null;
  const d = new Date(raw);
  if (isNaN(d.getTime())) return null;
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
}

function formatDateDMY(rawDate) {
  const d = new Date(rawDate);
  if (isNaN(d.getTime())) return rawDate; // unparsed — show the raw string rather than hide it
  const dd   = String(d.getDate()).padStart(2, '0');
  const mm   = String(d.getMonth() + 1).padStart(2, '0');
  return `${dd}-${mm}-${d.getFullYear()}`;
}

// range: { from, to } where either side may be '' (open-ended). Both empty → null.
function formatDateRangeLabel(range) {
  if (!range || (!range.from && !range.to)) return '';
  if (range.from && range.to) return `${formatDateDMY(range.from)} to ${formatDateDMY(range.to)}`;
  if (range.from) return `from ${formatDateDMY(range.from)}`;
  return `until ${formatDateDMY(range.to)}`;
}

function tripDateInRange(tripId, range) {
  if (!range || (!range.from && !range.to)) return true;
  const d = getTripDateOnly(tripId);
  if (!d) return false;
  if (range.from && d < range.from) return false;
  if (range.to   && d > range.to)   return false;
  return true;
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

function formatDuration(s) {
  const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60);
  return h > 0 ? `${h}h ${m}m` : `${m}m`;
}

function updateResetButtonVisibility() {
  const active = showSpeedColors || showRoadQuality || showAveragedSegments || showBraking || showCrashes || searchActive || !!selectedTrip;
  document.getElementById('resetButton').style.display = active ? 'block' : 'none';
}

function currentColorExpression() {
  if (showSpeedColors) return getSpeedColorExpression(speedMode);
  if (showRoadQuality) return getRoadQualityColorExpression();
  return getSensorColorExpression();
}

function getSelectedTripIds() {
  if (!activeFilter) return null;
  return Array.isArray(activeFilter) ? activeFilter : [activeFilter];
}

function applyTripFilter(filterTripId) {
  activeFilter = filterTripId;
  if (!map.getLayer('trips-layer')) return;

  if (filterTripId) {
    let highlightColor;
    if (showSpeedColors) {
      highlightColor = getSpeedColorExpression(speedMode);
    } else if (showRoadQuality) {
      highlightColor = getRoadQualityColorExpression();
    } else {
      highlightColor = '#FF69B4';
    }
    map.setPaintProperty('trips-layer', 'line-color', [
      'case',
      ['==', ['get', 'trip_id'], filterTripId],
      highlightColor,
      'rgba(0,0,0,0)'
    ]);
    map.setPaintProperty('trips-layer', 'line-opacity', 1);
    map.setPaintProperty('trips-layer', 'line-width', [
      'case', ['==', ['get', 'trip_id'], filterTripId], 4, 0
    ]);
  } else {
    map.setPaintProperty('trips-layer', 'line-color', currentColorExpression());
    map.setPaintProperty('trips-layer', 'line-opacity', 0.7);
    map.setPaintProperty('trips-layer', 'line-width', 3);
  }

  // If braking is active, sync hotspot filter to the selected trip
  if (showBraking) applyBrakingTripFilter(filterTripId);
}

function applyGroupFilter(matchingIds) {
  activeFilter = matchingIds;
  if (!map.getLayer('trips-layer')) return;
  const set = new Set(matchingIds);

  let highlightColor;
  if (showSpeedColors) {
    highlightColor = getSpeedColorExpression(speedMode);
  } else if (showRoadQuality) {
    highlightColor = getRoadQualityColorExpression();
  } else {
    highlightColor = '#FF69B4';
  }

  map.setPaintProperty('trips-layer', 'line-color', [
    'case',
    ['in', ['get', 'trip_id'], ['literal', [...set]]],
    highlightColor,
    'rgba(0,0,0,0)'
  ]);
  map.setPaintProperty('trips-layer', 'line-opacity', 1);
  map.setPaintProperty('trips-layer', 'line-width', [
    'case', ['in', ['get', 'trip_id'], ['literal', [...set]]], 4, 0
  ]);
}

// ─── Selection / search ───────────────────────────────────────────────────────
function resetSelection() {
  selectedTrip         = null;
  activeFilter         = null;
  searchActive         = false;
  showSpeedColors      = false;
  showRoadQuality      = false;
  showAveragedSegments = false;
  showBraking          = false;

  if (currentPopup) { currentPopup.remove(); currentPopup = null; }
  applyTripFilter(null);

  ['speedColorsCheckbox','roadQualityCheckbox','averagedSegmentsCheckbox','brakingCheckbox','crashCheckbox'].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.checked = false;
  });

  document.getElementById('speedLegend').style.display            = 'none';
  document.getElementById('speedModeGroup').style.display         = 'none';
  document.getElementById('roadQualityLegend').style.display      = 'none';
  document.getElementById('averagedSegmentsLegend').style.display = 'none';
  document.getElementById('averagedModeGroup').style.display      = 'none';
  const brakingLegend = document.getElementById('brakingLegend');
  if (brakingLegend) brakingLegend.style.display = 'none';
  const crashLegend = document.getElementById('crashLegend');
  if (crashLegend) crashLegend.style.display = 'none';

  if (map.getLayer('averaged-segments'))
    map.setLayoutProperty('averaged-segments', 'visibility', 'none');
  if (map.getLayer('braking-hotspots-halo'))
    map.setLayoutProperty('braking-hotspots-halo', 'visibility', 'none');
  if (map.getLayer('braking-hotspots-dot'))
    map.setLayoutProperty('braking-hotspots-dot', 'visibility', 'none');
  if (map.getLayer('crash-events-halo'))
    map.setLayoutProperty('crash-events-halo', 'visibility', 'none');
  if (map.getLayer('crash-events-dot'))
    map.setLayoutProperty('crash-events-dot', 'visibility', 'none');

  if (map.getLayer('trips-layer')) {
    map.setLayoutProperty('trips-layer', 'visibility', 'visible');
    map.setPaintProperty('trips-layer', 'line-color', getSensorColorExpression());
    map.setPaintProperty('trips-layer', 'line-opacity', 0.7);
    map.setPaintProperty('trips-layer', 'line-width', 3);
  }

  const searchInput  = document.getElementById('tripSearchInput');
  const dateFromInput = document.getElementById('tripDateFrom');
  const dateToInput   = document.getElementById('tripDateTo');
  const clearBtn     = document.getElementById('tripClearButton');
  if (searchInput)   searchInput.value   = '';
  if (dateFromInput) dateFromInput.value = '';
  if (dateToInput)   dateToInput.value   = '';
  if (clearBtn)       clearBtn.style.display = 'none';

  document.getElementById('selectedTripRow').style.display  = 'none';
  document.getElementById('statTripRow').style.display      = 'flex';
  document.getElementById('statDistanceRow').style.display  = 'flex';
  document.getElementById('statAvgSpeedRow').style.display  = 'flex';
  document.getElementById('statTotalTimeRow').style.display = 'flex';

  updateResetButtonVisibility();
  setTimeout(updateLegendPositions, 50);
  updateStatsVisibility();
}

function clearSearch() {
  searchActive = false;
  selectedTrip = null;
  activeFilter = null;
  const input        = document.getElementById('tripSearchInput');
  const dateFromInput = document.getElementById('tripDateFrom');
  const dateToInput   = document.getElementById('tripDateTo');
  const clearBtn      = document.getElementById('tripClearButton');
  if (input)          input.value          = '';
  if (dateFromInput)  dateFromInput.value  = '';
  if (dateToInput)    dateToInput.value    = '';
  if (clearBtn)        clearBtn.style.display = 'none';
  if (currentPopup) { currentPopup.remove(); currentPopup = null; }
  applyTripFilter(null);

  // Restore all braking hotspots when deselecting
  if (showBraking) applyBrakingTripFilter(null);

  document.getElementById('selectedTripRow').style.display  = 'none';
  document.getElementById('statTripRow').style.display      = 'flex';
  document.getElementById('statDistanceRow').style.display  = 'flex';
  document.getElementById('statAvgSpeedRow').style.display  = 'flex';
  document.getElementById('statTotalTimeRow').style.display = 'flex';

  updateResetButtonVisibility();
}

function showSelection(tripId) {
  document.getElementById('statTripRow').style.display      = 'none';
  document.getElementById('statDistanceRow').style.display  = 'none';
  document.getElementById('statAvgSpeedRow').style.display  = 'none';
  document.getElementById('statTotalTimeRow').style.display = 'none';
  document.getElementById('selectedTripRow').style.display  = 'flex';
  const name = tripId.replace(/_/g, ' ').replace(/processed/gi, '').replace(/clean/gi, '').trim();
  document.getElementById('selectedTrip').textContent = name;
  updateResetButtonVisibility();
}

function searchAndHighlightTrip(term, dateFilter) {
  const q = (term || '').toLowerCase().trim();
  const hasDateFilter = !!(dateFilter && (dateFilter.from || dateFilter.to));
  if (!q && !hasDateFilter) { resetSelection(); return; }

  let matches = tripIds;
  if (q)            matches = matches.filter(id => id.toLowerCase().includes(q));
  if (hasDateFilter) matches = matches.filter(id => tripDateInRange(id, dateFilter));

  const label = [term && term.trim(), hasDateFilter && formatDateRangeLabel(dateFilter)].filter(Boolean).join(' on ');

  if (matches.length === 0) {
    alert(`No trip found matching: ${label}`);
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
    document.getElementById('statTripRow').style.display      = 'none';
    document.getElementById('statDistanceRow').style.display  = 'none';
    document.getElementById('statAvgSpeedRow').style.display  = 'none';
    document.getElementById('statTotalTimeRow').style.display = 'none';
    document.getElementById('selectedTripRow').style.display  = 'flex';
    document.getElementById('selectedTrip').textContent = `${label.toUpperCase()} — ${matches.length} trips`;
    updateResetButtonVisibility();
  }

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
  const exprs = {
    speed:     getAveragedSpeedColorExpression(),
    quality:   getAveragedQualityColorExpression(),
    composite: getCompositeScoreColorExpression(),
  };
  map.setPaintProperty('averaged-segments', 'circle-color', exprs[averagedSegmentMode]);
}

async function setupAveragedSegments(labelLayerId) {
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
      'circle-color':           getCompositeScoreColorExpression(),
      'circle-radius':          ['interpolate', ['linear'], ['zoom'], 10, 18, 13, 28, 16, 45],
      'circle-blur':            1.2,
      'circle-opacity':         0.6,
      'circle-pitch-alignment': 'map',
    }
  }, labelLayerId);

  map.on('click', 'averaged-segments', (e) => {
    e.preventDefault();
    if (e.originalEvent) e.originalEvent.stopPropagation();
    const p = e.features[0].properties;
    const qualityText = p.avg_quality
      ? `🛣️ Avg Quality: ${p.avg_quality} (${getQualityLabel(p.avg_quality)})`
      : '🛣️ Quality: No data';
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

// ─── Braking hotspot accumulation ────────────────────────────────────────────
function buildBrakingHotspots(features) {
  const CELL_SIZE = 0.0002;
  const grid = new Map();

  for (const f of features) {
    if (!f.properties.is_braking) continue;

    const coords = f.geometry.coordinates;
    const mid    = coords[Math.floor(coords.length / 2)] || coords[0];
    const [lng, lat] = mid;

    const cellLng = Math.round(lng / CELL_SIZE) * CELL_SIZE;
    const cellLat = Math.round(lat / CELL_SIZE) * CELL_SIZE;
    const key     = `${cellLng.toFixed(4)},${cellLat.toFixed(4)}`;

    if (!grid.has(key)) {
      grid.set(key, {
        lng: cellLng, lat: cellLat,
        count: 0, totalIntensity: 0, maxIntensity: 0,
        trips: new Set(),
      });
    }

    const cell = grid.get(key);
    cell.count++;
    cell.totalIntensity += f.properties.braking_intensity || 0;
    cell.maxIntensity    = Math.max(cell.maxIntensity, f.properties.braking_intensity || 0);
    cell.trips.add(f.properties.trip_id);
  }

  const hotspotFeatures = [...grid.values()].map(cell => ({
    type: 'Feature',
    geometry: { type: 'Point', coordinates: [cell.lng, cell.lat] },
    properties: {
      count:         cell.count,
      avg_intensity: parseFloat((cell.totalIntensity / cell.count).toFixed(1)),
      max_intensity: parseFloat(cell.maxIntensity.toFixed(1)),
      trip_count:    cell.trips.size,
    },
  }));

  console.log(`🛑 Built ${hotspotFeatures.length} braking hotspot cells`);
  return { type: 'FeatureCollection', features: hotspotFeatures };
}

function setupBrakingLayer(geojson, labelLayerId) {
  const hotspotData = buildBrakingHotspots(geojson.features || []);

  map.addSource('braking-hotspots', { type: 'geojson', data: hotspotData });

  // Outer glow
  map.addLayer({
    id: 'braking-hotspots-halo',
    type: 'circle',
    source: 'braking-hotspots',
    layout: { visibility: 'none' },
    paint: {
      'circle-color': getHotspotColorExpression(),
      'circle-radius': [
        'interpolate', ['linear'], ['zoom'],
        10, ['interpolate', ['linear'], ['get', 'count'],  1, 12,  5, 20, 15, 34],
        14, ['interpolate', ['linear'], ['get', 'count'],  1, 20,  5, 32, 15, 52],
        17, ['interpolate', ['linear'], ['get', 'count'],  1, 30,  5, 50, 15, 75],
      ],
      'circle-blur':            1.2,
      'circle-opacity':         0.35,
      'circle-pitch-alignment': 'map',
    },
  }, labelLayerId);

  // Solid dot — no stroke
  map.addLayer({
    id: 'braking-hotspots-dot',
    type: 'circle',
    source: 'braking-hotspots',
    layout: { visibility: 'none' },
    paint: {
      'circle-color': getHotspotColorExpression(),
      'circle-radius': [
        'interpolate', ['linear'], ['zoom'],
        10, ['interpolate', ['linear'], ['get', 'count'],  1,  4,  5,  8, 15, 14],
        14, ['interpolate', ['linear'], ['get', 'count'],  1,  7,  5, 13, 15, 20],
        17, ['interpolate', ['linear'], ['get', 'count'],  1, 11,  5, 18, 15, 28],
      ],
      'circle-opacity':         0.85,
      'circle-pitch-alignment': 'map',
    },
  }, labelLayerId);

  map.on('click', 'braking-hotspots-dot', (e) => {
    e.preventDefault();
    if (e.originalEvent) e.originalEvent.stopPropagation();
    const p = e.features[0].properties;
    let severity = 'Low';
    if (p.avg_intensity >= 15) severity = 'Emergency';
    else if (p.avg_intensity >= 5)   severity = 'Hard';
    else if (p.avg_intensity >= 2.5) severity = 'Firm';
    new mapboxgl.Popup()
      .setLngLat(e.lngLat)
      .setHTML(`
        <strong>🔴 Braking Hotspot</strong><br>
        📍 Events here: <strong>${p.count}</strong><br>
        ⚡ Avg deceleration: ${p.avg_intensity} km/h/s<br>
        🏎️ Peak deceleration: ${p.max_intensity} km/h/s<br>
        🚲 Across ${p.trip_count} trip(s)
      `)
      .addTo(map);
  });

  map.on('mouseenter', 'braking-hotspots-dot', () => { map.getCanvas().style.cursor = 'pointer'; });
  map.on('mouseleave', 'braking-hotspots-dot', () => { map.getCanvas().style.cursor = ''; });

  console.log('✅ Braking hotspot layer added');
}

// ─── Crash / fall events ────────────────────────────────────────────────────
// Severity (colour), type (glyph artwork), and outcome (white ring) are
// each baked directly into the marker's icon — see CRASH_SEVERITY_COLORS,
// CRASH_GLYPHS, and drawCrashIcon() below.

function getCrashTypeLabel(properties) {
  if (properties.crash_type) return properties.crash_type;

  const speed = Number(
    properties.preimpact_speed_kmh ??
    properties.speed_at_impact_kmh
  );

  if (!Number.isFinite(speed)) return 'Unclassified';
  if (speed <= 1) return 'Stationary Fall';
  if (speed <= 10) return 'Low-Speed Fall';
  return 'High-Speed Fall';
}

function getCrashOutcomeLabel(properties) {
  if (properties.crash_outcome) return properties.crash_outcome;

  if (properties.unresolved === true || properties.unresolved === 'true') {
    return 'Unresolved';
  }

  if (properties.came_to_stop === true || properties.came_to_stop === 'true') {
    return 'Resolved';
  }

  return 'Unclassified';
}

function buildCrashFeatures(features) {
  const crashFeatures = (features || [])
    .filter(f =>
      f?.geometry?.type === 'Point' &&
      f?.properties?.event_type === 'crash'
    )
    .map(f => {
      const crash_type = getCrashTypeLabel(f.properties || {});
      const crash_outcome = getCrashOutcomeLabel(f.properties || {});
      const severity = f.properties?.severity || 'Minor';
      return {
        ...f,
        properties: {
          ...f.properties,
          // Compute these once, here, so the icon, and the popup all read
          // the same classification — previously they read
          // crash_type/crash_outcome straight off the raw feature, which
          // only worked when the source data already had those exact
          // fields; otherwise every marker silently fell back to
          // "Unclassified" regardless of its actual speed/outcome data.
          crash_type,
          crash_outcome,
          // The icon id that bakes in glyph (crash_type) + colour
          // (severity) + ring (crash_outcome) — see drawCrashIcon /
          // ensureCrashIcons above. Computed here so the source data and
          // ensureCrashIcons() always agree on which icon id a feature
          // needs.
          crash_icon_id: crashIconId(
            crash_type,
            severity,
            crash_outcome === 'Unresolved'
          )
        }
      };
    });
  console.log(`🚨 Found ${crashFeatures.length} crash marker(s)`);
  if (crashFeatures.length > 0) {
    console.log('🚨 Crash markers:', crashFeatures);
  }
  return { type: 'FeatureCollection', features: crashFeatures };
}

// Crash marker icons, drawn on canvas rather than rendered as map-font
// text/circle layers. The vector basemap's font glyphs (Open Sans /
// Montserrat / Noto Sans) only ship ordinary Latin ranges — Geometric
// Shapes characters (◆ ▲ ● ■) fall outside that range and silently fail to
// render from a `text-field`. Drawing them as small raster icons via
// map.addImage() sidesteps font glyph coverage entirely, and also lets each
// icon be the actual severity colour (rather than a black glyph sitting on
// top of a separately-coloured circle).
//
// Each icon bakes in two of the three classification dimensions at once:
//   - glyph  = crash_type   (the actual artwork below, per event type)
//   - colour = severity     (Minor/Hard/Severe — same hex codes as the
//                             legend)
// Outcome (resolved/unresolved) is the third dimension: unresolved crashes
// get a white ring baked into the same icon; resolved ones don't.
//
// Glyph artwork is the path data from the provided icon set (Hard_Crash /
// Severe_Crash / Tipping_Over / Unknown_Crash — same 4 glyphs also shipped
// as the "Simple_-_..." PNGs, just under crash-type names there instead of
// severity names), traced out of a 256×256 viewBox and redrawn here as
// Path2D so the fill colour can be swapped per severity at draw time,
// rather than using the fixed colour baked into the source files.
const CRASH_GLYPHS = {
  // Arrow swoosh
  'Stationary Fall': {
    type: 'path',
    d: 'M43.3,141.6s19.3-35.5,53.1-44.3c33.8-8.9,70.4,14,70.4,14l5.5-18.8c.1-.5.5-.8,1-1,.8-.3,1.6.1,1.9.9l18.3,53.7c0,.1,0,.2,0,.3,0,.8-.5,1.6-1.3,1.6l-53.3,5c-.4,0-.9-.1-1.2-.4-.6-.6-.6-1.5,0-2.1l15.3-16s-30.5-23.2-59.1-17.9c-28.6,5.3-50.6,24.9-50.6,24.9Z'
  },
  // Wifi-style signal arcs
  'Low-Speed Fall': {
    type: 'path',
    d: 'M187.9,145.5c-17-38.7-62.1-60.5-103-49.8-1.5.4-2.3,2-1.7,3.4l3.1,7.8c.6,1.4,2.2,2.2,3.6,1.8,33.6-8.6,70.5,9.2,84.6,40.9.6,1.4,2.2,2.1,3.7,1.7l8.1-2.4c1.5-.5,2.3-2,1.6-3.5h0ZM161.3,153.8c-11.4-24.5-40-38.3-66.3-32.1-1.6.4-2.4,2-1.8,3.4l3.1,7.9c.5,1.4,2.1,2.2,3.5,1.9,19-4.3,39.6,5.7,48,23.2.6,1.3,2.2,2,3.7,1.6l8.1-2.4c1.5-.5,2.3-2.1,1.6-3.5Z'
  },
  // Zigzag impact spike — drawn stroked (source is a polyline), not filled
  'High-Speed Fall': {
    type: 'polyline',
    points: [[68.5,167.8],[50.8,139.7],[79.9,139.7],[79.9,91.7],[123.1,138.1],[142.7,76.4],[167.5,127.2],[209.1,106.8],[209.1,141.8]],
    strokeWidth: 19.8
  },
  // Triangle with a question mark
  'Unclassified': {
    type: 'path',
    d: 'M184.2,161.2l-51-100.3c-2.8-5.6-7.5-5.6-10.3,0l-51,100.3c-2.8,5.6,0,10.1,6.4,10.1h99.6c6.3,0,9.2-4.5,6.4-10.1ZM126.3,163.1c-4.9,0-8.2-3.5-8.2-8.3,0-4.9,3.4-8.4,8.2-8.4,4.9,0,8.1,3.4,8.2,8.4,0,4.8-3.2,8.3-8.2,8.3ZM137.4,129c-3.3,3.7-4.7,7.3-4.7,11.4v1.6s-12.2,0-12.2,0v-2.3c-.4-4.7,1.2-9.5,5.3-14.4,3-3.5,5.3-6.5,5.3-9.6s-2.1-5.4-6.7-5.5c-3.1,0-6.7,1-9.1,2.7l-3.1-10c3.3-2,8.8-3.8,15.3-3.8,12.1,0,17.6,6.7,17.6,14.3s-4.4,11.6-7.9,15.4Z'
  }
};
const CRASH_GLYPH_VIEWBOX = 256; // all path/point data above is in this coordinate space

const CRASH_SEVERITY_COLORS = {
  'Minor':  '#ffea00',
  'Hard':   '#ff9100',
  'Severe': '#ff1744'
};

function crashColorForSeverity(severity) {
  return CRASH_SEVERITY_COLORS[severity] || CRASH_SEVERITY_COLORS.Minor;
}

// Small inline SVG version of a crash-type glyph, for the legend (which is
// plain HTML/CSS, not the map canvas) — reuses the same path/polyline data
// as drawCrashIcon() so the legend and the map markers always agree on
// what each glyph looks like. Neutral colour since the legend's
// Classification row is shape-only; severity colour is a separate row.
function crashGlyphSvg(crashType, color = '#cfcfcf') {
  const glyph = CRASH_GLYPHS[crashType] || CRASH_GLYPHS['Unclassified'];
  const inner = glyph.type === 'polyline'
    ? `<polyline points="${glyph.points.map(p => p.join(',')).join(' ')}" fill="none" stroke="${color}" stroke-width="${glyph.strokeWidth}" stroke-linejoin="round" stroke-linecap="round"/>`
    : `<path d="${glyph.d}" fill="${color}"/>`;
  return `<svg viewBox="0 0 ${CRASH_GLYPH_VIEWBOX} ${CRASH_GLYPH_VIEWBOX}" width="16" height="16">${inner}</svg>`;
}

// Icon id encodes all three baked-in dimensions so distinct combinations
// each get their own registered image.
function crashIconId(crashType, severity, unresolved) {
  const safeType = (crashType || 'Unclassified').replace(/[^a-zA-Z0-9]+/g, '_');
  return `crash-icon-${safeType}-${severity}-${unresolved ? 'ring' : 'plain'}`;
}

// Renders one crash-type glyph as a round badge: dark disc, a severity-
// coloured ring, the glyph artwork in that same colour, and — for
// unresolved crashes — an extra white ring outside it. size is the
// icon's logical (CSS-pixel) diameter; the canvas itself is rendered at
// size*devicePixelRatio for crispness, matching the pixelRatio passed to
// map.addImage().
function drawCrashIcon(crashType, color, unresolved, size = 40) {
  const glyph = CRASH_GLYPHS[crashType] || CRASH_GLYPHS['Unclassified'];
  const canvas = document.createElement('canvas');
  const dpr = window.devicePixelRatio || 1;
  canvas.width = size * dpr;
  canvas.height = size * dpr;
  const ctx = canvas.getContext('2d');

  // 1 drawing unit = 1 unit in the glyphs' original 256×256 viewBox.
  const scale = (size * dpr) / CRASH_GLYPH_VIEWBOX;
  ctx.scale(scale, scale);

  const cx = CRASH_GLYPH_VIEWBOX / 2;
  const cy = CRASH_GLYPH_VIEWBOX / 2;
  const discRadius = 118;

  // Dark disc background, matching the badge look of the source icon set.
  ctx.beginPath();
  ctx.arc(cx, cy, discRadius, 0, Math.PI * 2);
  ctx.fillStyle = '#3a3838';
  ctx.fill();

  // Outcome ring (unresolved only) sits outside the severity ring.
  if (unresolved) {
    ctx.beginPath();
    ctx.arc(cx, cy, discRadius + 6, 0, Math.PI * 2);
    ctx.lineWidth = 8;
    ctx.strokeStyle = '#ffffff';
    ctx.stroke();
  }

  // Severity-coloured ring, same colour as the glyph itself.
  ctx.beginPath();
  ctx.arc(cx, cy, discRadius - 5, 0, Math.PI * 2);
  ctx.lineWidth = 10;
  ctx.strokeStyle = color;
  ctx.stroke();

  // The glyph itself.
  ctx.fillStyle = color;
  ctx.strokeStyle = color;
  if (glyph.type === 'polyline') {
    ctx.beginPath();
    glyph.points.forEach(([x, y], i) => {
      if (i === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    });
    ctx.lineWidth = glyph.strokeWidth;
    ctx.lineJoin = 'round';
    ctx.lineCap = 'round';
    ctx.stroke();
  } else {
    ctx.fill(new Path2D(glyph.d));
  }

  return { width: canvas.width, height: canvas.height, data: ctx.getImageData(0, 0, canvas.width, canvas.height).data };
}

// Registers exactly the icon combinations present in this trip's crash
// data (at most 4 types × 3 severities × 2 ring states = 24, usually far
// fewer) — cheap, and map.hasImage() skips anything already registered
// from a previous trip.
function ensureCrashIcons(crashFeatures) {
  const seen = new Set();
  (crashFeatures || []).forEach(f => {
    const p = f.properties || {};
    const id = p.crash_icon_id;
    if (!id || seen.has(id) || map.hasImage(id)) return;
    seen.add(id);
    const severity = p.severity || 'Minor';
    const unresolved = p.crash_outcome === 'Unresolved';
    map.addImage(id, drawCrashIcon(p.crash_type, crashColorForSeverity(severity), unresolved), { pixelRatio: window.devicePixelRatio || 1 });
  });
}

function setupCrashLayer(geojson, labelLayerId) {
  const crashData = buildCrashFeatures(geojson.features || []);

  ensureCrashIcons(crashData.features);

  map.addSource('crash-events', {
    type: 'geojson',
    data: crashData
  });

  // ─────────────────────────────────────────────────────────────────────────
  // TAP-TARGET CIRCLE (id kept as 'crash-events-halo' for compatibility with
  // existing visibility toggles / click handlers elsewhere in the file)
  //
  // Fully invisible — the visible marker is now the colour+shape icon in
  // 'crash-events-dot' below. This layer exists purely to give crash
  // markers a comfortably large hit target, since the icon alone can be a
  // small tap target at low zoom.
  // ─────────────────────────────────────────────────────────────────────────
  map.addLayer({
    id: 'crash-events-halo',
    type: 'circle',
    source: 'crash-events',
    layout: {
      visibility: 'none'
    },
    paint: {
      'circle-color': '#000000',
      'circle-opacity': 0,

      'circle-radius': [
        'interpolate',
        ['linear'],
        ['zoom'],
        10, 9,
        14, 13,
        17, 18
      ],

      'circle-pitch-alignment': 'map'
    }
  }, labelLayerId);


  // ─────────────────────────────────────────────────────────────────────────
  // CRASH MARKER
  //
  // Each icon is a round badge: severity (colour), type (glyph artwork),
  // and outcome (white ring) are all baked into a single raster icon by
  // drawCrashIcon() and registered by ensureCrashIcons(); 'crash_icon_id'
  // (computed once in buildCrashFeatures) picks the right one per feature.
  // Rendered as canvas-drawn icons, not map-font text glyphs — the
  // basemap's fonts don't cover custom glyph shapes, so a text-field
  // version of this would silently fail to draw.
  //
  //   Stationary Fall → arrow swoosh
  //   Low-Speed Fall  → wifi-style signal arcs
  //   High-Speed Fall → zigzag impact spike
  //   Unclassified    → triangle with a question mark
  //   colour: Minor #ffea00 / Hard #ff9100 / Severe #ff1744
  //   white ring: unresolved crashes only
  // ─────────────────────────────────────────────────────────────────────────
  map.addLayer({
    id: 'crash-events-dot',
    type: 'symbol',
    source: 'crash-events',
    layout: {
      visibility: 'none',

      'icon-image': ['get', 'crash_icon_id'],

      // Icons are rendered at a 40px logical diameter (see drawCrashIcon),
      // so icon-size is a multiplier of that base.
      'icon-size': [
        'interpolate',
        ['linear'],
        ['zoom'],
        10, 0.48,
        14, 0.68,
        17, 0.95
      ],

      'icon-allow-overlap': true,
      'icon-ignore-placement': true
    },

    paint: {
      'icon-opacity': 1
    }
  }, labelLayerId);


  // ─────────────────────────────────────────────────────────────────────────
  // POPUP
  //
  // Bound to BOTH the circle ('crash-events-halo') and the glyph on top of
  // it ('crash-events-dot'), since the glyph alone is a tiny hit target —
  // most clicks on a bubble land on the circle, not the small symbol text.
  // ─────────────────────────────────────────────────────────────────────────
  function showCrashPopup(e) {
    e.preventDefault();
    if (e.originalEvent) {
      e.originalEvent.stopPropagation();
    }

    if (currentPopup) { currentPopup.remove(); }

    const p = e.features[0].properties;

    const speedLine = p.speed_at_impact_kmh != null
      ? `🚴 Speed at impact: ${p.speed_at_impact_kmh} km/h`
      : `🚴 Speed at impact: unknown`;

    const crashType = getCrashTypeLabel(p);
    const crashOutcome = getCrashOutcomeLabel(p);

    const recoveryLine =
      p.unresolved === true || p.unresolved === 'true'
        ? `⚠️ <strong>Wheel never turned again</strong>`
        : p.came_to_stop === true || p.came_to_stop === 'true'
          ? `🧍 Came to a stop, moving again after ${p.recovery_time_s}s`
          : `↪️ Kept moving — no stop detected nearby`;

    currentPopup = new mapboxgl.Popup()
      .setLngLat(e.lngLat)
      .setHTML(`
        <strong>🚨 ${p.severity} Impact</strong><br>
        💥 Peak force: ${p.peak_g}g<br>
        ⚡ Onset: ${p.suddenness_s}s to peak<br>
        🚲 Classification: ${crashType}<br>
        📍 Outcome: ${crashOutcome}<br>
        ${speedLine}<br>
        ${recoveryLine}<br>
        🕐 ${p.time_str || 'time unknown'} · trip ${p.trip_id}
      `)
      .addTo(map);
  }

  map.on('click', 'crash-events-halo', showCrashPopup);
  map.on('click', 'crash-events-dot',  showCrashPopup);

  map.on('mouseenter', 'crash-events-halo', () => { map.getCanvas().style.cursor = 'pointer'; });
  map.on('mouseleave', 'crash-events-halo', () => { map.getCanvas().style.cursor = ''; });
  map.on('mouseenter', 'crash-events-dot',  () => { map.getCanvas().style.cursor = 'pointer'; });
  map.on('mouseleave', 'crash-events-dot',  () => { map.getCanvas().style.cursor = ''; });

  console.log('✅ Crash/fall layer added');
}


function setupCrashControls() {
  const cb = document.getElementById('crashCheckbox');
  if (!cb) return;

  cb.addEventListener('change', (e) => {
    showCrashes = e.target.checked;

    const legend = document.getElementById('crashLegend');
    const visibility = showCrashes ? 'visible' : 'none';

    if (map.getLayer('crash-events-halo')) {
      map.setLayoutProperty(
        'crash-events-halo',
        'visibility',
        visibility
      );
    }

    if (map.getLayer('crash-events-dot')) {
      map.setLayoutProperty(
        'crash-events-dot',
        'visibility',
        visibility
      );
    }

    if (legend) {
      legend.style.display = showCrashes ? 'block' : 'none';
    }

    updateResetButtonVisibility();
    setTimeout(updateLegendPositions, 50);
    updateStatsVisibility();
  });
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

  const labelLayerId = getFirstLabelLayerId();
  console.log(`📌 Inserting layers before basemap layer: "${labelLayerId}"`);

  try {
    const geojson = await loadTripsGeoJSON();

    const sensors = buildLeaderboard(geojson.features);
    renderLeaderboard(sensors);

    tripIds = [...new Set((geojson.features || []).map(f => f.properties.trip_id).filter(Boolean))].sort();
    console.log(`📊 ${tripIds.length} unique trips loaded`);

    buildSensorColorMap(tripIds);
    buildTripDateMap(geojson.features || []);

    map.addSource('trips', {
      type: 'geojson',
      data: geojson,
      attribution: 'Bike sensor data',
    });

    map.addLayer({
      id: 'trips-layer',
      type: 'line',
      source: 'trips',
      layout: {
        'line-cap':  'round',
        'line-join': 'round',
      },
      paint: {
        'line-color':   getSensorColorExpression(),
        'line-width':   3,
        'line-opacity': 0.7,
      }
    }, labelLayerId);

    map.on('click', 'trips-layer', async (e) => {
      // A crash bubble sits directly on top of its trip's line, and Mapbox
      // fires each layer's click handler independently — so a click on a
      // bubble can also match the route underneath it. When the crash
      // layer is on, let the crash marker's own handler own that click
      // (crash info) instead of showing route info here, and — just as
      // importantly — don't set selectedTrip for it, so clicking off the
      // popup afterwards doesn't reset the crash layer off along with it.
      if (showCrashes) {
        const crashHit = map.queryRenderedFeatures(e.point, {
          layers: ['crash-events-halo', 'crash-events-dot']
        });
        if (crashHit.length > 0) return;
      }

      e.preventDefault();
      if (e.originalEvent) e.originalEvent.stopPropagation();
      if (currentPopup) { currentPopup.remove(); }

      const props       = e.features[0].properties;
      const tripId      = props.trip_id;
      const speed       = parseFloat(props.Speed || props.speed || 0);
      const roadQuality = parseInt(props.road_quality || 0);

      selectedTrip = tripId;
      applyTripFilter(tripId); // also filters braking hotspots if showBraking
      showSelection(tripId);

      const stats      = getTripStats(tripId);
      const allFeats   = map.getSource('trips')?._data?.features || [];
      const tripFeats  = allFeats.filter(f => f.properties.trip_id === tripId);
      const geoDistKm  = (tripFeats.reduce((s, f) => s + (f.properties.gps_distance_m || 0), 0) / 1000).toFixed(2);
      const geoTime    = tripFeats.reduce((s, f) => s + (f.properties.time_diff_s || 0), 0);
      const geoSpeeds  = tripFeats.map(f => f.properties.Speed || 0).filter(s => s > 0);
      const geoAvgSpd  = geoSpeeds.length ? (geoSpeeds.reduce((a, b) => a + b, 0) / geoSpeeds.length).toFixed(1) : '—';
      const geoMaxSpd  = geoSpeeds.length ? Math.max(...geoSpeeds).toFixed(1) : '—';
      const geoBraking = tripFeats.filter(f => f.properties.is_braking).length;

      const distanceKm = stats ? stats.distance.toFixed(2) : geoDistKm;
      const avgSpeed   = stats ? stats.avgSpeed.toFixed(1)  : geoAvgSpd;
      const maxSpeed   = stats ? stats.maxSpeed.toFixed(1)  : geoMaxSpd;
      const duration   = stats ? stats.duration             : formatDuration(Math.round(geoTime));

      const qualityLabels = { 0:'Unknown', 1:'Perfect', 2:'Normal', 3:'Outdated', 4:'Bad', 5:'No road' };
      const popupName  = tripId.replace(/_/g, ' ').trim();
      const brakingLine = geoBraking > 0 ? `<br>🛑 Braking events: ${geoBraking}` : '';
      const rideDate    = getTripDate(tripId);
      const dateLine    = rideDate ? `<br>📅 Date: ${formatDateDMY(rideDate)}` : '';

      currentPopup = new mapboxgl.Popup()
        .setLngLat(e.lngLat)
        .setHTML(`
          <strong>${popupName}</strong><br>
          🚴 Speed at point: ${speed} km/h<br>
          🛣️ Road quality: ${roadQuality} (${qualityLabels[roadQuality] || 'Unknown'})<br>
          📊 Average speed: ${avgSpeed} km/h<br>
          🏁 Max speed: ${maxSpeed} km/h<br>
          📍 Total distance: ${distanceKm} km<br>
          ⏱️ Duration: ${duration}${brakingLine}${dateLine}
        `)
        .addTo(map);
    });

    map.on('mouseenter', 'trips-layer', () => { map.getCanvas().style.cursor = 'pointer'; });
    map.on('mouseleave', 'trips-layer', () => { map.getCanvas().style.cursor = ''; });

    map.on('click', e => {
      if (!e.defaultPrevented) {
        if (searchActive) clearSearch();
        else if (selectedTrip) {
          // If braking is active, deselecting restores all hotspots
          if (showBraking) applyBrakingTripFilter(null);
          resetSelection();
        }
      }
    });

    setupBrakingLayer(geojson, labelLayerId);
    setupCrashLayer(geojson, labelLayerId);
    await setupAveragedSegments(labelLayerId);

    setupControls();
    updateStatsFromMetadata();
    renderSensorLegend();
    renderCrashLegend();
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
  map.addLayer({
    id: 'marineterrein-outline-layer', type: 'line', source: 'marineterrein-outline',
    layout: { 'line-join':'round', 'line-cap':'round' },
    paint: { 'line-color':'#ffffff', 'line-width':2, 'line-dasharray':[3,2], 'line-opacity':0.8 }
  }, labelLayerId);
});

// ─── UI helpers ───────────────────────────────────────────────────────────────
function isFilteredMode() { return showSpeedColors || showRoadQuality || showAveragedSegments || showBraking || showCrashes || searchActive; }

function updateStatsVisibility() {
  const statsEl = document.getElementById('stats');
  if (statsEl) statsEl.style.display = (window.innerWidth <= 768 && isFilteredMode()) ? 'none' : 'block';
  const sensorLegend = document.getElementById('sensorLegend');
  if (sensorLegend) sensorLegend.style.display = isFilteredMode() ? 'none' : 'block';
}

window.addEventListener('resize', updateStatsVisibility);

function updateLegendPositions() {
  // Sensors is pinned first (closest to the true screen edge) so it never
  // drifts depending on which other legends happen to be open — everything
  // else stacks relative to it, not the other way around.
  const order         = ['averagedSegmentsLegend','speedLegend','roadQualityLegend','brakingLegend','crashLegend'];
  const sensorLegend  = document.getElementById('sensorLegend');
  const crashLegend   = document.getElementById('crashLegend');
  const sensorVisible = sensorLegend && sensorLegend.style.display === 'block';
  const crashVisible  = crashLegend && crashLegend.style.display === 'block';
  const others        = order.map(id => document.getElementById(id)).filter(el => el && el.style.display === 'block');
  const mobile        = window.matchMedia('(max-width: 768px)').matches;
  updateStatsVisibility();

  // The crash legend's accordion is substantially taller than the other
  // filter legends, so it always sits immediately next to the sensor
  // legend (second position) instead of wherever it happens to fall in
  // `order` — otherwise it can end up pushed out to the far edge whenever
  // another legend is also active, which looks off given its height.
  if (crashVisible) {
    const idx = others.indexOf(crashLegend);
    if (idx > -1) others.splice(idx, 1);
    others.unshift(crashLegend);
  }

  if (mobile) {
    let b = 10;
    if (sensorVisible) {
      sensorLegend.style.right  = '10px';
      sensorLegend.style.bottom = `${b}px`;
      b += (sensorLegend.offsetHeight || 150) + 8;
    }
    others.forEach(el => { el.style.right = '10px'; el.style.bottom = `${b}px`; b += (el.offsetHeight || 150) + 8; });
  } else {
    let r = 10;
    if (sensorVisible) {
      sensorLegend.style.bottom = '10px';
      sensorLegend.style.right  = `${r}px`;
      r += (sensorLegend.offsetWidth || 220) + 10;
    }
    others.forEach(el => { el.style.bottom = '10px'; el.style.right = `${r}px`; r += (el.offsetWidth || 220) + 10; });
  }
}

function setupAveragedSegmentControls() {
  const cb = document.getElementById('averagedSegmentsCheckbox');
  if (cb) {
    cb.addEventListener('change', e => {
      showAveragedSegments = e.target.checked;
      const modeGroup    = document.getElementById('averagedModeGroup');
      const legend       = document.getElementById('averagedSegmentsLegend');
      const sensorLegend = document.getElementById('sensorLegend');

      if (showAveragedSegments) {
        if (map.getLayer('averaged-segments')) map.setLayoutProperty('averaged-segments', 'visibility', 'visible');
        if (modeGroup)    modeGroup.style.display    = 'flex';
        if (legend)       legend.style.display       = 'block';
        if (sensorLegend) sensorLegend.style.display = 'none';
        if (map.getLayer('trips-layer')) map.setLayoutProperty('trips-layer', 'visibility', 'none');
        updateAveragedSegmentColors();
      } else {
        if (map.getLayer('averaged-segments')) map.setLayoutProperty('averaged-segments', 'visibility', 'none');
        if (modeGroup)    modeGroup.style.display    = 'none';
        if (legend)       legend.style.display       = 'none';
        if (sensorLegend) sensorLegend.style.display = 'block';
        if (map.getLayer('trips-layer')) map.setLayoutProperty('trips-layer', 'visibility', 'visible');
      }
      updateResetButtonVisibility();
      setTimeout(updateLegendPositions, 50);
      updateStatsVisibility();
    });
  }
  document.querySelectorAll('input[name="averagedMode"]').forEach(r => {
    r.addEventListener('change', e => {
      averagedSegmentMode = e.target.value;
      if (showAveragedSegments) updateAveragedSegmentColors();
    });
  });
}

function setupBrakingControls() {
  const cb = document.getElementById('brakingCheckbox');
  if (!cb) return;

  cb.addEventListener('change', e => {
    showBraking = e.target.checked;
    const legend     = document.getElementById('brakingLegend');
    const visibility = showBraking ? 'visible' : 'none';

    if (map.getLayer('braking-hotspots-halo')) map.setLayoutProperty('braking-hotspots-halo', 'visibility', visibility);
    if (map.getLayer('braking-hotspots-dot'))  map.setLayoutProperty('braking-hotspots-dot',  'visibility', visibility);

    if (legend) legend.style.display = showBraking ? 'block' : 'none';

    // When enabling braking, if a trip is already selected filter immediately
    if (showBraking && selectedTrip) applyBrakingTripFilter(selectedTrip);
    // When disabling, restore full hotspot data
    if (!showBraking) applyBrakingTripFilter(null);

    updateResetButtonVisibility();
    setTimeout(updateLegendPositions, 50);
    updateStatsVisibility();
  });
}

function refreshTripLayerColor() {
  if (!map.getLayer('trips-layer')) return;

  const selectedIds = getSelectedTripIds();

  if (!selectedIds) {
    map.setPaintProperty('trips-layer', 'line-color', currentColorExpression());
    map.setPaintProperty('trips-layer', 'line-opacity', 0.7);
    map.setPaintProperty('trips-layer', 'line-width', 3);
    return;
  }

  const baseExpr = currentColorExpression();
  const isSingle = selectedIds.length === 1;

  map.setPaintProperty('trips-layer', 'line-color', [
    'case',
    isSingle
      ? ['==', ['get', 'trip_id'], selectedIds[0]]
      : ['in', ['get', 'trip_id'], ['literal', selectedIds]],
    baseExpr,
    'rgba(0,0,0,0)'
  ]);
  map.setPaintProperty('trips-layer', 'line-opacity', 1);
  map.setPaintProperty('trips-layer', 'line-width', [
    'case',
    isSingle
      ? ['==', ['get', 'trip_id'], selectedIds[0]]
      : ['in', ['get', 'trip_id'], ['literal', selectedIds]],
    4,
    0
  ]);
}

function setupControls() {
  const resetBtn = document.getElementById('resetButton');
  if (resetBtn) resetBtn.addEventListener('click', resetSelection);

  const searchInput    = document.getElementById('tripSearchInput');
  const dateFromInput  = document.getElementById('tripDateFrom');
  const dateToInput    = document.getElementById('tripDateTo');
  const searchButton   = document.getElementById('tripSearchButton');
  const suggestionBox  = document.getElementById('searchSuggestions');

  if (searchInput && searchButton && suggestionBox) {
    function currentDateRange() {
      const from = dateFromInput ? dateFromInput.value : '';
      const to   = dateToInput   ? dateToInput.value   : '';
      return (from || to) ? { from, to } : null;
    }
    function idsForDate(ids) { const r = currentDateRange(); return r ? ids.filter(id => tripDateInRange(id, r)) : ids; }
    function getSensorNames() { return [...new Set(idsForDate(tripIds).map(id => id.split('_')[0]))].sort(); }
    function hideSuggestions() { suggestionBox.style.display = 'none'; suggestionBox.innerHTML = ''; }
    function showSuggestions(query) {
      const q = query.trim().toLowerCase();
      suggestionBox.innerHTML = '';
      if (!q && !currentDateRange()) { hideSuggestions(); return; }
      const pool    = idsForDate(tripIds);
      const sensors = getSensorNames().filter(s => !q || s.toLowerCase().startsWith(q));
      const trips   = pool.filter(id => (!q || id.toLowerCase().startsWith(q)) && !sensors.some(s => id.startsWith(s)));
      if (!sensors.length && !trips.length) { hideSuggestions(); return; }
      sensors.forEach(sensor => {
        const count = pool.filter(id => id.startsWith(sensor)).length;
        const li = document.createElement('li');
        li.textContent = `📡 ${sensor}  (${count} trip${count !== 1 ? 's' : ''})`;
        li.className = 'suggestion-sensor';
        li.addEventListener('mousedown', () => { searchInput.value = sensor; hideSuggestions(); searchAndHighlightTrip(sensor, currentDateRange()); });
        suggestionBox.appendChild(li);
      });
      trips.forEach(tripId => {
        const li = document.createElement('li');
        li.textContent = `🚴 ${tripId}`;
        li.className = 'suggestion-trip';
        li.addEventListener('mousedown', () => { searchInput.value = tripId; hideSuggestions(); searchAndHighlightTrip(tripId, currentDateRange()); });
        suggestionBox.appendChild(li);
      });
      suggestionBox.style.display = 'block';
    }

    searchButton.addEventListener('click', () => { hideSuggestions(); searchAndHighlightTrip(searchInput.value, currentDateRange()); });
    const clearBtn = document.getElementById('tripClearButton');
    if (clearBtn) clearBtn.addEventListener('click', () => { hideSuggestions(); clearSearch(); });
    searchInput.addEventListener('keypress', e => { if (e.key === 'Enter') { hideSuggestions(); searchAndHighlightTrip(searchInput.value, currentDateRange()); } });
    searchInput.addEventListener('input',  e => showSuggestions(e.target.value));
    searchInput.addEventListener('focus',  e => { if (e.target.value || currentDateRange()) showSuggestions(e.target.value); });
    searchInput.addEventListener('blur',   () => setTimeout(hideSuggestions, 150));

    // Keep from/to in sensible order — if the user picks a "from" date after
    // the current "to" (or vice versa), nudge the other bound to match rather
    // than silently returning zero results.
    if (dateFromInput) {
      dateFromInput.addEventListener('change', () => {
        if (dateToInput && dateToInput.value && dateFromInput.value > dateToInput.value) {
          dateToInput.value = dateFromInput.value;
        }
        hideSuggestions();
        searchAndHighlightTrip(searchInput.value, currentDateRange());
      });
    }
    if (dateToInput) {
      dateToInput.addEventListener('change', () => {
        if (dateFromInput && dateFromInput.value && dateToInput.value < dateFromInput.value) {
          dateFromInput.value = dateToInput.value;
        }
        hideSuggestions();
        searchAndHighlightTrip(searchInput.value, currentDateRange());
      });
    }
  }

  const speedCb = document.getElementById('speedColorsCheckbox');
  if (speedCb) {
    speedCb.addEventListener('change', e => {
      showSpeedColors = e.target.checked;
      if (showSpeedColors && showRoadQuality) {
        showRoadQuality = false;
        document.getElementById('roadQualityCheckbox').checked     = false;
        document.getElementById('roadQualityLegend').style.display = 'none';
      }
      const legend    = document.getElementById('speedLegend');
      const modeGroup = document.getElementById('speedModeGroup');
      if (showSpeedColors) {
        if (legend)    legend.style.display    = 'block';
        if (modeGroup) modeGroup.style.display = 'flex';
      } else {
        if (legend)    legend.style.display    = 'none';
        if (modeGroup) modeGroup.style.display = 'none';
      }
      refreshTripLayerColor();
      updateResetButtonVisibility();
      setTimeout(updateLegendPositions, 50);
      updateStatsVisibility();
    });
  }

  const qualityCb = document.getElementById('roadQualityCheckbox');
  if (qualityCb) {
    qualityCb.addEventListener('change', e => {
      showRoadQuality = e.target.checked;
      if (showRoadQuality && showSpeedColors) {
        showSpeedColors = false;
        document.getElementById('speedColorsCheckbox').checked    = false;
        document.getElementById('speedLegend').style.display      = 'none';
        document.getElementById('speedModeGroup').style.display   = 'none';
      }
      const legend = document.getElementById('roadQualityLegend');
      if (showRoadQuality) {
        if (legend) legend.style.display = 'block';
      } else {
        if (legend) legend.style.display = 'none';
      }
      refreshTripLayerColor();
      updateResetButtonVisibility();
      updateLegendPositions();
      updateStatsVisibility();
    });
  }

  document.querySelectorAll('input[name="speedMode"]').forEach(r => {
    r.addEventListener('change', e => {
      speedMode = e.target.value;
      if (showSpeedColors) refreshTripLayerColor();
    });
  });

  setupAveragedSegmentControls();
  setupBrakingControls();
  setupCrashControls();

  const isoToggle = document.getElementById('isoToggle');
  if (isoToggle) isoToggle.addEventListener('change', e => updateIsochrone(e.target.checked));
}

// Crash / fall legend — three dimensions (intensity / classification /
// outcome), each shown as a collapsible category. Tap a category to reveal
// its sub-labels; only the relevant one needs to be open at a time, which
// keeps the panel short instead of listing all three dimensions at once.
const CRASH_LEGEND_CATEGORIES = [
  {
    id: 'intensity',
    label: 'Intensity',
    hint: 'Circle colour = impact severity',
    rows: [
      { swatch: `<div class="cl-swatch" style="background:#ffea00;"></div>`, label: 'Minor' },
      { swatch: `<div class="cl-swatch" style="background:#ff9100;"></div>`, label: 'Hard' },
      { swatch: `<div class="cl-swatch" style="background:#ff1744;"></div>`, label: 'Severe' },
    ]
  },
  {
    id: 'classification',
    label: 'Classification',
    hint: 'Symbol shape = type of event',
    rows: [
      { swatch: `<div class="cl-swatch cl-swatch--glyph">${crashGlyphSvg('Stationary Fall')}</div>`, label: 'Stationary Fall' },
      { swatch: `<div class="cl-swatch cl-swatch--glyph">${crashGlyphSvg('Low-Speed Fall')}</div>`, label: 'Low-Speed Fall' },
      { swatch: `<div class="cl-swatch cl-swatch--glyph">${crashGlyphSvg('High-Speed Fall')}</div>`, label: 'High-Speed Fall' },
      { swatch: `<div class="cl-swatch cl-swatch--glyph">${crashGlyphSvg('Unclassified')}</div>`, label: 'Unclassified' },
    ]
  },
  {
    id: 'outcome',
    label: 'Outcome',
    hint: 'White ring = rider did not recover',
    rows: [
      { swatch: `<div class="cl-swatch cl-swatch--outcome"></div>`, label: 'Resolved' },
      { swatch: `<div class="cl-swatch cl-swatch--outcome unresolved"></div>`, label: 'Unresolved (white ring)' },
    ]
  },
];

function renderCrashLegend() {
  const legend = document.getElementById('crashLegend');
  if (!legend) return;

  legend.innerHTML = `
    <strong>CRASHES &amp; FALLS</strong>
    <p class="cl-sub">Tap a category to see what its colours &amp; symbols mean.</p>
    ${CRASH_LEGEND_CATEGORIES.map((cat, i) => `
      <button type="button" class="cl-cat" data-cat="${cat.id}" aria-expanded="false">
        <span class="cl-cat-name">${cat.label}</span>
        <span class="cl-chevron">▶</span>
      </button>
      <div class="cl-panel" data-panel="${cat.id}" data-open="false">
        <p class="cl-sub" style="margin:0 0 6px;">${cat.hint}</p>
        ${cat.rows.map(r => `
          <div class="cl-row">
            ${r.swatch}
            <span class="cl-row-label">${r.label}</span>
          </div>
        `).join('')}
      </div>
    `).join('')}
  `;

  // Accordion behaviour: opening one category closes the others so the
  // panel stays short rather than growing with every click.
  legend.querySelectorAll('.cl-cat').forEach(btn => {
    btn.addEventListener('click', () => {
      const catId = btn.dataset.cat;
      const isOpen = btn.getAttribute('aria-expanded') === 'true';

      legend.querySelectorAll('.cl-cat').forEach(b => b.setAttribute('aria-expanded', 'false'));
      legend.querySelectorAll('.cl-panel').forEach(p => p.setAttribute('data-open', 'false'));

      if (!isOpen) {
        btn.setAttribute('aria-expanded', 'true');
        const panel = legend.querySelector(`.cl-panel[data-panel="${catId}"]`);
        if (panel) panel.setAttribute('data-open', 'true');
      }
    });
  });
}


function updateStatsFromMetadata() {
  const source      = map.getSource('trips');
  const allFeatures = source?._data?.features || [];

  const tripStats = {};
  for (const f of allFeatures) {
    const tid   = f.properties.trip_id;
    const dist  = f.properties.gps_distance_m || 0;
    const time  = f.properties.time_diff_s    || 0;
    const speed = f.properties.Speed          || 0;

    if (!tripStats[tid]) tripStats[tid] = { dist: 0, time: 0, speeds: [] };
    tripStats[tid].dist += dist;
    tripStats[tid].time += time;
    if (speed > 0) tripStats[tid].speeds.push(speed);
  }

  let totalDist = 0, totalTime = 0, allSpeeds = [];
  for (const t of Object.values(tripStats)) {
    totalDist += t.dist;
    totalTime += t.time;
    allSpeeds.push(...t.speeds);
  }

  const avgSpeed = allSpeeds.length
    ? (allSpeeds.reduce((a, b) => a + b, 0) / allSpeeds.length).toFixed(1)
    : '—';

  document.getElementById('statTrips').textContent     = tripIds.length;
  document.getElementById('statDistance').textContent  = `${(totalDist / 1000).toFixed(1)} km`;
  document.getElementById('statAvgSpeed').textContent  = `${avgSpeed} km/h`;
  document.getElementById('statTotalTime').textContent = formatDuration(Math.round(totalTime));
}

function renderSensorLegend() {
  const legend = document.getElementById('sensorLegend');
  if (!legend) return;
  legend.innerHTML = `<h4>Sensors</h4>` + Object.entries(sensorColorMap).map(([s, c]) => `
    <div class="speed-legend-item sensor-legend-item" data-sensor="${s}" style="cursor:pointer;" title="Click to highlight ${s}">
      <div class="speed-color-box" style="background:${c};"></div>
      <span>${s}</span>
    </div>`).join('');

  legend.querySelectorAll('.sensor-legend-item').forEach(item => {
    item.addEventListener('click', () => {
      const sensor       = item.dataset.sensor;
      const input        = document.getElementById('tripSearchInput');
      const dateFromInput = document.getElementById('tripDateFrom');
      const dateToInput   = document.getElementById('tripDateTo');
      if (input) input.value = sensor;
      const from = dateFromInput ? dateFromInput.value : '';
      const to   = dateToInput   ? dateToInput.value   : '';
      searchAndHighlightTrip(sensor, (from || to) ? { from, to } : null);
    });
  });

  legend.style.display = 'block';
  updateLegendPositions();
}

window.searchTrip = searchAndHighlightTrip;
