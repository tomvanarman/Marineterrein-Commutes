// app.js 
// Bike Sensor Data Visualization
// This application visualizes bike trip data from PMTiles, including speed, road quality,
// traffic light analysis, and averaged road segments

import { CONFIG } from './config.js';

console.log('🚀 Starting bike visualization...');

// Initialize map
const map = new mapboxgl.Map({
  container: 'map',
  style: CONFIG.MAP_STYLE,
  center: CONFIG.MAP_CENTER,
  zoom: CONFIG.MAP_ZOOM
});

window.map = map; // Make map available globally for debugging

// State management

let tripLayers = [];                    // Array of all trip layer IDs loaded from PMTiles
let speedMode = 'gradient';             // 'gradient' or 'categories' for speed visualization
let showSpeedColors = false;            // Toggle for speed color overlay
let showRoadQuality = false;            // Toggle for road quality overlay
let selectedTrip = null;                // Currently selected trip layer ID
let tripsMetadata = null;               // Metadata object containing trip statistics
let currentPopup = null;                // Reference to currently open popup
let showAveragedSegments = false;       // Toggle for averaged road segments layer
let averagedSegmentMode = 'composite';  // 'speed', 'quality', or 'composite' for segment visualization
let searchActive = false;               // True when a search filter is currently applied

// Sensor color palette — fits the cyan/gold dashboard scheme
const SENSOR_COLORS = [
  '#34CCCC', // sc-cyan
  '#FFCC33', // sc-gold
  '#5B8FFF', // electric blue
  '#CC5BAA', // violet-magenta
  '#33CCAA', // teal-green
  '#FF7A3D', // amber-orange
  '#88DDFF', // ice blue
  '#FFE066', // soft gold
  '#CC3355', // crimson
  '#66FF99', // mint
  '#AA88FF', // lavender
  '#FF9966', // peach
  '#00CCFF', // sky cyan
  '#FFB3DE', // rose
  '#44FFDD', // aqua
  '#FFAA00', // deep amber
  '#7BFFB3', // seafoam
  '#FF6680', // coral
  '#B3EEFF', // pale ice
  '#D4FF66', // lime
];

const DEFAULT_COLOR = '#34CCCC'; // fallback (sc-cyan)

// Map from sensor prefix → colour, built once trips are loaded
const sensorColorMap = {};

function buildSensorColorMap(layerIds) {
  const sensors = [...new Set(layerIds.map(id => id.split('_')[0]))].sort();
  sensors.forEach((sensor, i) => {
    sensorColorMap[sensor] = SENSOR_COLORS[i % SENSOR_COLORS.length];
  });
  console.log('🎨 Sensor colour map:', sensorColorMap);
}

// Return the colour assigned to a layer based on its sensor prefix
function getSensorColor(layerId) {
  const sensor = layerId.split('_')[0];
  return sensorColorMap[sensor] || DEFAULT_COLOR;
}

// Color expressions- Returns different colors based on different scores/values
function getSpeedColorExpression(mode) {
  const speedValue = ['to-number', ['coalesce', ['get', 'Speed'], ['get', 'speed'], 0]];
  
  if (mode === 'gradient') {
    return ['interpolate', ['linear'], speedValue, 0, '#808080', 2, '#DC2626', 5, '#F97316', 10, '#FACC15', 15, '#22C55E', 20, '#3B82F6', 25, '#6366F1'];
  } else {
    return ['step', speedValue, '#808080', 2, '#DC2626', 5, '#F97316', 10, '#FACC15', 15, '#22C55E', 20, '#3B82F6', 25, '#6366F1'];
  }
}


function getRoadQualityColorExpression() {
  return ['match', ['get', 'road_quality'], 1, '#22C55E', 2, '#84CC16', 3, '#FACC15', 4, '#F97316', 5, '#DC2626', '#808080'];
}


function getAveragedSpeedColorExpression() {
  return ['interpolate', ['linear'], ['get', 'avg_speed'], 0, '#DC2626', 5, '#F97316', 10, '#FACC15', 15, '#22C55E', 20, '#3B82F6', 25, '#6366F1'];
}

function getAveragedQualityColorExpression() {
  return ['interpolate', ['linear'], ['get', 'avg_quality'], 1, '#22C55E', 2, '#84CC16', 3, '#FACC15', 4, '#F97316', 5, '#DC2626'];
}

function getCompositeScoreColorExpression() {
  return ['interpolate', ['linear'], ['get', 'composite_score'], 0, '#22C55E', 25, '#84CC16', 50, '#FACC15', 75, '#F97316', 100, '#DC2626'];
}

function getQualityLabel(quality) {
  if (quality <= 1.5) return 'Perfect';
  if (quality <= 2.5) return 'Normal';
  if (quality <= 3.5) return 'Outdated';
  if (quality <= 4.5) return 'Bad';
  return 'No road';
}

function getCompositeLabel(score) {
  if (score < 20) return 'Excellent';
  if (score < 40) return 'Good';
  if (score < 60) return 'Moderate';
  if (score < 80) return 'Poor';
  return 'Critical';
}

// Data loading functions
async function loadMetadata() {
  const possiblePaths = [`${CONFIG.DATA_URL}/trips_metadata.json`, '/trips_metadata.json', './trips_metadata.json', 'trips_metadata.json'];
  
  for (const path of possiblePaths) {
    try {
      console.log('Trying to load metadata from:', path);
      const response = await fetch(path);
      if (response.ok) {
        tripsMetadata = await response.json();
        console.log('✅ Loaded trip metadata from', path, 'for', Object.keys(tripsMetadata).length, 'trips');
        return tripsMetadata;
      }
    } catch (err) {
      console.log('❌ Failed to load from', path);
    }
  }
  
  console.warn('⚠️ Could not load metadata');
  return null;
}

async function loadAveragedSegments() {
  const possiblePaths = ['./road_segments_averaged.json', 'road_segments_averaged.json', '/road_segments_averaged.json', `${CONFIG.DATA_URL}/road_segments_averaged.json`];
  
  for (const path of possiblePaths) {
    try {
      console.log('🔍 Trying to load averaged segments from:', path);
      const response = await fetch(path);
      if (response.ok) {
        const data = await response.json();
        console.log(`✅ Loaded ${data.features.length} averaged road segments from`, path);
        return data;
      }
    } catch (err) {
      console.log('❌ Failed to load from', path);
    }
  }
  
  console.error('❌ Could not load averaged segments');
  return null;
}

// Statistics calculator (ie: for aggregated stats)
function getTripStats(tripId) {
  if (!tripsMetadata) {
    console.warn('⚠️ No metadata loaded');
    return null;
  }
  
  const variations = [
    tripId,
    tripId.replace(/_clean_processed$/i, ''),
    tripId.replace(/_clean$/i, ''),
    tripId.replace(/_processed$/i, ''),
    tripId.replace(/_clean/gi, '').replace(/_processed/gi, ''),
    tripId.split('_clean')[0],
    tripId.split('_processed')[0]
  ];
  // Add: extract just SensorID_TripN
  const tripMatch = tripId.match(/^(.+_Trip\d+)/i);
  if (tripMatch) variations.push(tripMatch[1]);

  let tripData = null;
  for (const variant of variations) {
    if (tripsMetadata[variant]) {
      tripData = tripsMetadata[variant];
      break;
    }
  }
  
  if (!tripData) return null;
  
  const meta = tripData.metadata || tripData;
  const gnssLine = meta['GNSS'];
  if (!gnssLine) return null;
  
  // Parse GNSS line (comma-separated values)
  const parts = gnssLine.split(',');
  return {
    duration: parts[1],
    stops: parts[2],
    distance: parseFloat(parts[3]) || 0,
    avgSpeed: parseFloat(parts[4]) || 0,
    avgSpeedWOS: parseFloat(parts[5]) || 0,
    maxSpeed: parseFloat(parts[6]) || 0,
    elevation: parseFloat(parts[11]) || 0
  };
}

function calculateAggregateStats() {
  if (!tripsMetadata) return null;
  
  let totalDistance = 0, totalTime = 0, totalAvgSpeed = 0, tripCount = 0;
  
  Object.keys(tripsMetadata).forEach(tripId => {
    const stats = getTripStats(tripId);
    if (stats) {
      totalDistance += stats.distance;
      const [part1, part2] = stats.duration.split(':').map(Number);
      totalTime += (part1 * 60 + part2) * 60;
      totalAvgSpeed += stats.avgSpeed;
      tripCount++;
    }
  });
  
  const avgSpeed = tripCount > 0 ? (totalAvgSpeed / tripCount) : 0;
  return { tripCount, totalDistance: totalDistance.toFixed(1), totalTime: formatDuration(totalTime), avgSpeed: avgSpeed.toFixed(1) };
}

function formatDuration(seconds) {
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  return hours > 0 ? `${hours}h ${minutes}m` : `${minutes}m`;
}

// Trip selection functions
function resetSelection() {
  console.log('Resetting selection');
  selectedTrip = null;
  
  if (currentPopup) {
    currentPopup.remove();
    currentPopup = null;
  }
  
  // Reset all trip layers to default style
  tripLayers.forEach(layerId => {
    try {
      map.setPaintProperty(layerId, 'line-opacity', 0.7);
      map.setPaintProperty(layerId, 'line-width', 3);
      
      if (showSpeedColors) {
        map.setPaintProperty(layerId, 'line-color', getSpeedColorExpression(speedMode));
      } else if (showRoadQuality) {
        map.setPaintProperty(layerId, 'line-color', getRoadQualityColorExpression());
      } else {
        map.setPaintProperty(layerId, 'line-color', getSensorColor(layerId));
      }
    } catch (err) {
      console.error('Error resetting layer:', layerId, err);
    }
  });
  
  // Reset UI
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

  const searchInput = document.getElementById('tripSearchInput');
  const clearBtn = document.getElementById('tripClearButton');
  if (searchInput) searchInput.value = '';
  if (clearBtn) clearBtn.style.display = 'none';

  if (currentPopup) {
    currentPopup.remove();
    currentPopup = null;
  }

  // Restore all trips to full opacity with whatever layer colour is active
  tripLayers.forEach(layerId => {
    try {
      map.setPaintProperty(layerId, 'line-opacity', 0.7);
      map.setPaintProperty(layerId, 'line-width', 3);
      if (showSpeedColors) {
        map.setPaintProperty(layerId, 'line-color', getSpeedColorExpression(speedMode));
      } else if (showRoadQuality) {
        map.setPaintProperty(layerId, 'line-color', getRoadQualityColorExpression());
      } else {
        map.setPaintProperty(layerId, 'line-color', getSensorColor(layerId));
      }
    } catch (err) {
      console.error('Error clearing search on layer:', layerId, err);
    }
  });

  // Reset stats panel
  document.getElementById('resetButton').style.display = 'none';
  document.getElementById('selectedTripRow').style.display = 'none';
  document.getElementById('statTripRow').style.display = 'flex';
  document.getElementById('statDistanceRow').style.display = 'flex';
  document.getElementById('statAvgSpeedRow').style.display = 'flex';
  document.getElementById('statTotalTimeRow').style.display = 'flex';
}


function showSelection(layerId) {
  console.log('Showing selection for:', layerId);
  document.getElementById('resetButton').style.display = 'block';
  document.getElementById('statTripRow').style.display = 'none';
  document.getElementById('statDistanceRow').style.display = 'none';
  document.getElementById('statAvgSpeedRow').style.display = 'none';
  document.getElementById('statTotalTimeRow').style.display = 'none';
  document.getElementById('selectedTripRow').style.display = 'flex';
  
  const tripName = layerId.replace(/_/g, ' ').replace(/processed/gi, '').replace(/clean/gi, '').trim();
  document.getElementById('selectedTrip').textContent = tripName;
}

function searchAndHighlightTrip(searchTerm) {
  if (!searchTerm) {
    resetSelection();
    return;
  }

  const normalizedSearch = searchTerm.toLowerCase().trim();

  // Find all trips that match the search term
  const matchingTrips = tripLayers.filter(layerId =>
    layerId.toLowerCase().includes(normalizedSearch)
  );

  if (matchingTrips.length === 0) {
    console.log('❌ No trip found matching:', searchTerm);
    alert(`No trip found matching: ${searchTerm}`);
    return false;
  }

  // Multiple matches = sensor-group search; single match = exact trip
  const isGroupSearch = matchingTrips.length > 1;

  selectedTrip = isGroupSearch ? null : matchingTrips[0];
  searchActive = true;

  const clearBtn = document.getElementById('tripClearButton');
  if (clearBtn) clearBtn.style.display = 'inline-block';

  tripLayers.forEach(id => {
    try {
      if (matchingTrips.includes(id)) {
        map.setPaintProperty(id, 'line-opacity', 0.85);
        map.setPaintProperty(id, 'line-width', 3);
        map.setPaintProperty(id, 'line-color', '#FF69B4'); // hot pink for all matches
      } else {
        map.setPaintProperty(id, 'line-opacity', 0.12);
        map.setPaintProperty(id, 'line-width', 2);
      }
    } catch (err) {
      console.error('Error updating layer:', id, err);
    }
  });

  if (isGroupSearch) {
    // Show group summary in stats panel
    document.getElementById('resetButton').style.display = 'block';
    document.getElementById('statTripRow').style.display = 'none';
    document.getElementById('statDistanceRow').style.display = 'none';
    document.getElementById('statAvgSpeedRow').style.display = 'none';
    document.getElementById('statTotalTimeRow').style.display = 'none';
    document.getElementById('selectedTripRow').style.display = 'flex';
    document.getElementById('selectedTrip').textContent =
      `${searchTerm.toUpperCase()} — ${matchingTrips.length} trips`;
  } else {
    showSelection(matchingTrips[0]);
  }

  // Zoom to fit all matched trips
  try {
    const allFeatures = matchingTrips.flatMap(tripId =>
      map.querySourceFeatures('trips', { sourceLayer: tripId })
    );
    if (allFeatures.length > 0) {
      const bbox = turf.bbox({ type: 'FeatureCollection', features: allFeatures });
      map.fitBounds(bbox, { padding: 50, duration: 1000 });
    }
  } catch (err) {
    console.error('Error zooming to trips:', err);
  }

  return true;
}


function updateAveragedSegmentColors() {
  if (!map.getLayer('averaged-segments')) return;
  
  let colorExpression;
  switch (averagedSegmentMode) {
    case 'speed': colorExpression = getAveragedSpeedColorExpression(); break;
    case 'quality': colorExpression = getAveragedQualityColorExpression(); break;
    case 'composite': colorExpression = getCompositeScoreColorExpression(); break;
  }
  
  map.setPaintProperty('averaged-segments', 'line-color', colorExpression);
  console.log('🎨 Updated averaged segment colors to:', averagedSegmentMode);
}

// Map layer setup
async function setupAveragedSegments() {
  console.log('📡 Loading averaged road segments...');
  const segmentsData = await loadAveragedSegments();
  if (!segmentsData) {
    console.error('❌ Could not load averaged segments');
    return;
  }
  
  // Add GeoJSON source
  map.addSource('averaged-segments', { type: 'geojson', data: segmentsData });
  
  // Add line layer (initially hidden)
  map.addLayer({
    id: 'averaged-segments',
    type: 'line',
    source: 'averaged-segments',
    layout: { 'visibility': 'none', 'line-cap': 'round', 'line-join': 'round' },
    paint: {
      'line-color': getCompositeScoreColorExpression(),
      'line-width': ['interpolate', ['linear'], ['zoom'], 10, 2, 14, 4, 16, 6],
      'line-opacity': 0.8
    }
  });
  
  console.log('✅ Averaged segments layer added');
  
  // Click handler to show segment details
  map.on('click', 'averaged-segments', (e) => {
    e.preventDefault();
    if (e.originalEvent) e.originalEvent.stopPropagation();
    
    const props = e.features[0].properties;
    let qualityText = props.avg_quality ? `🛣️ Avg Quality: ${props.avg_quality} (${getQualityLabel(props.avg_quality)})` : '🛣️ Quality: No data';
    let compositeText = getCompositeLabel(props.composite_score);
    
    new mapboxgl.Popup().setLngLat(e.lngLat).setHTML(`
      <strong>📊 Averaged Road Segment</strong><br>
      🚴 Avg Speed: ${props.avg_speed} km/h<br>
      📈 Speed Range: ${props.min_speed} - ${props.max_speed} km/h<br>
      ${qualityText}<br>
      📏 Distance: ${props.distance_m}m<br>
      🎯 Composite Score: ${props.composite_score} (${compositeText})<br>
      📍 Observations: ${props.observation_count}<br>
      🚲 From ${props.trip_count} trips
    `).addTo(map);
  });
  
  // Cursor changes
  map.on('mouseenter', 'averaged-segments', () => { map.getCanvas().style.cursor = 'pointer'; });
  map.on('mouseleave', 'averaged-segments', () => { map.getCanvas().style.cursor = ''; });
}

// Map event handlers (ie: initializing all data and layers)
map.on('error', (e) => { console.error('❌ Map error:', e); });

map.on('load', async () => {
  console.log('✅ Map loaded');
  await loadMetadata();
  
  try {
    console.log('📡 Loading bike trips from:', CONFIG.PMTILES_URL);
    
    // Setup PMTiles protocol
    const protocol = new pmtiles.Protocol();
    mapboxgl.addProtocol('pmtiles', protocol.tile);
    
    const pmtilesUrl = CONFIG.PMTILES_URL;
    const p = new pmtiles.PMTiles(pmtilesUrl);
    protocol.add(p);
    
    const metadata = await p.getMetadata();
    console.log('✅ PMTiles loaded:', metadata);
    
    const layers = metadata.vector_layers || [];
    tripLayers = layers.map(l => l.id);
    console.log('📊 Found', tripLayers.length, 'trips');
    
    // Add trips source
    map.addSource('trips', { type: 'vector', url: `pmtiles://${pmtilesUrl}`, attribution: 'Bike sensor data' });
    
    // Build sensor → colour mapping before adding layers
    buildSensorColorMap(tripLayers);

    // Add a layer for each trip, coloured by sensor
    tripLayers.forEach(layerId => {
      map.addLayer({
        id: layerId,
        type: 'line',
        source: 'trips',
        'source-layer': layerId,
        paint: { 'line-color': getSensorColor(layerId), 'line-width': 3, 'line-opacity': 0.7 }
      });
    });

    console.log('✅ All trips loaded and visible');
    
    await setupAveragedSegments();
    
    map.setCenter([4.9041, 52.3676]);
    map.setZoom(13);
    
    setupControls();
    setupClickHandlers();
    updateStatsFromMetadata();
    renderSensorLegend();
    updateStatsVisibility();

  } catch (err) {
    console.error('❌ Error loading trips:', err);
  }
});

function updateStatsVisibility() {
  const statsEl = document.getElementById('stats');
  if (!statsEl) return;
  const anyLegendActive = showSpeedColors || showRoadQuality || showAveragedSegments;
  statsEl.style.display = window.innerWidth <= 768 && anyLegendActive ? 'none' : 'block';
}

window.addEventListener('resize', updateStatsVisibility);

function updateLegendPositions() {
  const legendOrder = ['sensorLegend', 'speedLegend', 'roadQualityLegend', 'averagedSegmentsLegend'];
  
  const visibleLegends = legendOrder
    .map(id => document.getElementById(id))
    .filter(el => el && el.style.display === 'block');

  const isMobile = window.innerWidth <= 768;

  if (isMobile) {
    let cumulativeBottom = 10;
    visibleLegends.forEach(el => {
      el.style.right = '10px';
      el.style.bottom = `${cumulativeBottom}px`;
      cumulativeBottom += (el.offsetHeight || el.scrollHeight || 150) + 8;
    });
  } else {
    let cumulativeOffset = 10;
    visibleLegends.forEach(el => {
      el.style.bottom = '10px';
      el.style.right = `${cumulativeOffset}px`;
      cumulativeOffset += (el.offsetWidth || 220) + 10;
    });
  }
}

function setupAveragedSegmentControls() {
  const avgSegmentsCheckbox = document.getElementById('averagedSegmentsCheckbox');
  if (avgSegmentsCheckbox) {
    avgSegmentsCheckbox.addEventListener('change', (e) => {
      showAveragedSegments = e.target.checked;
      const avgModeGroup = document.getElementById('averagedModeGroup');
      const avgLegend = document.getElementById('averagedSegmentsLegend');
      
      if (showAveragedSegments) {
        if (map.getLayer('averaged-segments')) map.setLayoutProperty('averaged-segments', 'visibility', 'visible');
        if (avgModeGroup) avgModeGroup.style.display = 'flex';
        if (avgLegend) avgLegend.style.display = 'block';
        const sensorLegend = document.getElementById('sensorLegend');
        if (sensorLegend) sensorLegend.style.display = 'none';
        updateAveragedSegmentColors();
        tripLayers.forEach(layerId => { map.setLayoutProperty(layerId, 'visibility', 'none'); });
        console.log('📊 Averaged segments ON');
      } else {
        if (map.getLayer('averaged-segments')) map.setLayoutProperty('averaged-segments', 'visibility', 'none');
        if (avgModeGroup) avgModeGroup.style.display = 'none';
        if (avgLegend) avgLegend.style.display = 'none';
        const sensorLegend = document.getElementById('sensorLegend');
        if (sensorLegend) sensorLegend.style.display = 'block';
        tripLayers.forEach(layerId => { map.setLayoutProperty(layerId, 'visibility', 'visible'); });
        console.log('📊 Averaged segments OFF');
      }
      
      setTimeout(updateLegendPositions, 50);

      updateStatsVisibility();
    });
  }
  
  document.querySelectorAll('input[name="averagedMode"]').forEach(radio => {
    radio.addEventListener('change', (e) => {
      averagedSegmentMode = e.target.value;
      if (showAveragedSegments) updateAveragedSegmentColors();
    });
  });
}

function setupControls() {
  const resetButton = document.getElementById('resetButton');
  if (resetButton) resetButton.addEventListener('click', () => { resetSelection(); });
  
  const searchInput = document.getElementById('tripSearchInput');
  const searchButton = document.getElementById('tripSearchButton');
  const suggestionBox = document.getElementById('searchSuggestions');

  if (searchInput && searchButton && suggestionBox) {

    // --- Autocomplete helpers ---

    // Extract unique sensor prefixes (everything before the first underscore)
    function getSensorNames() {
      return [...new Set(tripLayers.map(id => id.split('_')[0]))].sort();
    }

    function hideSuggestions() {
      suggestionBox.style.display = 'none';
      suggestionBox.innerHTML = '';
    }

    function showSuggestions(query) {
      const q = query.trim().toLowerCase();
      suggestionBox.innerHTML = '';

      if (!q) { hideSuggestions(); return; }

      const sensorNames = getSensorNames();

      // Sensor-level matches (e.g. "602" matches "602CA")
      const sensorMatches = sensorNames.filter(s => s.toLowerCase().startsWith(q));

      // Full trip-level matches not already covered by a sensor match
      const tripMatches = tripLayers.filter(id => {
        const lower = id.toLowerCase();
        return lower.startsWith(q) && !sensorMatches.some(s => id.startsWith(s));
      });

      if (sensorMatches.length === 0 && tripMatches.length === 0) {
        hideSuggestions();
        return;
      }

      sensorMatches.forEach(sensor => {
        const count = tripLayers.filter(id => id.startsWith(sensor)).length;
        const li = document.createElement('li');
        li.textContent = `📡 ${sensor}  (${count} trip${count !== 1 ? 's' : ''})`;
        li.className = 'suggestion-sensor';
        li.addEventListener('mousedown', () => {   // mousedown fires before blur
          searchInput.value = sensor;
          hideSuggestions();
          searchAndHighlightTrip(sensor);
        });
        suggestionBox.appendChild(li);
      });

      tripMatches.forEach(tripId => {
        const li = document.createElement('li');
        li.textContent = `🚴 ${tripId}`;
        li.className = 'suggestion-trip';
        li.addEventListener('mousedown', () => {
          searchInput.value = tripId;
          hideSuggestions();
          searchAndHighlightTrip(tripId);
        });
        suggestionBox.appendChild(li);
      });

      suggestionBox.style.display = 'block';
    }

    // --- Event listeners ---

    searchButton.addEventListener('click', () => {
      hideSuggestions();
      searchAndHighlightTrip(searchInput.value);
    });

    const clearButton = document.getElementById('tripClearButton');
    if (clearButton) {
      clearButton.addEventListener('click', () => {
        hideSuggestions();
        clearSearch();
      });
    }

    searchInput.addEventListener('keypress', (e) => {
      if (e.key === 'Enter') {
        hideSuggestions();
        searchAndHighlightTrip(searchInput.value);
      }
    });

    searchInput.addEventListener('input', (e) => showSuggestions(e.target.value));
    searchInput.addEventListener('focus', (e) => { if (e.target.value) showSuggestions(e.target.value); });
    // Delay hide so mousedown on a suggestion fires first
    searchInput.addEventListener('blur', () => setTimeout(hideSuggestions, 150));
  }
  
  const speedColorsCheckbox = document.getElementById('speedColorsCheckbox');
  if (speedColorsCheckbox) {
    speedColorsCheckbox.addEventListener('change', (e) => {
      showSpeedColors = e.target.checked;
      console.log('Speed colors toggled:', showSpeedColors);
      
      if (showSpeedColors && showRoadQuality) {
        showRoadQuality = false;
        document.getElementById('roadQualityCheckbox').checked = false;
        document.getElementById('roadQualityLegend').style.display = 'none';
      }
      
      const speedLegend = document.getElementById('speedLegend');
      const speedModeGroup = document.getElementById('speedModeGroup');
      
      if (showSpeedColors) {
        const colorExpression = getSpeedColorExpression(speedMode);
        tripLayers.forEach(layerId => { map.setPaintProperty(layerId, 'line-color', colorExpression); });
        speedLegend.style.display = 'block';
        speedModeGroup.style.display = 'flex';
      } else {
        tripLayers.forEach(layerId => { map.setPaintProperty(layerId, 'line-color', getSensorColor(layerId)); });
        speedLegend.style.display = 'none';
        speedModeGroup.style.display = 'none';
      }
      
      setTimeout(updateLegendPositions, 50);

      updateStatsVisibility();
    });
  }

  const roadQualityCheckbox = document.getElementById('roadQualityCheckbox');
  if (roadQualityCheckbox) {
    roadQualityCheckbox.addEventListener('change', (e) => {
      showRoadQuality = e.target.checked;
      console.log('Road quality toggled:', showRoadQuality);
      
      if (showRoadQuality && showSpeedColors) {
        showSpeedColors = false;
        document.getElementById('speedColorsCheckbox').checked = false;
        document.getElementById('speedLegend').style.display = 'none';
        document.getElementById('speedModeGroup').style.display = 'none';
      }
      
      const roadQualityLegend = document.getElementById('roadQualityLegend');
      
      if (showRoadQuality) {
        const colorExpression = getRoadQualityColorExpression();
        tripLayers.forEach(layerId => {
          map.setPaintProperty(layerId, 'line-color', colorExpression);
        });
        roadQualityLegend.style.display = 'block';
      } else {
        tripLayers.forEach(layerId => {
          map.setPaintProperty(layerId, 'line-color', getSensorColor(layerId));
        });
        roadQualityLegend.style.display = 'none';
      }
      
      updateLegendPositions();

      updateStatsVisibility();
    });
  }

  document.querySelectorAll('input[name="speedMode"]').forEach(radio => {
    radio.addEventListener('change', (e) => {
      speedMode = e.target.value;
      if (showSpeedColors) {
        tripLayers.forEach(layerId => {
          map.setPaintProperty(layerId, 'line-color', getSpeedColorExpression(speedMode));
        });
      }
    });
  });

  // Setup averaged segment controls
  setupAveragedSegmentControls();
}

function setupClickHandlers() {
  tripLayers.forEach(layerId => {
    map.on('click', layerId, async (e) => {
      console.log('Layer clicked:', layerId);
      e.preventDefault();
      if (e.originalEvent) {
        e.originalEvent.stopPropagation();
      }
      
      if (currentPopup) {
        currentPopup.remove();
      }
      
      const props = e.features[0].properties;
      const speed = parseFloat(props.Speed || props.speed || 0);
      const roadQuality = parseInt(props.road_quality || props.roadQuality || 0);
      
      selectedTrip = layerId;
      tripLayers.forEach(id => {
        try {
          if (id === layerId) {
            map.setPaintProperty(id, 'line-opacity', 1.0);
            map.setPaintProperty(id, 'line-width', 4);
          } else {
            map.setPaintProperty(id, 'line-opacity', 0.15);
            map.setPaintProperty(id, 'line-width', 2);
          }
        } catch (err) {
          console.error('Error updating layer:', id, err);
        }
      });
      
      showSelection(layerId);
      
      const stats = getTripStats(props.trip_id || layerId);
      
      let distanceKm, avgSpeed, maxSpeed, durationFormatted;
      
      if (stats) {
        distanceKm = stats.distance.toFixed(2);
        avgSpeed = stats.avgSpeed.toFixed(1);
        maxSpeed = stats.maxSpeed.toFixed(1);
        durationFormatted = stats.duration;
      } else {
        distanceKm = '—';
        avgSpeed = '—';
        maxSpeed = '—';
        durationFormatted = '—';
      }
      
      const qualityLabels = {
        1: 'Perfect',
        2: 'Normal',
        3: 'Outdated',
        4: 'Bad',
        5: 'No road',
        0: 'Unknown'
      };
      const qualityLabel = qualityLabels[roadQuality] || 'Unknown';
      
      const popupTripName = layerId.replace(/_/g, ' ').replace(/processed/gi, '').replace(/clean/gi, '').trim();
      currentPopup = new mapboxgl.Popup()
        .setLngLat(e.lngLat)
        .setHTML(`
          <strong>${popupTripName}</strong><br>
          🚴 Speed at point: ${speed} km/h<br>
          🛣️ Road quality: ${roadQuality} (${qualityLabel})<br>
          📊 Average speed: ${avgSpeed} km/h<br>
          🏁 Max speed: ${maxSpeed} km/h<br>
          📍 Total distance: ${distanceKm} km<br>
          ⏱️ Duration: ${durationFormatted}
        `)
        .addTo(map);
    });

    map.on('mouseenter', layerId, () => {
      map.getCanvas().style.cursor = 'pointer';
    });

    map.on('mouseleave', layerId, () => {
      map.getCanvas().style.cursor = '';
    });
  });
  
  // Click anywhere on map to deselect trip or clear search
  map.on('click', (e) => {
    if (!e.defaultPrevented) {
      if (searchActive) {
        clearSearch();
      } else if (selectedTrip) {
        resetSelection();
      }
    }
  });
}

function updateStatsFromMetadata() {
  // Always show the actual number of loaded trips first
  const actualTripCount = tripLayers.length;
  document.getElementById('statTrips').textContent = actualTripCount;
  
  if (!tripsMetadata) {
    console.warn('⚠️ No metadata loaded, showing trip count only');
    return;
  }
    
  const aggregateStats = calculateAggregateStats();
  
  if (aggregateStats) {
    // Use actual loaded trip count, not metadata count
    document.getElementById('statTrips').textContent = actualTripCount;
    document.getElementById('statDistance').textContent = `${aggregateStats.totalDistance} km`;
    document.getElementById('statAvgSpeed').textContent = `${aggregateStats.avgSpeed} km/h`;
    document.getElementById('statTotalTime').textContent = aggregateStats.totalTime;
    console.log('✅ Stats updated from metadata:', aggregateStats);
    console.log(`📊 Actual trips loaded: ${actualTripCount}, Metadata trips: ${aggregateStats.tripCount}`);
  }
}

// Render the sensor colour legend dynamically
function renderSensorLegend() {
  const legend = document.getElementById('sensorLegend');
  if (!legend) return;

  const items = Object.entries(sensorColorMap).map(([sensor, color]) => `
    <div class="speed-legend-item">
      <div class="speed-color-box" style="background:${color};"></div>
      <span>${sensor}</span>
    </div>
  `).join('');

  legend.innerHTML = `<h4>Sensors</h4>${items}`;
  legend.style.display = 'block';
  updateLegendPositions();
}

// Make search function available globally for console testing
window.searchTrip = searchAndHighlightTrip;
