// leaderboard.js
// Reads trips.geojson and computes per-sensor leaderboard stats.

export function buildLeaderboard(geojsonFeatures) {
  const sensors = {};

  for (const feat of geojsonFeatures) {
    const props = feat.properties;
    const tripId = props.trip_id || '';

    // Extract sensor ID (first part before _Trip)
    const sensorId = tripId.split('_Trip')[0] || 'Unknown';
    const tripNum  = tripId.split('_Trip')[1] || '0';

    if (!sensors[sensorId]) {
      sensors[sensorId] = {
        id:       sensorId,
        trips:    new Set(),
        totalKm:  0,
        stops:    0,
        topSpeed: 0,
      };
    }

    const s = sensors[sensorId];
    s.trips.add(tripId);

    const dist  = props.gps_distance_m || 0;
    const speed = props.Speed          || 0;

    s.totalKm  += dist / 1000;
    s.topSpeed  = Math.max(s.topSpeed, speed);

    // Count stops: segments where speed <= 2 km/h
    if (speed <= 2) s.stops++;
  }

  // Convert to array
  return Object.values(sensors).map(s => ({
    ...s,
    rides:    s.trips.size,
    totalKm:  Math.round(s.totalKm * 10) / 10,
  }));
}

function rankLabel(i) {
  if (i === 0) return ['gold',   '1st'];
  if (i === 1) return ['silver', '2nd'];
  if (i === 2) return ['bronze', '3rd'];
  return ['', `${i + 1}th`];
}

function renderList(elId, items, valueKey, unit, decimals = 1) {
  const el = document.getElementById(elId);
  if (!el) return;
  el.innerHTML = items
    .map((item, i) => {
      const [cls, label] = rankLabel(i);
      const val = typeof item[valueKey] === 'number'
        ? item[valueKey].toFixed(decimals)
        : item[valueKey];
      return `
        <li>
          <span class="lb-rank ${cls}">${label}</span>
          <span class="lb-sensor">${item.id}</span>
          <span class="lb-value">${val}${unit}</span>
        </li>`;
    })
    .join('');
}

export function renderLeaderboard(sensors) {
  const byKm    = [...sensors].sort((a, b) => b.totalKm  - a.totalKm).slice(0, 5);
  const byRides = [...sensors].sort((a, b) => b.rides    - a.rides).slice(0, 5);
  const byStops = [...sensors].sort((a, b) => b.stops    - a.stops).slice(0, 5);
  const bySpeed = [...sensors].sort((a, b) => b.topSpeed - a.topSpeed).slice(0, 5);

  renderList('lbKm',    byKm,    'totalKm',  ' km', 1);
  renderList('lbRides', byRides, 'rides',    ' rides', 0);
  renderList('lbStops', byStops, 'stops',    ' stops', 0);
  renderList('lbSpeed', bySpeed, 'topSpeed', ' km/h', 1);
}