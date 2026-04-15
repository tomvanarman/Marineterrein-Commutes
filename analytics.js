// analytics.js
// Handles analytics visualizations- speed and road quality

let analyticsCharts = {};
let analyticsData = null;

// Initialize analytics panel
function initAnalytics() {
  // Panel toggle
  const panelHandle = document.getElementById('panelHandle');
  const panel = document.getElementById('analyticsPanel');
  const toggleIcon = document.getElementById('panelToggleIcon');
  const toggleText = document.getElementById('panelToggleText');
  
  panelHandle.addEventListener('click', () => {
    panel.classList.toggle('open');
    if (panel.classList.contains('open')) {
      toggleIcon.textContent = '▼';
      toggleText.textContent = 'Hide Analytics';
    } else {
      toggleIcon.textContent = '▲';
      toggleText.textContent = 'Show Analytics & Visualizations';
    }
  });

  // Tab switching
  document.querySelectorAll('.panel-tab').forEach(tab => {
    tab.addEventListener('click', () => {
      const targetTab = tab.dataset.tab;
      
      // Update active states
      document.querySelectorAll('.panel-tab').forEach(t => t.classList.remove('active'));
      document.querySelectorAll('.tab-pane').forEach(p => p.classList.remove('active'));
      
      tab.classList.add('active');
      document.getElementById(`tab-${targetTab}`).classList.add('active');
    });
  });
}

// Process metadata and generate analytics
function processMetadataForAnalytics(metadata) {
  if (!metadata) return null;
  
  const trips = [];
  const sensors = {};
  
  Object.keys(metadata).forEach(tripId => {
    const trip = metadata[tripId];
    const gnss = trip['GNSS'];
    
    if (!gnss) return;
    
    const parts = gnss.split(',');
    const duration = parts[1]; // "14:50"
    const stops = parts[2]; // "01:12"
    const distance = parseFloat(parts[3]) || 0;
    const avgSpeed = parseFloat(parts[4]) || 0;
    const avgSpeedWOS = parseFloat(parts[5]) || 0;
    const maxSpeed = parseFloat(parts[6]) || 0;
    
    // Extract sensor ID from trip ID (e.g., "602B3" from "602B3_Trip4")
    const sensorId = tripId.split('_')[0];
    
    // Parse timestamps
    const tripStartEnd = trip['Trip start/end'];
    let startTime = null;
    if (tripStartEnd) {
      const timestampMatch = tripStartEnd.match(/(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})/);
      if (timestampMatch) {
        startTime = new Date(timestampMatch[1]);
      }
    }
    
    const tripData = {
      id: tripId,
      sensorId,
      distance,
      avgSpeed,
      avgSpeedWOS,
      maxSpeed,
      duration: parseDurationToSeconds(duration),
      stopTime: parseDurationToSeconds(stops),
      movingTime: parseDurationToSeconds(duration) - parseDurationToSeconds(stops),
      startTime,
      charge: trip['Charge(start | stop)']
    };
    
    trips.push(tripData);
    
    // Group by sensor
    if (!sensors[sensorId]) {
      sensors[sensorId] = {
        id: sensorId,
        trips: [],
        totalDistance: 0,
        totalTime: 0,
        avgSpeed: 0
      };
    }
    sensors[sensorId].trips.push(tripData);
    sensors[sensorId].totalDistance += distance;
    sensors[sensorId].totalTime += tripData.duration;
  });
  
  // Calculate sensor averages
  Object.values(sensors).forEach(sensor => {
    const totalSpeed = sensor.trips.reduce((sum, t) => sum + t.avgSpeed, 0);
    sensor.avgSpeed = totalSpeed / sensor.trips.length;
  });
  
  return { trips, sensors };
}

// Parse duration "MM:SS" to seconds
function parseDurationToSeconds(duration) {
  if (!duration) return 0;
  const [part1, part2] = duration.split(':').map(Number);
  return part1 * 60 + part2;
}

// Format seconds to readable string
function formatDuration(seconds) {
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  if (hours > 0) return `${hours}h ${minutes}m`;
  return `${minutes}m ${seconds % 60}s`;
}

// Create speed distribution chart
function createSpeedChart(trips) {
  const ctx = document.getElementById('speedChart');
  if (!ctx) return;
  
  // Calculate speed distribution
  const speedRanges = {
    'Stopped (0-2)': 0,
    'Very Slow (2-5)': 0,
    'Slow (5-10)': 0,
    'Moderate (10-15)': 0,
    'Fast (15-20)': 0,
    'Very Fast (20-25)': 0,
    'Extreme (25+)': 0
  };
  
  trips.forEach(trip => {
    const speed = trip.avgSpeed;
    if (speed < 2) speedRanges['Stopped (0-2)']++;
    else if (speed < 5) speedRanges['Very Slow (2-5)']++;
    else if (speed < 10) speedRanges['Slow (5-10)']++;
    else if (speed < 15) speedRanges['Moderate (10-15)']++;
    else if (speed < 20) speedRanges['Fast (15-20)']++;
    else if (speed < 25) speedRanges['Very Fast (20-25)']++;
    else speedRanges['Extreme (25+)']++;
  });
  
  if (analyticsCharts.speedChart) {
    analyticsCharts.speedChart.destroy();
  }
  
  analyticsCharts.speedChart = new Chart(ctx, {
    type: 'pie',
    data: {
      labels: Object.keys(speedRanges),
      datasets: [{
        data: Object.values(speedRanges),
        backgroundColor: [
          '#808080', '#DC2626', '#F97316', '#FACC15',
          '#22C55E', '#3B82F6', '#6366F1'
        ]
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { position: 'bottom' }
      }
    }
  });
}

// Create stop vs move chart
function createStopMoveChart(trips) {
  const ctx = document.getElementById('stopMoveChart');
  if (!ctx) return;
  
  const totalMoving = trips.reduce((sum, t) => sum + t.movingTime, 0);
  const totalStopped = trips.reduce((sum, t) => sum + t.stopTime, 0);
  
  if (analyticsCharts.stopMoveChart) {
    analyticsCharts.stopMoveChart.destroy();
  }
  
  analyticsCharts.stopMoveChart = new Chart(ctx, {
    type: 'doughnut',
    data: {
      labels: ['Moving', 'Stopped'],
      datasets: [{
        data: [totalMoving / 60, totalStopped / 60], // Convert to minutes
        backgroundColor: ['#22C55E', '#DC2626']
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { position: 'bottom' },
        tooltip: {
          callbacks: {
            label: function(context) {
              const label = context.label || '';
              const value = context.parsed;
              const hours = Math.floor(value / 60);
              const mins = Math.round(value % 60);
              return `${label}: ${hours}h ${mins}m`;
            }
          }
        }
      }
    }
  });
}

// Create time of day chart
function createTimeOfDayChart(trips) {
  const ctx = document.getElementById('timeOfDayChart');
  if (!ctx) return;
  
  const timeSlots = {
    '0-6': { trips: 0, totalSpeed: 0 },
    '6-9': { trips: 0, totalSpeed: 0 },
    '9-12': { trips: 0, totalSpeed: 0 },
    '12-15': { trips: 0, totalSpeed: 0 },
    '15-18': { trips: 0, totalSpeed: 0 },
    '18-21': { trips: 0, totalSpeed: 0 },
    '21-24': { trips: 0, totalSpeed: 0 }
  };
  
  trips.forEach(trip => {
    if (!trip.startTime) return;
    const hour = trip.startTime.getHours();
    
    let slot;
    if (hour < 6) slot = '0-6';
    else if (hour < 9) slot = '6-9';
    else if (hour < 12) slot = '9-12';
    else if (hour < 15) slot = '12-15';
    else if (hour < 18) slot = '15-18';
    else if (hour < 21) slot = '18-21';
    else slot = '21-24';
    
    timeSlots[slot].trips++;
    timeSlots[slot].totalSpeed += trip.avgSpeed;
  });
  
  const labels = Object.keys(timeSlots);
  const tripCounts = labels.map(slot => timeSlots[slot].trips);
  const avgSpeeds = labels.map(slot => 
    timeSlots[slot].trips > 0 ? timeSlots[slot].totalSpeed / timeSlots[slot].trips : 0
  );
  
  if (analyticsCharts.timeOfDayChart) {
    analyticsCharts.timeOfDayChart.destroy();
  }
  
  analyticsCharts.timeOfDayChart = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: labels.map(l => l + 'h'),
      datasets: [{
        label: 'Number of Trips',
        data: tripCounts,
        backgroundColor: '#3B82F6',
        yAxisID: 'y'
      }, {
        label: 'Avg Speed (km/h)',
        data: avgSpeeds,
        backgroundColor: '#22C55E',
        yAxisID: 'y1'
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        y: {
          type: 'linear',
          position: 'left',
          title: { display: true, text: 'Trips' }
        },
        y1: {
          type: 'linear',
          position: 'right',
          title: { display: true, text: 'Speed (km/h)' },
          grid: { drawOnChartArea: false }
        }
      }
    }
  });
}

// Create day of week chart
function createDayOfWeekChart(trips) {
  const ctx = document.getElementById('dayOfWeekChart');
  if (!ctx) return;
  
  const days = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
  const dayCounts = [0, 0, 0, 0, 0, 0, 0];
  
  trips.forEach(trip => {
    if (trip.startTime) {
      dayCounts[trip.startTime.getDay()]++;
    }
  });
  
  if (analyticsCharts.dayOfWeekChart) {
    analyticsCharts.dayOfWeekChart.destroy();
  }
  
  analyticsCharts.dayOfWeekChart = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: days,
      datasets: [{
        label: 'Trips per Day',
        data: dayCounts,
        backgroundColor: '#E67E22'
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false }
      }
    }
  });
}

// Update summary cards
function updateSummaryCards(trips) {
  const totalSpeed = trips.reduce((sum, t) => sum + t.avgSpeed, 0);
  const avgSpeed = totalSpeed / trips.length;
  const maxSpeed = Math.max(...trips.map(t => t.maxSpeed));
  
  const totalDistance = trips.reduce((sum, t) => sum + t.distance, 0);
  const avgDistance = totalDistance / trips.length;
  
  const totalDuration = trips.reduce((sum, t) => sum + t.duration, 0);
  const avgDuration = totalDuration / trips.length;
  
  document.getElementById('avgSpeedCard').textContent = avgSpeed.toFixed(1) + ' km/h';
  document.getElementById('maxSpeedCard').textContent = maxSpeed.toFixed(1) + ' km/h';
  document.getElementById('avgDistanceCard').textContent = avgDistance.toFixed(2) + ' km';
  document.getElementById('avgDurationCard').textContent = formatDuration(avgDuration);
}

// Create sensor cards
function createSensorCards(sensors) {
  const container = document.getElementById('sensorCards');
  if (!container) return;
  
  container.innerHTML = '';
  
  Object.values(sensors).forEach(sensor => {
    const card = document.createElement('div');
    card.className = 'sensor-card';
    card.innerHTML = `
      <div class="sensor-card-title">Sensor ${sensor.id}</div>
      <div class="sensor-stat">
        <span>Total Trips:</span>
        <strong>${sensor.trips.length}</strong>
      </div>
      <div class="sensor-stat">
        <span>Total Distance:</span>
        <strong>${sensor.totalDistance.toFixed(2)} km</strong>
      </div>
      <div class="sensor-stat">
        <span>Average Speed:</span>
        <strong>${sensor.avgSpeed.toFixed(1)} km/h</strong>
      </div>
      <div class="sensor-stat">
        <span>Total Time:</span>
        <strong>${formatDuration(sensor.totalTime)}</strong>
      </div>
    `;
    
    card.addEventListener('click', () => {
      console.log('Filter by sensor:', sensor.id);
      // You can add filtering logic here to show only this sensor's routes on map
    });
    
    container.appendChild(card);
  });
}

// Create trip details list
function createTripsList(trips) {
  const container = document.getElementById('tripsList');
  if (!container) return;
  
  container.innerHTML = '<div style="margin-bottom: 15px;"><strong>All Trips</strong></div>';
  
  trips.sort((a, b) => (b.startTime || 0) - (a.startTime || 0)).forEach(trip => {
    const card = document.createElement('div');
    card.className = 'sensor-card';
    card.innerHTML = `
      <div class="sensor-card-title">${trip.id.replace(/_/g, ' ')}</div>
      <div class="sensor-stat">
        <span>Date:</span>
        <strong>${trip.startTime ? trip.startTime.toLocaleDateString() : 'Unknown'}</strong>
      </div>
      <div class="sensor-stat">
        <span>Time:</span>
        <strong>${trip.startTime ? trip.startTime.toLocaleTimeString() : 'Unknown'}</strong>
      </div>
      <div class="sensor-stat">
        <span>Distance:</span>
        <strong>${trip.distance.toFixed(2)} km</strong>
      </div>
      <div class="sensor-stat">
        <span>Avg Speed:</span>
        <strong>${trip.avgSpeed.toFixed(1)} km/h</strong>
      </div>
      <div class="sensor-stat">
        <span>Max Speed:</span>
        <strong>${trip.maxSpeed.toFixed(1)} km/h</strong>
      </div>
      <div class="sensor-stat">
        <span>Duration:</span>
        <strong>${formatDuration(trip.duration)}</strong>
      </div>
    `;
    container.appendChild(card);
  });
}

// Main function to update all analytics
export function updateAnalytics(metadata) {
  if (!metadata) {
    console.warn('No metadata provided for analytics');
    return;
  }
  
  analyticsData = processMetadataForAnalytics(metadata);
  if (!analyticsData) return;
  
  const { trips, sensors } = analyticsData;
  
  console.log('Analytics data processed:', { tripCount: trips.length, sensorCount: Object.keys(sensors).length });
  
  // Update all visualizations
  updateSummaryCards(trips);
  createSpeedChart(trips);
  createStopMoveChart(trips);
  createTimeOfDayChart(trips);
  createDayOfWeekChart(trips);
  createSensorCards(sensors);
  createTripsList(trips);
}

// Initialize when DOM is ready
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initAnalytics);
} else {
  initAnalytics();
}

// Export for use in main app
window.updateAnalytics = updateAnalytics;