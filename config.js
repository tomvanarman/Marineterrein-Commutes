// config.js
export const CONFIG = {
  MAP_STYLE: 'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json',
  MAP_CENTER: [4.9041, 52.3676], // Amsterdam
  MAP_ZOOM: 13,
  PMTILES_URL: './trips.pmtiles',        
  DATA_URL: './sensor_data'  
};

// Make available globally
window.CONFIG = CONFIG;