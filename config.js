// config.js
export const CONFIG = {
  MAP_STYLE: 'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json',
  MAP_CENTER: [4.9041, 52.3676], // Amsterdam
  MAP_ZOOM: 13,

  // Supabase Edge Function — serves live GeoJSON, no PMTiles needed
  // Replace <PROJECT_REF> with your Supabase project ref
  TRIPS_API_URL: 'https://<PROJECT_REF>.supabase.co/functions/v1/trips-geojson',

  // Optional: load only trips from the last N days on startup (keeps initial load fast)
  INITIAL_DAYS: 90,

  DATA_URL: './'
};

window.CONFIG = CONFIG;
