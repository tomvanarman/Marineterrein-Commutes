// config.js
export const CONFIG = {
  MAP_STYLE: 'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json',
  MAP_CENTER: [4.9041, 52.3676],
  MAP_ZOOM: 13,

  // Static file written by the GitHub Action and served by GitHub Pages.
  // No Supabase Edge Function needed — just a file committed to the repo root.
  TRIPS_GEOJSON_URL: './trips.geojson',

  // Match the INITIAL_DAYS in generate_trips_geojson.py so the
  // client-side filter aligns with what's actually in the file.
  INITIAL_DAYS: 90,

  DATA_URL: './',
};

window.CONFIG = CONFIG;
