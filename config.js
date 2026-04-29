// config.js
export const CONFIG = {
  MAP_STYLE: 'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json',
  MAP_CENTER: [4.9041, 52.3676], // Amsterdam
  MAP_ZOOM: 13,

  // Supabase Edge Function — serves live GeoJSON for new API-fetched trips.
  // Replace <PROJECT_REF> with your Supabase project ref.
  // If not yet deployed, leave as-is — the map will fall back to local files only.
  TRIPS_API_URL: 'https://<PROJECT_REF>.supabase.co/functions/v1/trips-geojson',

  DATA_URL: './'
};

window.CONFIG = CONFIG;
