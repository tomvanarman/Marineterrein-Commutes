import json
import math
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
from road_quality_calculator import calculate_road_quality

# Configuration
DEFAULT_WHEEL_DIAMETER_MM = 711  # 26 inches - fallback only
SAMPLE_RATE_HZ = 50
SECONDS_PER_SAMPLE = 1 / SAMPLE_RATE_HZ  # 0.02 seconds

INPUT_ROOT = "sensor_data"
OUTPUT_ROOT = "processed_sensor_data"

# Braking detection: flag a segment when deceleration exceeds this threshold.
# Units: km/h lost per second. 5 = firm intentional braking; lower = more sensitive.
BRAKING_DECEL_THRESHOLD_KMH_S = 5.0

# Braking sanity caps
# Hard ceiling on deceleration rate — anything above this is a data artefact.
BRAKING_INTENSITY_CAP_KMH_S = 50

# If speed changes by more than this between consecutive segments, the gap is
# treated as a discontinuity: braking is skipped and prev_speed_kmh is reset.
SPEED_JUMP_THRESHOLD_KMH = 20

# Minimum plausible time for a segment (one sample interval).
# Segments shorter than this produce unreliable deceleration estimates.
MIN_SEGMENT_TIME_S = SECONDS_PER_SAMPLE  # 0.02 s

# Minimum wheel-rotation ticks for a segment to be used in braking detection
# (CSV path only — API trips use GPS speed and set hrot_diff=0 by design).
# hrot_diff=1 is a single half-rotation tick: timing noise at this resolution
# produces large apparent speed swings that are not real braking events.
MIN_HROT_FOR_BRAKING = 2

# Minimum accelerometer-sample span for a segment to be used in braking
# detection (API/GPS path only — the equivalent of MIN_HROT_FOR_BRAKING above).
# A single-sample segment (sample_diff=1, ~0.02s) is well below GPS update
# resolution: point-to-point GPS jitter over that short a window produces
# apparent decelerations of hundreds of km/h/s that are not real braking.
# Set to match GPS_SMOOTHING_WINDOW (defined below) so the time base backing
# a braking read is at least as wide as the window already used to smooth
# GPS speed for display.
MIN_SAMPLES_FOR_BRAKING = 5

# ─── Display-only smoothing for API/GPS-sourced trips ───────────────────────
# Raw point-to-point GPS speed is noisy (urban multipath, satellite geometry).
# GPS_SMOOTHING_WINDOW controls a centered moving average over this many
# consecutive raw points, used ONLY to compute Speed_display (map color).
# It never touches `Speed` (the raw value used for braking, stats, exports)
# and it never touches local CSV / wheel-rotation trips, which are already
# a physically-integrated measurement and don't need this smoothing.
# Bigger window = smoother-looking color, less responsive to real speed changes.
GPS_SMOOTHING_WINDOW = 5

# ─── Crash / fall detection ──────────────────────────────────────────────────
# A candidate impact becomes a confirmed crash when:
#   Local/manual trip:
#     1. |Acc Y| >= 6g
#     2. acceleration settles after the impact
#     3. wheel rotation remains stopped for at least 2 continuous seconds
#        before resuming (or remains stopped for the rest of the trip)
#
#   API-rendered trip:
#     1. |Acc Y| >= 6g
#     2. acceleration settles after the impact
#     3. GPS speed remains at/under the stop threshold for at least 2 seconds
#        because reconstructed API trips do not reliably contain HRot.
#
# GPS speed is still used as additional corroboration on both paths.
# The frontend receives only confirmed crash Point features (event_type='crash').

CRASH_IMPACT_G = 6.0
CRASH_SETTLE_G = 1.5
CRASH_SETTLE_WINDOW_SAMPLES = 100       # 2.0s at 50 Hz
CRASH_MIN_GAP_SAMPLES = 150             # ~3s to split separate impacts
CRASH_SPEED_CONFIRM_KMH = 6.0           # corroborating post-impact speed ceiling

# Local/manual CSV path: wheel must stop for at least 2 continuous seconds.
CRASH_RESUME_HROT_TICKS = 4
CRASH_MIN_STOP_GAP_SAMPLES = 100        # ~2.0s at 50 Hz

# API/GPS path: with no reliable HRot, require near-zero GPS speed for 2 seconds.
CRASH_API_STOP_SPEED_KMH = 1.5
CRASH_API_STOP_WINDOW_SAMPLES = 100      # 2.0s at 50 Hz

# Severity buckets for the map legend.
CRASH_SEVERITY_MINOR_MAX_G = 8.0
CRASH_SEVERITY_HARD_MAX_G = 11.0

# Crash type buckets, keyed off estimated pre-impact speed (km/h).
CRASH_STATIONARY_MAX_KMH = 1.0      # <= this -> Stationary Fall
CRASH_LOW_SPEED_MAX_KMH = 10.0      # >1 to <=10 -> Low-Speed Fall; >10 -> Moving Crash

# ─── Processing-version cache invalidation ────────────────────────────────────
# A trip's *_processed.geojson is only regenerated when this version differs
# from the version recorded for it in PROCESSING_VERSION_FILE (below). Bump
# PROCESSING_VERSION any time crash detection, braking detection, road-quality
# mapping, or any other logic in process_geojson_file() changes — that forces
# exactly one full reprocess of every trip on the next run, after which only
# newly-fetched trips are processed until the version changes again.
#
# This replaces blindly reprocessing everything every run (correct, but
# throws away all caching and gets slower forever) and blindly skipping
# anything already on disk (fast, but silently freezes trips at whatever
# logic produced them — the bug that let a simulated crash go undetected
# because the trip predated crash detection and was never recognised as
# stale).
PROCESSING_VERSION = 2
PROCESSING_VERSION_FILE = Path("processing_versions.json")

# Manual escape hatch: set True to force a full rebuild regardless of version
# (e.g. to sanity-check current logic against everything on disk). Leave False
# normally — bump PROCESSING_VERSION instead, so the rebuild is tracked and
# only happens once.
FORCE_REPROCESS_ALL = False


def load_processing_versions():
    """Load {trip_id: version} map of the processing version each trip was
    last generated with."""
    if PROCESSING_VERSION_FILE.exists():
        try:
            with open(PROCESSING_VERSION_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"⚠️ Could not load {PROCESSING_VERSION_FILE.name}: {e}")
    return {}


def save_processing_versions(versions):
    """Persist {trip_id: version} map."""
    try:
        with open(PROCESSING_VERSION_FILE, 'w') as f:
            json.dump(versions, f, indent=2)
    except Exception as e:
        print(f"⚠️ Could not save {PROCESSING_VERSION_FILE.name}: {e}")

assert MIN_SAMPLES_FOR_BRAKING == GPS_SMOOTHING_WINDOW, (
    "MIN_SAMPLES_FOR_BRAKING is meant to track GPS_SMOOTHING_WINDOW — "
    "update both together if you change one."
)

SKIP_TRIPS = {
    "602CD": ["Trip1"],
    "604F0": ["Trip1"]
}


def load_metadata():
    """Load existing metadata file if it exists - READ ONLY"""
    meta_file = Path("trips_metadata.json")
    if meta_file.exists():
        try:
            with open(meta_file, 'r') as f:
                metadata = json.load(f)
            print(f"📖 Loaded metadata for {len(metadata)} trips (read-only)")
            return metadata
        except Exception as e:
            print(f"⚠️ Could not load metadata file: {e}")
    return {}


def is_api_trip(trip_id, saved_metadata):
    """Return True if this trip was fetched from the Supabase API."""
    entry = saved_metadata.get(trip_id, {})
    return entry.get("source") == "api"


def parse_time(time_str, milliseconds):
    """Parse HH:mm:ss and SSS into datetime"""
    if not time_str or not milliseconds:
        return None
    try:
        base_time = datetime.strptime(str(time_str), "%H:%M:%S")
        return base_time + timedelta(milliseconds=int(milliseconds))
    except:
        return None


def safe_int(value, default=0):
    """Convert value to int"""
    if value is None or value == '':
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        try:
            if isinstance(value, str) and '-' in value:
                dt = datetime.fromisoformat(value.strip())
                return int(dt.timestamp() * 1000)
            return default
        except:
            return default


def safe_float(value, default=0.0):
    """Convert value to float"""
    if value is None or value == '':
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def haversine_distance(lon1, lat1, lon2, lat2):
    """Calculate distance between two points in meters"""
    if not all([lon1, lat1, lon2, lat2]):
        return 0
    R = 6371000
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    a = math.sin(delta_phi / 2) ** 2 + \
        math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def extract_metadata_and_features(data):
    """Separate metadata (features without coordinates) from actual features"""
    features = []
    metadata = {}
    important_keys = {
        'WheelDiam', 'Wheel mm', 'Frequency', 'GNSS', 'SENSOR',
        'Trip stop code', 'Trip start/end', 'Duration', 'Charge(start | stop)',
        'Hardware', 'Firmware', 'SystemID', 'App version',
        'BLE Device Information Service', 'Sensor\'s connection',
        ',Duration,Stops,Dist km,AVG km/h,AVGWOS km/h,MAX km/h,MAX- m/s²,MAX+ m/s²,Falls,Bamps,Elevation m'
    }
    for feat in data.get("features", []):
        geom = feat.get("geometry", {})
        coords = geom.get("coordinates", None)
        if coords is None or (isinstance(coords, list) and len(coords) == 0):
            props = feat.get("properties", {})
            for key, value in props.items():
                if key in important_keys or (not key.startswith(',,') and len(key) < 100):
                    metadata[key] = value
        else:
            features.append(feat)

    if not metadata and 'properties' in data:
        top_props = data.get('properties', {})
        for key, value in top_props.items():
            if key in important_keys or (not key.startswith(',,') and len(key) < 100):
                metadata[key] = value

    return features, metadata


def get_wheel_diameter(trip_id, file_metadata, saved_metadata):
    """Get wheel diameter from file metadata or saved metadata, in mm"""
    def parse_wheel_diameter(value):
        if not value:
            return None
        if isinstance(value, str):
            value = value.strip(', ')
            parts = value.split()
            if parts:
                try:
                    diameter_inches = float(parts[0])
                    return diameter_inches * 25.4
                except (ValueError, IndexError):
                    pass
        try:
            return float(value)
        except (ValueError, TypeError):
            pass
        return None

    if file_metadata:
        wheel_value = file_metadata.get('WheelDiam') or file_metadata.get('Wheel mm')
        diameter = parse_wheel_diameter(wheel_value)
        if diameter:
            print(f"  ✓ Using wheel diameter from file metadata: {diameter:.1f}mm")
            return diameter

    if trip_id in saved_metadata:
        trip_meta = saved_metadata[trip_id]
        if isinstance(trip_meta, dict):
            wheel_value = trip_meta.get('WheelDiam') or trip_meta.get('Wheel mm')
            if not wheel_value and 'metadata' in trip_meta:
                wheel_value = trip_meta['metadata'].get('WheelDiam') or trip_meta['metadata'].get('Wheel mm')
            diameter = parse_wheel_diameter(wheel_value)
            if diameter:
                print(f"  ✓ Using wheel diameter from saved metadata: {diameter:.1f}mm")
                return diameter

    print(f"  ⚠️ Wheel diameter not found, using default: {DEFAULT_WHEEL_DIAMETER_MM}mm")
    return DEFAULT_WHEEL_DIAMETER_MM


def extract_acceleration_data(features):
    """Extract Y-axis acceleration data from features"""
    acc_y_values = []
    for feature in features:
        props = feature.get('properties', {})
        acc_y = (props.get('Acc Y (g)') or
                 props.get('Acc Y') or
                 props.get('AccY') or
                 props.get('acc_y'))
        if acc_y is not None:
            acc_y_values.append(safe_float(acc_y, 0.0))
        else:
            acc_y_values.append(0.0)
    return np.array(acc_y_values)


def extract_gps_speed_data(features):
    """Extract GPS speed in km/h from raw feature properties."""
    speed_values = []
    for feature in features:
        raw = feature.get('properties', {}).get('Speed GPS')
        if raw is None or raw == '':
            speed_values.append(np.nan)
        else:
            try:
                # Speed GPS is stored in m/s for API-rendered trips.
                speed_values.append(float(raw) * 3.6)
            except (ValueError, TypeError):
                speed_values.append(np.nan)
    return np.array(speed_values, dtype=float)


def extract_hrot_data(features):
    """Extract raw HRot Count for local/manual trips."""
    return np.array([
        safe_int(f.get('properties', {}).get('HRot Count'), 0)
        for f in features
    ], dtype=int)


def _forward_fill(arr):
    """Forward-fill NaNs while preserving leading NaNs."""
    out = np.asarray(arr, dtype=float).copy()
    last = np.nan
    for i in range(len(out)):
        if np.isnan(out[i]):
            out[i] = last
        else:
            last = out[i]
    return out


def classify_crash_severity(peak_g):
    """Classify peak absolute acceleration into the map's severity buckets."""
    mag = abs(float(peak_g))
    if mag < CRASH_SEVERITY_MINOR_MAX_G:
        return 'Minor'
    if mag < CRASH_SEVERITY_HARD_MAX_G:
        return 'Hard'
    return 'Severe'


def classify_crash_type(preimpact_speed_kmh):
    """Classify an incident by how fast the bike was moving immediately before impact."""
    if preimpact_speed_kmh is None or np.isnan(preimpact_speed_kmh):
        return 'Unclassified'
    if preimpact_speed_kmh <= CRASH_STATIONARY_MAX_KMH:
        return 'Stationary Fall'
    if preimpact_speed_kmh <= CRASH_LOW_SPEED_MAX_KMH:
        return 'Low-Speed Fall'
    return 'Moving Crash'


def classify_crash_outcome(unresolved, came_to_stop, recovery_time_s):
    """Classify the post-impact outcome independently from crash type."""
    if unresolved:
        return 'Unresolved'
    if came_to_stop and recovery_time_s is not None:
        return 'Resolved'
    return 'Unclassified'


def estimate_preimpact_wheel_speed(hrot_data, samples_seq, peak_idx, wheel_circumference_m):
    """Estimate wheel speed over the ~1 second immediately before the impact peak.

    Local/manual (CSV) path: derives speed from wheel rotation ticks, since
    that data is a physically-integrated measurement and doesn't need GPS.
    """
    if len(hrot_data) == 0 or peak_idx <= 0 or not samples_seq or peak_idx >= len(samples_seq):
        return None

    peak_sample = samples_seq[peak_idx]
    target_sample = peak_sample - int(SAMPLE_RATE_HZ)
    prior_idx = None
    for idx in range(peak_idx - 1, -1, -1):
        if samples_seq[idx] <= target_sample:
            prior_idx = idx
            break
    if prior_idx is None:
        prior_idx = 0

    sample_diff = samples_seq[peak_idx] - samples_seq[prior_idx]
    if sample_diff <= 0:
        return None

    hrot_diff = hrot_data[peak_idx] - hrot_data[prior_idx]
    if hrot_diff <= 0:
        return 0.0

    time_s = sample_diff * SECONDS_PER_SAMPLE
    if time_s <= 0:
        return None

    revolutions = hrot_diff / 2.0
    distance_m = revolutions * wheel_circumference_m
    speed_kmh = (distance_m / time_s) * 3.6
    return round(min(speed_kmh, 40.0), 1)


def estimate_preimpact_gps_speed(gps_speed_data, samples_seq, peak_idx):
    """GPS-based equivalent of estimate_preimpact_wheel_speed, for API trips.

    API-rendered trips have no reliable HRot, so instead of deriving speed
    from wheel ticks this averages the raw GPS speed (already in km/h) over
    the same ~1 second window immediately before the impact peak.
    """
    if gps_speed_data is None or len(gps_speed_data) == 0 or peak_idx <= 0 \
            or not samples_seq or peak_idx >= len(samples_seq):
        return None

    peak_sample = samples_seq[peak_idx]
    target_sample = peak_sample - int(SAMPLE_RATE_HZ)
    prior_idx = None
    for idx in range(peak_idx - 1, -1, -1):
        if samples_seq[idx] <= target_sample:
            prior_idx = idx
            break
    if prior_idx is None:
        prior_idx = 0

    window = np.asarray(gps_speed_data[prior_idx:peak_idx + 1], dtype=float)
    valid = window[~np.isnan(window)]
    if len(valid) == 0:
        return None

    return round(min(float(np.mean(valid)), 40.0), 1)


def analyze_crash_recovery(hrot_data, end_idx):
    """
    Local/manual trip recovery test.

    A confirmed stop requires at least CRASH_MIN_STOP_GAP_SAMPLES samples
    without enough HRot movement. If the wheel never resumes, it is marked
    unresolved only after the full 2-second observation window is available.
    """
    if hrot_data is None or end_idx >= len(hrot_data):
        return False, None, False

    baseline = hrot_data[end_idx]
    resume_idx = None

    for idx in range(end_idx + 1, len(hrot_data)):
        if hrot_data[idx] - baseline >= CRASH_RESUME_HROT_TICKS:
            resume_idx = idx
            break

    if resume_idx is None:
        stop_samples = len(hrot_data) - end_idx - 1
        if stop_samples >= CRASH_MIN_STOP_GAP_SAMPLES:
            return True, None, True
        return False, None, False

    gap = resume_idx - end_idx
    if gap >= CRASH_MIN_STOP_GAP_SAMPLES:
        return True, round(gap / SAMPLE_RATE_HZ, 1), False

    return False, None, False


def analyze_api_crash_recovery(speed_gps_data, end_idx):
    """
    API/GPS trip recovery test.

    API-rendered trips do not reliably carry HRot, so require forward-filled
    GPS speed to remain at/below the near-zero stop threshold for a full
    2-second window. This is deliberately stricter than merely requiring
    speed <= 6 km/h.
    """
    if speed_gps_data is None or end_idx >= len(speed_gps_data):
        return False, None, False

    speed_ffill = _forward_fill(speed_gps_data)
    post_lo = end_idx + 1
    post_hi = min(
        len(speed_ffill),
        post_lo + CRASH_API_STOP_WINDOW_SAMPLES
    )
    post = speed_ffill[post_lo:post_hi]

    if len(post) < CRASH_API_STOP_WINDOW_SAMPLES:
        return False, None, False

    valid = post[~np.isnan(post)]
    if len(valid) < CRASH_API_STOP_WINDOW_SAMPLES:
        return False, None, False

    if np.all(valid <= CRASH_API_STOP_SPEED_KMH):
        # Find the first point in the post-impact sequence at which the
        # 2-second stopped condition is satisfied.
        return True, round(len(post) / SAMPLE_RATE_HZ, 1), False

    return False, None, False


def detect_crash_events(
    acc_y_data,
    speed_gps_data,
    hrot_data=None,
    use_gps_recovery=False,
    impact_g=CRASH_IMPACT_G,
    settle_g=CRASH_SETTLE_G,
    settle_window=CRASH_SETTLE_WINDOW_SAMPLES,
    min_gap=CRASH_MIN_GAP_SAMPLES,
    speed_confirm_kmh=CRASH_SPEED_CONFIRM_KMH,
):
    """
    Detect confirmed crash/fall events from raw per-sample data.

    Local/manual trips use HRot to confirm a >=2s wheel stop.
    API-rendered trips use a >=2s near-zero GPS-speed stop because HRot is
    not reliably available in the reconstructed API data.

    Confirmation requires:
      - peak |Acc Y| >= impact_g
      - accelerometer settles after the impact
      - post-impact speed corroboration
      - path-specific 2-second stop confirmation
    """
    n = len(acc_y_data)
    if n == 0:
        return []

    flagged = np.where(np.abs(acc_y_data) >= impact_g)[0]
    if len(flagged) == 0:
        return []

    events = []
    start = flagged[0]
    prev = flagged[0]

    for idx in flagged[1:]:
        if idx - prev > min_gap:
            events.append((start, prev))
            start = idx
        prev = idx
    events.append((start, prev))

    speed_ffill = _forward_fill(speed_gps_data) if speed_gps_data is not None else None
    results = []

    for s, e in events:
        seg = acc_y_data[s:e + 1]
        peak_idx = s + int(np.argmax(np.abs(seg)))
        peak_g = float(acc_y_data[peak_idx])

        post_lo = e + 1
        post_hi = min(n, post_lo + settle_window)
        post = acc_y_data[post_lo:post_hi]
        settled = len(post) == settle_window and np.nanstd(post) < settle_g

        if speed_ffill is not None:
            post_speed = speed_ffill[post_lo:post_hi]
            valid = post_speed[~np.isnan(post_speed)]
            speed_ok = (
                len(valid) == 0 or
                (valid <= speed_confirm_kmh).mean() >= 0.7
            )
        else:
            speed_ok = True

        if use_gps_recovery:
            came_to_stop, recovery_time_s, unresolved = analyze_api_crash_recovery(
                speed_gps_data, e
            )
        else:
            came_to_stop, recovery_time_s, unresolved = analyze_crash_recovery(
                hrot_data, e
            )

        confirmed = bool(settled and speed_ok and came_to_stop)

        results.append({
            'start_idx': int(s),
            'end_idx': int(e),
            'peak_idx': int(peak_idx),
            'peak_g': round(peak_g, 2),
            'settled': bool(settled),
            'speed_low_after': bool(speed_ok),
            'came_to_stop': bool(came_to_stop),
            'recovery_time_s': recovery_time_s,
            'unresolved': bool(unresolved),
            'is_crash': confirmed,
        })

    return results


def map_road_quality_to_segments(points, road_quality_data):
    """Map road quality scores to segments based on sample indices."""
    if road_quality_data is None:
        return None

    quality_scores = road_quality_data['road_quality']
    time_windows = road_quality_data['time_windows']

    def get_quality_at_sample(sample_idx):
        if len(time_windows) == 0:
            return 0
        closest_idx = np.argmin(np.abs(time_windows - sample_idx))
        return int(quality_scores[closest_idx])

    return get_quality_at_sample


def calculate_braking_intensity(prev_speed_kmh, curr_speed_kmh, time_diff_s):
    """
    Return the deceleration rate (km/h per second) for a segment.

    prev_speed_kmh : speed at the START of the segment (km/h)
    curr_speed_kmh : speed at the END of the segment (km/h)
    time_diff_s    : elapsed time for the segment (seconds); must be >= MIN_SEGMENT_TIME_S

    Returns 0.0 when the bike is accelerating or holding speed.
    Returns a positive value (km/h / s) when decelerating, capped at
    BRAKING_INTENSITY_CAP_KMH_S to suppress data artefacts.
    """
    if time_diff_s is None or time_diff_s < MIN_SEGMENT_TIME_S:
        return 0.0

    delta = prev_speed_kmh - curr_speed_kmh  # positive = deceleration
    if delta <= 0:
        return 0.0

    raw = delta / time_diff_s
    # Hard cap: anything above this threshold is almost certainly a sensor/timing
    # artefact rather than real braking behaviour.
    return round(min(raw, BRAKING_INTENSITY_CAP_KMH_S), 2)


def moving_average_speeds(raw_speeds, window=GPS_SMOOTHING_WINDOW):
    """
    Centered moving average over a list of raw speeds (km/h).

    Display-only smoothing for GPS/API trips: reduces point-to-point noise
    from urban multipath / satellite geometry so the map's color rendering
    reads as continuous bands instead of speckled noise. Does NOT alter the
    underlying raw speed values — this is a separate output used only to
    color the map, never fed into braking detection, stats, or exports.
    """
    n = len(raw_speeds)
    if n == 0:
        return []
    half = window // 2
    smoothed = []
    for i in range(n):
        lo = max(0, i - half)
        hi = min(n, i + half + 1)
        window_vals = raw_speeds[lo:hi]
        smoothed.append(sum(window_vals) / len(window_vals))
    return smoothed


def process_geojson_file(filepath, trip_id, saved_metadata, debug=False):
    """Process a single GeoJSON file: clean, calculate speeds, add road quality.

    For API-sourced trips (source == 'api' in trips_metadata.json), speed is
    taken directly from the 'Speed GPS' property (m/s → km/h) since wheel
    rotation data (HRot) is not reliably available via the reconstruction query.
    For local CSV trips, speed is calculated from wheel rotations as before.

    Every segment carries two speed properties:
      - Speed:         raw computed speed (unchanged). Used for braking
                        detection, trip stats, and any export/analysis.
      - Speed_display:  smoothed speed used only for map coloring. On the
                        CSV/wheel path this equals Speed (that data is
                        already a physically-integrated measurement and
                        needs no further smoothing). On the API/GPS path
                        this is a rolling average of raw per-point GPS
                        speed (see GPS_SMOOTHING_WINDOW) — noise reduction
                        for rendering only, raw data is untouched.

    Braking detection notes by path:
    - CSV path: requires hrot_diff >= MIN_HROT_FOR_BRAKING so that single-tick
      segments (whose timing noise mimics large decelerations) are excluded.
    - API path: hrot_diff is always 0 by design, so the hrot guard is not
      applied; braking is detected from raw GPS speed changes (Speed, not
      Speed_display) — smoothing is never allowed to mask or manufacture
      braking events.
    """
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)

        if 'features' not in data:
            return None, None

        # Determine data source for this trip
        use_gps_speed = is_api_trip(trip_id, saved_metadata)
        if use_gps_speed:
            print(f"  🌐 API trip — using GPS speed instead of wheel rotation")

        # Step 1: Extract features and metadata
        features, file_metadata = extract_metadata_and_features(data)
        if not features:
            return None, file_metadata

        # Get wheel diameter (still needed for API trips for metadata consistency)
        wheel_diameter_mm = get_wheel_diameter(trip_id, file_metadata, saved_metadata)
        wheel_circumference_m = (wheel_diameter_mm / 1000) * math.pi

        # Step 2: Extract acceleration data and calculate road quality
        print(f"  🛣️ Calculating road quality...")
        acc_y_data = extract_acceleration_data(features)

        # Crash/fall detection runs on the raw feature stream before trimming
        # and before speed segmentation, preserving full accelerometer resolution.
        gps_speed_data = extract_gps_speed_data(features)
        hrot_raw_data = extract_hrot_data(features)

        crash_events_raw = detect_crash_events(
            acc_y_data,
            gps_speed_data,
            hrot_raw_data,
            use_gps_recovery=use_gps_speed,
        )

        confirmed_crashes = [e for e in crash_events_raw if e['is_crash']]

        if confirmed_crashes:
            print(
                f"  🚨 Confirmed crash/fall events: {len(confirmed_crashes)} "
                f"({'API/GPS stop logic' if use_gps_speed else 'wheel-stop logic'})"
            )
        elif crash_events_raw:
            print(
                f"  ℹ️ {len(crash_events_raw)} impact candidate(s) rejected by "
                f"the strict crash confirmation rules"
            )

        crash_events_enriched = []
        samples_seq = [
            safe_int(f.get('properties', {}).get('Samples', 0), 0)
            for f in features
        ]

        for event in confirmed_crashes:
            suddenness_s = round(
                (event['peak_idx'] - event['start_idx']) / SAMPLE_RATE_HZ,
                2
            )
            came_to_stop = event['came_to_stop']
            recovery_time_s = event['recovery_time_s']
            unresolved = event['unresolved']

            # Pre-impact speed: wheel-tick based for local/CSV trips, GPS-average
            # based for API trips (no reliable HRot there — see the function's
            # docstring for why this can't just reuse the wheel-speed estimator).
            if use_gps_speed:
                preimpact_speed_kmh = estimate_preimpact_gps_speed(
                    gps_speed_data, samples_seq, event['peak_idx']
                )
            else:
                preimpact_speed_kmh = estimate_preimpact_wheel_speed(
                    hrot_raw_data, samples_seq, event['peak_idx'], wheel_circumference_m
                )

            crash_type = classify_crash_type(preimpact_speed_kmh)
            crash_outcome = classify_crash_outcome(unresolved, came_to_stop, recovery_time_s)

            crash_events_enriched.append({
                'sample_start': samples_seq[event['start_idx']],
                'sample_end': samples_seq[event['end_idx']],
                'peak_g': event['peak_g'],
                'severity': classify_crash_severity(event['peak_g']),
                'suddenness_s': suddenness_s,
                'came_to_stop': came_to_stop,
                'recovery_time_s': recovery_time_s,
                'unresolved': unresolved,
                'preimpact_speed_kmh': preimpact_speed_kmh,
                'crash_type': crash_type,
                'crash_outcome': crash_outcome,
            })

        road_quality_data = None
        if len(acc_y_data) > 200:
            try:
                road_quality_data = calculate_road_quality(
                    acc_y_data,
                    window_size=100,
                    overlap=0.5
                )
                print(f"  ✓ Road quality calculated for {len(road_quality_data['road_quality'])} windows")
            except Exception as e:
                print(f"  ⚠️ Road quality calculation failed: {e}")
        else:
            print(f"  ⚠️ Not enough acceleration data for road quality analysis")

        if debug:
            print(f"\n  DEBUG - Metadata extraction:")
            print(f"    Found {len(features)} features")
            print(f"    Acceleration data points: {len(acc_y_data)}")
            print(f"    Metadata keys: {list(file_metadata.keys()) if file_metadata else 'None'}")
            print(f"    Speed source: {'GPS (Speed GPS)' if use_gps_speed else 'Wheel rotation (HRot)'}")
            print(f"\n  DEBUG - Wheel configuration:")
            print(f"    Diameter: {wheel_diameter_mm}mm")
            print(f"    Circumference: {wheel_circumference_m:.3f}m")
            if road_quality_data:
                print(f"\n  DEBUG - Road quality:")
                print(f"    Unique scores: {np.unique(road_quality_data['road_quality'])}")
                print(f"    Score distribution: {np.bincount(road_quality_data['road_quality'], minlength=6)[1:]}")

        # Step 3: Extract and sort points
        points = []
        for idx, feature in enumerate(features):
            coords = feature['geometry']['coordinates']
            props = feature['properties']

            if len(coords) >= 2:
                lon, lat = coords[-1]
            else:
                continue

            if not lon or not lat or lon == 0 or lat == 0:
                continue

            samples_value = props.get('Samples', 0)
            samples_int = safe_int(samples_value, 0)

            # Speed GPS is in m/s from the DB — convert to km/h here
            raw_gps_speed = safe_float(props.get('Speed GPS'), 0.0)
            gps_speed_kmh = raw_gps_speed * 3.6 if raw_gps_speed else 0.0

            points.append({
                'lon': float(lon),
                'lat': float(lat),
                'marker': safe_int(props.get('marker', 0)),
                'samples': samples_int,
                'samples_raw': samples_value,
                'hrot': safe_int(props.get('HRot Count', 0)),
                'time': parse_time(props.get('HH:mm:ss'), props.get('SSS')),
                'time_str': props.get('HH:mm:ss'),
                'time_ms': props.get('SSS'),
                'original_speed': props.get('Speed'),
                'gps_speed_kmh': gps_speed_kmh,
                'idx': idx
            })

        points.sort(key=lambda p: p['samples'])

        if len(points) < 2:
            return None, file_metadata

        # Step 3b: Drop the first and last 100m (privacy / identifiability)
        TRIM_DISTANCE_METRES = 100
        cumulative_start_dist = 0.0
        start_trim_index = 0
        for k in range(1, len(points)):
            dist = haversine_distance(
                points[k - 1]['lon'], points[k - 1]['lat'],
                points[k]['lon'], points[k]['lat']
            )
            if dist > 500:
                continue
            cumulative_start_dist += dist
            if cumulative_start_dist >= TRIM_DISTANCE_METRES:
                start_trim_index = k
                break

        cumulative_end_dist = 0.0
        end_trim_index = len(points) - 1
        for k in range(len(points) - 1, 0, -1):
            dist = haversine_distance(
                points[k]['lon'], points[k]['lat'],
                points[k - 1]['lon'], points[k - 1]['lat']
            )
            if dist > 500:
                continue
            cumulative_end_dist += dist
            if cumulative_end_dist >= TRIM_DISTANCE_METRES:
                end_trim_index = k
                break

        # A confirmed crash whose sample range falls inside a would-be-trimmed
        # zone loses its anchor once that zone is cut — new_features (below)
        # only comes from surviving points, and a crash marker with no
        # overlapping segment is silently dropped, not just fuzzed. Skip the
        # trim on whichever end has a crash in it, so the crash keeps its
        # real location instead of disappearing. This does mean that end's
        # true GPS points are kept (not privacy-trimmed) whenever a crash
        # lands there — a deliberate trade of location privacy for crash
        # visibility on that specific trip end.
        crash_in_start_zone = any(
            c['sample_start'] < points[start_trim_index]['samples']
            for c in crash_events_enriched
        ) if start_trim_index < len(points) else False

        crash_in_end_zone = any(
            c['sample_end'] > points[end_trim_index]['samples']
            for c in crash_events_enriched
        ) if end_trim_index < len(points) else False

        effective_start_trim_index = 0 if crash_in_start_zone else start_trim_index
        effective_end_trim_index = (len(points) - 1) if crash_in_end_zone else end_trim_index

        if crash_in_start_zone:
            print(f"  ⚠️ Crash within first {TRIM_DISTANCE_METRES}m — keeping full start so it isn't lost")
        if crash_in_end_zone:
            print(f"  ⚠️ Crash within last {TRIM_DISTANCE_METRES}m — keeping full end so it isn't lost")

        if effective_start_trim_index < effective_end_trim_index:
            points = points[effective_start_trim_index:effective_end_trim_index]
            print(f"  ✂️ Trimmed: Start {TRIM_DISTANCE_METRES}m, End {TRIM_DISTANCE_METRES}m")
            print(f"  Remaining points: {len(points)}")
        else:
            print("  ⚠️ Trip too short to trim both ends. Keeping original.")

        if len(points) < 2:
            return None, file_metadata

        # Step 3c: Precompute smoothed GPS speed for display (API path only)
        if use_gps_speed:
            smoothed = moving_average_speeds([p['gps_speed_kmh'] for p in points])
            for p, s in zip(points, smoothed):
                p['gps_speed_kmh_display'] = s

        # Step 4: Create road quality lookup
        quality_lookup = map_road_quality_to_segments(points, road_quality_data)

        # Step 5: Create line segments
        new_features = []

        if use_gps_speed:
            # ── API path: one segment per consecutive point pair, GPS speed ──────
            # hrot_diff is 0 for all API segments by design; braking is detected
            # from GPS speed changes. MIN_HROT_FOR_BRAKING itself doesn't apply
            # here (it counts wheel ticks), but MIN_SAMPLES_FOR_BRAKING is its
            # GPS-path equivalent: it rejects segments too short to distinguish
            # real braking from point-to-point GPS jitter (see constant above).
            prev_speed_kmh = None
            for i in range(len(points) - 1):
                start_point = points[i]
                end_point = points[i + 1]

                gps_distance = haversine_distance(
                    start_point['lon'], start_point['lat'],
                    end_point['lon'], end_point['lat']
                )

                # Skip GPS jumps — also marks a discontinuity in the speed series.
                if gps_distance > 1000:
                    prev_speed_kmh = None
                    continue

                # Average GPS speed of the two endpoints, capped at 40 km/h
                # (raw — this drives braking detection and stats, unchanged)
                speed_kmh = (start_point['gps_speed_kmh'] + end_point['gps_speed_kmh']) / 2
                speed_kmh = min(speed_kmh, 40)

                # Smoothed average for display/coloring only
                speed_display_kmh = (
                    start_point['gps_speed_kmh_display'] + end_point['gps_speed_kmh_display']
                ) / 2
                speed_display_kmh = min(speed_display_kmh, 40)

                if start_point['lon'] == end_point['lon'] and start_point['lat'] == end_point['lat']:
                    prev_speed_kmh = speed_kmh
                    continue

                midpoint_sample = (start_point['samples'] + end_point['samples']) // 2
                road_quality = quality_lookup(midpoint_sample) if quality_lookup else 0

                # Estimate time from sample count (API trips lack real wall-clock time)
                sample_diff = end_point['samples'] - start_point['samples']
                est_time_s = sample_diff * SECONDS_PER_SAMPLE if sample_diff > 0 else None

                # Braking: guard against implausible speed jumps and bad time estimates.
                # Always computed from raw Speed, never from Speed_display.
                braking_intensity = 0.0
                if (
                    prev_speed_kmh is not None
                    and est_time_s is not None
                    and est_time_s >= MIN_SEGMENT_TIME_S
                    and sample_diff is not None
                    and sample_diff >= MIN_SAMPLES_FOR_BRAKING
                    and abs(speed_kmh - prev_speed_kmh) <= SPEED_JUMP_THRESHOLD_KMH
                ):
                    braking_intensity = calculate_braking_intensity(
                        prev_speed_kmh, speed_kmh, est_time_s
                    )
                elif prev_speed_kmh is not None and abs(speed_kmh - prev_speed_kmh) > SPEED_JUMP_THRESHOLD_KMH:
                    # Speed jumped implausibly — treat as a discontinuity.
                    prev_speed_kmh = None

                new_features.append({
                    'type': 'Feature',
                    'geometry': {
                        'type': 'LineString',
                        'coordinates': [
                            [start_point['lon'], start_point['lat']],
                            [end_point['lon'], end_point['lat']]
                        ]
                    },
                    'properties': {
                        'Speed': round(speed_kmh, 1),
                        'Speed_display': round(speed_display_kmh, 1),
                        'road_quality': road_quality,
                        'marker': start_point['marker'],
                        'trip_id': trip_id,
                        'hrot_diff': 0,
                        'sample_diff': sample_diff,
                        'time_diff_s': round(est_time_s, 3) if est_time_s else None,
                        'gps_distance_m': round(gps_distance, 1),
                        'original_speed': start_point['original_speed'],
                        'wheel_diameter_mm': wheel_diameter_mm,
                        'braking_intensity': braking_intensity,
                        'is_braking': braking_intensity >= BRAKING_DECEL_THRESHOLD_KMH_S,
                        'is_crash': False,
                        'crash_intensity_g': 0.0,
                        'segment_start_sample': start_point['samples'],
                        'segment_end_sample': end_point['samples'],
                        'time_str': start_point['time_str'],
                    }
                })

                prev_speed_kmh = speed_kmh

        else:
            # ── Local CSV path: wheel-rotation-based speed ───────────────────────
            # Braking requires hrot_diff >= MIN_HROT_FOR_BRAKING because a single
            # half-rotation tick (hrot_diff=1) has too little timing resolution:
            # the apparent speed swing between adjacent single-tick segments easily
            # exceeds the braking threshold even at steady cruising speed.
            prev_speed_kmh = None
            i = 0
            while i < len(points) - 1:
                start_point = points[i]
                j = i + 1
                while j < len(points) and points[j]['hrot'] == start_point['hrot']:
                    j += 1

                if j >= len(points):
                    break

                end_point = points[j]

                if start_point['time'] and end_point['time']:
                    time_diff_seconds = (end_point['time'] - start_point['time']).total_seconds()
                else:
                    sample_diff = end_point['samples'] - start_point['samples']
                    time_diff_seconds = sample_diff * SECONDS_PER_SAMPLE

                if time_diff_seconds <= 0 or time_diff_seconds > 600:
                    i = j
                    continue

                # Reject segments too short to yield a reliable deceleration estimate.
                if time_diff_seconds < MIN_SEGMENT_TIME_S:
                    prev_speed_kmh = None
                    i = j
                    continue

                hrot_diff = end_point['hrot'] - start_point['hrot']

                if hrot_diff > 0 and time_diff_seconds > 0:
                    revolutions = hrot_diff / 2.0
                    distance_m = revolutions * wheel_circumference_m
                    speed_ms = distance_m / time_diff_seconds
                    speed_kmh = speed_ms * 3.6
                else:
                    speed_kmh = 0

                gps_distance = haversine_distance(
                    start_point['lon'], start_point['lat'],
                    end_point['lon'], end_point['lat']
                )

                # GPS jump — treat as a discontinuity in the speed series.
                if gps_distance > 1000:
                    prev_speed_kmh = None
                    i = j
                    continue

                if speed_kmh > 40:
                    speed_kmh = 40

                midpoint_sample = (start_point['samples'] + end_point['samples']) // 2
                road_quality = quality_lookup(midpoint_sample) if quality_lookup else 0

                # Braking: only calculate when hrot_diff is large enough to give a
                # reliable speed baseline, and when the speed change is plausible.
                braking_intensity = 0.0
                if prev_speed_kmh is not None and hrot_diff >= MIN_HROT_FOR_BRAKING:
                    if abs(speed_kmh - prev_speed_kmh) > SPEED_JUMP_THRESHOLD_KMH:
                        # Implausible jump — reset series and skip this segment.
                        prev_speed_kmh = speed_kmh
                        i = j
                        continue
                    braking_intensity = calculate_braking_intensity(
                        prev_speed_kmh, speed_kmh, time_diff_seconds
                    )
                elif prev_speed_kmh is not None and abs(speed_kmh - prev_speed_kmh) > SPEED_JUMP_THRESHOLD_KMH:
                    # hrot_diff too small AND speed jumped — reset to avoid poisoning
                    # the next comparison.
                    prev_speed_kmh = speed_kmh
                    i = j
                    continue

                if (start_point['lon'] != end_point['lon'] or
                        start_point['lat'] != end_point['lat']) and speed_kmh < 100:
                    new_features.append({
                        'type': 'Feature',
                        'geometry': {
                            'type': 'LineString',
                            'coordinates': [
                                [start_point['lon'], start_point['lat']],
                                [end_point['lon'], end_point['lat']]
                            ]
                        },
                        'properties': {
                            'Speed': round(speed_kmh, 1),
                            # CSV/wheel-rotation speed is already a physically
                            # integrated measurement — no further smoothing needed.
                            # Kept equal to Speed so app.js can always read
                            # Speed_display for coloring regardless of trip source.
                            'Speed_display': round(speed_kmh, 1),
                            'road_quality': road_quality,
                            'marker': start_point['marker'],
                            'trip_id': trip_id,
                            'hrot_diff': hrot_diff,
                            'sample_diff': end_point['samples'] - start_point['samples'],
                            'time_diff_s': round(time_diff_seconds, 3),
                            'gps_distance_m': round(gps_distance, 1),
                            'original_speed': start_point['original_speed'],
                            'wheel_diameter_mm': wheel_diameter_mm,
                            'braking_intensity': braking_intensity,
                            'is_braking': braking_intensity >= BRAKING_DECEL_THRESHOLD_KMH_S,
                        }
                    })
                    prev_speed_kmh = speed_kmh
                else:
                    # Segment skipped but still update rolling speed to avoid
                    # false braking signals across a gap.
                    prev_speed_kmh = speed_kmh

                i = j

        if not new_features:
            return None, file_metadata

        if quality_lookup:
            qualities = [f['properties']['road_quality'] for f in new_features]
            quality_counts = np.bincount(qualities, minlength=6)[1:]
            print(f"  📊 Road quality distribution: {dict(enumerate(quality_counts, 1))}")

        braking_count = sum(1 for f in new_features if f['properties'].get('is_braking'))
        if braking_count:
            print(f"  🛑 Braking events detected: {braking_count}")

        # Tag output segments overlapping each confirmed crash and emit one
        # standalone Point feature per crash for the map crash layer.
        crash_point_features = []
        tagged_crashes = 0

        for crash in crash_events_enriched:
            crash_start = crash['sample_start']
            crash_end = crash['sample_end']
            anchor_feat = None

            for feat in new_features:
                props = feat['properties']
                seg_start = props.get('segment_start_sample')
                seg_end = props.get('segment_end_sample')

                if seg_start is None or seg_end is None:
                    continue

                if seg_start <= crash_end and seg_end >= crash_start:
                    props['is_crash'] = True
                    props['crash_intensity_g'] = crash['peak_g']
                    props['crash_type'] = crash['crash_type']
                    props['crash_outcome'] = crash['crash_outcome']
                    if anchor_feat is None:
                        anchor_feat = feat

            if anchor_feat is not None:
                tagged_crashes += 1
                anchor_coords = anchor_feat['geometry']['coordinates'][0]
                anchor_props = anchor_feat['properties']

                crash_point_features.append({
                    'type': 'Feature',
                    'geometry': {
                        'type': 'Point',
                        'coordinates': anchor_coords
                    },
                    'properties': {
                        'event_type': 'crash',
                        'trip_id': trip_id,
                        'severity': crash['severity'],
                        'peak_g': crash['peak_g'],
                        'suddenness_s': crash['suddenness_s'],
                        'speed_at_impact_kmh': anchor_props.get('Speed', 0),
                        'came_to_stop': crash['came_to_stop'],
                        'recovery_time_s': crash['recovery_time_s'],
                        'unresolved': crash['unresolved'],
                        'crash_type': crash['crash_type'],
                        'crash_outcome': crash['crash_outcome'],
                        'time_str': anchor_props.get('time_str'),
                    }
                })

        if tagged_crashes:
            print(
                f"  🚨 Crash events mapped onto {tagged_crashes} output crash range(s), "
                f"{len(crash_point_features)} crash marker(s) emitted"
            )

        return {
            'type': 'FeatureCollection',
            'features': new_features + crash_point_features
        }, file_metadata

    except Exception as e:
        import traceback
        print(f"  ⚠️ Error processing {filepath.name}: {e}")
        if debug:
            print(f"  Traceback: {traceback.format_exc()}")
        return None, None


def process_all_trips(input_dir=INPUT_ROOT, output_dir=OUTPUT_ROOT):
    """Process all GeoJSON files in sensor data directory"""
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)

    if not input_path.exists():
        print(f"❌ Directory not found: {input_dir}")
        return

    saved_metadata = load_metadata()
    processing_versions = load_processing_versions()

    print("\n🚴 Processing Bike Trip Data with Road Quality")
    print("=" * 60)
    print(f"📂 Input: {input_path}")
    print(f"📂 Output: {output_path}")
    print(f"⚠️ NOTE: Metadata file is managed by csv_to_geojson_converter.py")
    print(f"🏷️  Processing version: {PROCESSING_VERSION}"
          + (" (FORCE_REPROCESS_ALL is on — ignoring cache)" if FORCE_REPROCESS_ALL else ""))

    total_files = 0
    processed_files = 0
    skipped_files = 0
    already_processed = 0
    failed_files = 0
    total_segments = 0

    # processing_versions is only written once after the full loop (in the
    # finally below), not per-trip — writing the whole (growing) dict after
    # every trip would make total I/O for a run scale with the square of
    # trip count, same issue already fixed for trips_metadata.json. The
    # finally still persists whatever was accumulated so far if something
    # raises partway through, so a crash mid-run doesn't lose already-done
    # work on the next attempt.
    try:
      for folder in sorted(input_path.iterdir()):
        if not folder.is_dir():
            continue

        sensor_id = folder.name
        print(f"Processing sensor {sensor_id}...")

        geojson_files = list(folder.glob("*_clean.geojson"))

        for idx, geojson_file in enumerate(geojson_files):
            total_files += 1
            filename = geojson_file.stem
            trip_id = filename.replace("_clean", "")

            serial = trip_id.split("_")[0]
            trip = "_".join(trip_id.split("_")[1:])
            if serial in SKIP_TRIPS and trip in SKIP_TRIPS[serial]:
                print(f"  ⏩ Skipping {trip_id}")
                skipped_files += 1
                continue

            sensor_output_dir = output_path / sensor_id
            output_file = sensor_output_dir / f"{trip_id}_processed.geojson"

            cached_version = processing_versions.get(trip_id)
            up_to_date = (
                output_file.exists()
                and cached_version == PROCESSING_VERSION
                and not FORCE_REPROCESS_ALL
            )

            if up_to_date:
                print(f"  ✓ {trip_id} already processed (v{PROCESSING_VERSION})")
                already_processed += 1
                continue
            elif output_file.exists():
                reason = "FORCE_REPROCESS_ALL is on" if FORCE_REPROCESS_ALL else (
                    f"stale: v{cached_version} → v{PROCESSING_VERSION}"
                    if cached_version is not None else "no version recorded"
                )
                print(f"  🔄 {trip_id} already processed — regenerating ({reason})")

            print(f"  🔄 Processing {trip_id}...")
            debug = (idx == 0 and processed_files == 0)
            processed_data, metadata = process_geojson_file(
                geojson_file, trip_id, saved_metadata, debug=debug
            )

            if processed_data:
                sensor_output_dir.mkdir(exist_ok=True)
                with open(output_file, 'w') as f:
                    json.dump(processed_data, f)
                num_segments = len(processed_data['features'])
                total_segments += num_segments
                processed_files += 1
                processing_versions[trip_id] = PROCESSING_VERSION
                print(f"  ✅ {num_segments} segments created")
            else:
                failed_files += 1
                print(f"  ❌ Failed to process")

        print(f"  ✅ Sensor complete\n")
    finally:
        save_processing_versions(processing_versions)

    print("=" * 60)
    print(f"✅ Processing complete!")
    print(f"  Total _clean files found: {total_files}")
    print(f"  Already processed: {already_processed}")
    print(f"  Newly processed: {processed_files}")
    print(f"  Skipped: {skipped_files}")
    print(f"  Failed: {failed_files}")
    print(f"  Total segments created: {total_segments}")
    print(f"  Output saved to: {output_path}")

    if saved_metadata:
        print(f"  Metadata preserved: {len(saved_metadata)} trips")

    all_speeds = []
    all_qualities = []
    for sensor_folder in output_path.iterdir():
        if not sensor_folder.is_dir():
            continue
        for processed_file in sensor_folder.glob("*_processed.geojson"):
            try:
                with open(processed_file, 'r') as f:
                    data = json.load(f)
                for feat in data['features']:
                    speed = feat['properties'].get('Speed', 0)
                    quality = feat['properties'].get('road_quality', 0)
                    if speed > 0:
                        all_speeds.append(speed)
                    if quality > 0:
                        all_qualities.append(quality)
            except:
                pass

    if all_speeds:
        print(f"\n📊 Speed statistics (excluding stopped):")
        print(f"  Min: {min(all_speeds):.1f} km/h")
        print(f"  Max: {max(all_speeds):.1f} km/h")
        print(f"  Average: {sum(all_speeds) / len(all_speeds):.1f} km/h")
        print(f"  Median: {sorted(all_speeds)[len(all_speeds) // 2]:.1f} km/h")

    if all_qualities:
        quality_counts = np.bincount(all_qualities, minlength=6)[1:]
        print(f"\n🛣️ Road quality statistics:")
        quality_labels = ['Perfect', 'Normal', 'Outdated', 'Bad', 'No road']
        for i, (label, count) in enumerate(zip(quality_labels, quality_counts), 1):
            percentage = (count / len(all_qualities)) * 100
            print(f"  {i} ({label}): {count} segments ({percentage:.1f}%)")


if __name__ == "__main__":
    import sys
    input_dir = sys.argv[1] if len(sys.argv) >= 2 else INPUT_ROOT
    output_dir = sys.argv[2] if len(sys.argv) >= 3 else OUTPUT_ROOT
    process_all_trips(input_dir, output_dir)
