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

# Trips to skip
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
            print(f"⚠️  Could not load metadata file: {e}")
    return {}

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
    
    a = math.sin(delta_phi/2)**2 + \
        math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    
    return R * c

def extract_metadata_and_features(data):
    """Separate metadata (features without coordinates) from actual features"""
    features = []
    metadata = {}
    
    # Important metadata keys to keep
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
        
        # Check if this is a metadata feature (no coordinates or empty coordinates)
        if coords is None or (isinstance(coords, list) and len(coords) == 0):
            # This is metadata - only keep important keys
            props = feat.get("properties", {})
            for key, value in props.items():
                # Keep only important metadata keys (skip sensor data rows)
                if key in important_keys or (not key.startswith(',,') and len(key) < 100):
                    metadata[key] = value
        else:
            features.append(feat)
    
    # Also check if metadata is in the top-level properties
    if not metadata and 'properties' in data:
        top_props = data.get('properties', {})
        for key, value in top_props.items():
            if key in important_keys or (not key.startswith(',,') and len(key) < 100):
                metadata[key] = value
    
    return features, metadata

def get_wheel_diameter(trip_id, file_metadata, saved_metadata):
    """Get wheel diameter from file metadata or saved metadata, in mm"""
    
    def parse_wheel_diameter(value):
        """Parse wheel diameter from various formats"""
        if not value:
            return None
        
        # Handle string values like ", 26.0 inch" or "26.0 inch"
        if isinstance(value, str):
            # Remove leading comma and whitespace
            value = value.strip(', ')
            # Extract numeric part
            parts = value.split()
            if parts:
                try:
                    diameter_inches = float(parts[0])
                    # Convert inches to mm
                    diameter_mm = diameter_inches * 25.4
                    return diameter_mm
                except (ValueError, IndexError):
                    pass
        
        # Handle numeric values (assume already in mm)
        try:
            return float(value)
        except (ValueError, TypeError):
            pass
        
        return None
    
    # First try: metadata from current file
    if file_metadata:
        wheel_value = file_metadata.get('WheelDiam') or file_metadata.get('Wheel mm')
        diameter = parse_wheel_diameter(wheel_value)
        if diameter:
            print(f"    ✓ Using wheel diameter from file metadata: {diameter:.1f}mm")
            return diameter
    
    # Second try: previously saved metadata for this trip
    if trip_id in saved_metadata:
        trip_meta = saved_metadata[trip_id]
        # Check if it's nested or flat structure
        if isinstance(trip_meta, dict):
            # Try direct keys first (flat structure)
            wheel_value = trip_meta.get('WheelDiam') or trip_meta.get('Wheel mm')
            if not wheel_value and 'metadata' in trip_meta:
                # Try nested structure
                wheel_value = trip_meta['metadata'].get('WheelDiam') or trip_meta['metadata'].get('Wheel mm')
            
            diameter = parse_wheel_diameter(wheel_value)
            if diameter:
                print(f"    ✓ Using wheel diameter from saved metadata: {diameter:.1f}mm")
                return diameter
    
    # Fallback to default
    print(f"    ⚠️  Wheel diameter not found, using default: {DEFAULT_WHEEL_DIAMETER_MM}mm")
    return DEFAULT_WHEEL_DIAMETER_MM

def extract_acceleration_data(features):
    """Extract Y-axis acceleration data from features"""
    acc_y_values = []
    
    for feature in features:
        props = feature.get('properties', {})
        # Try different possible field names
        acc_y = (props.get('Acc Y (g)') or 
                 props.get('Acc Y') or 
                 props.get('AccY') or 
                 props.get('acc_y'))
        
        if acc_y is not None:
            acc_y_values.append(safe_float(acc_y, 0.0))
        else:
            acc_y_values.append(0.0)
    
    return np.array(acc_y_values)

def map_road_quality_to_segments(points, road_quality_data):
    """
    Map road quality scores to segments based on sample indices.
    Each point has a 'samples' index, and road quality has 'time_windows' indices.
    """
    if road_quality_data is None:
        return None
    
    quality_scores = road_quality_data['road_quality']
    time_windows = road_quality_data['time_windows']
    
    # Create a lookup function
    def get_quality_at_sample(sample_idx):
        """Find the road quality score for a given sample index"""
        if len(time_windows) == 0:
            return 0
        
        # Find the closest time window
        closest_idx = np.argmin(np.abs(time_windows - sample_idx))
        return int(quality_scores[closest_idx])
    
    return get_quality_at_sample

def process_geojson_file(filepath, trip_id, saved_metadata, debug=False):
    """Process a single GeoJSON file: clean, calculate speeds, add road quality"""
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        if 'features' not in data:
            return None, None
        
        # Step 1: Extract features and metadata
        features, file_metadata = extract_metadata_and_features(data)
        
        if not features:
            return None, file_metadata
        
        # Get wheel diameter from file or saved metadata
        wheel_diameter_mm = get_wheel_diameter(trip_id, file_metadata, saved_metadata)
        wheel_circumference_m = (wheel_diameter_mm / 1000) * math.pi
        
        # Step 2: Extract acceleration data and calculate road quality
        print(f"    🛣️  Calculating road quality...")
        acc_y_data = extract_acceleration_data(features)
        
        road_quality_data = None
        if len(acc_y_data) > 200:  # Need enough data for analysis
            try:
                road_quality_data = calculate_road_quality(
                    acc_y_data, 
                    window_size=100, 
                    overlap=0.5
                )
                print(f"    ✓ Road quality calculated for {len(road_quality_data['road_quality'])} windows")
            except Exception as e:
                print(f"    ⚠️  Road quality calculation failed: {e}")
        else:
            print(f"    ⚠️  Not enough acceleration data for road quality analysis")
        
        if debug:
            print(f"\n  DEBUG - Metadata extraction:")
            print(f"    Found {len(features)} features")
            print(f"    Acceleration data points: {len(acc_y_data)}")
            print(f"    Metadata keys: {list(file_metadata.keys()) if file_metadata else 'None'}")
            
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
                'idx': idx
            })
        
        points.sort(key=lambda p: p['samples'])
        
        if len(points) < 2:
            return None, file_metadata
        
        # Step 3b: Drop the first 100m of the trip (privacy / identifiability)
        TRIM_START_METRES = 100
        cumulative_distance = 0.0
        trim_index = 0
        for k in range(1, len(points)):
            cumulative_distance += haversine_distance(
                points[k-1]['lon'], points[k-1]['lat'],
                points[k]['lon'],   points[k]['lat']
            )
            if cumulative_distance >= TRIM_START_METRES:
                trim_index = k
                break
        if trim_index > 0:
            points = points[trim_index:]
            print(f"    ✂️  Trimmed first {TRIM_START_METRES}m ({trim_index} points dropped)")
        
        if len(points) < 2:
            return None, file_metadata

        # Step 4: Create road quality lookup function
        quality_lookup = map_road_quality_to_segments(points, road_quality_data)
        
        # Step 5: Calculate speeds and create line segments with road quality
        new_features = []
        
        i = 0
        while i < len(points) - 1:
            start_point = points[i]
            
            # Find next point where HRot has changed (actual wheel movement)
            j = i + 1
            while j < len(points) and points[j]['hrot'] == start_point['hrot']:
                j += 1
            
            if j >= len(points):
                break
            
            end_point = points[j]
            
            # Prefer actual time difference if timestamps exist
            if start_point['time'] and end_point['time']:
                time_diff_seconds = (end_point['time'] - start_point['time']).total_seconds()
            else:
                sample_diff = end_point['samples'] - start_point['samples']
                time_diff_seconds = sample_diff * SECONDS_PER_SAMPLE
            
            # Skip unrealistic or zero durations
            if time_diff_seconds <= 0 or time_diff_seconds > 600:
                i = j
                continue
            
            # Calculate speed from wheel rotations
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
            
            # Skip unrealistic GPS jumps
            if gps_distance > 1000:
                i = j
                continue
            
            # Cap speed safely (40 km/h)
            if speed_kmh > 40:
                speed_kmh = 40
            
            # Get road quality for this segment (use midpoint sample index)
            midpoint_sample = (start_point['samples'] + end_point['samples']) // 2
            road_quality = quality_lookup(midpoint_sample) if quality_lookup else 0
            
            # Only create segments with movement and reasonable speeds
            if (start_point['lon'] != end_point['lon'] or 
                start_point['lat'] != end_point['lat']) and speed_kmh < 100:
                
                new_feature = {
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
                        'road_quality': road_quality,
                        'marker': start_point['marker'],
                        'trip_id': trip_id,
                        'hrot_diff': hrot_diff,
                        'sample_diff': end_point['samples'] - start_point['samples'],
                        'time_diff_s': round(time_diff_seconds, 3),
                        'gps_distance_m': round(gps_distance, 1),
                        'original_speed': start_point['original_speed'],
                        'wheel_diameter_mm': wheel_diameter_mm
                    }
                }
                new_features.append(new_feature)
            
            i = j
        
        if not new_features:
            return None, file_metadata
        
        # Print road quality stats for this trip
        if quality_lookup:
            qualities = [f['properties']['road_quality'] for f in new_features]
            quality_counts = np.bincount(qualities, minlength=6)[1:]
            print(f"    📊 Road quality distribution: {dict(enumerate(quality_counts, 1))}")
        
        processed_data = {
            'type': 'FeatureCollection',
            'features': new_features
        }
        
        return processed_data, file_metadata
    
    except Exception as e:
        import traceback
        print(f"  ⚠️  Error processing {filepath.name}: {e}")
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
    
    # Load existing metadata (DO NOT overwrite - csv_to_geojson owns this file!)
    saved_metadata = load_metadata()
    
    print("\n🚴 Processing Bike Trip Data with Road Quality")
    print("=" * 60)
    print(f"📂 Input: {input_path}")
    print(f"📂 Output: {output_path}")
    print(f"⚠️  NOTE: Metadata file is managed by csv_to_geojson_converter.py")
    
    total_files = 0
    processed_files = 0
    skipped_files = 0
    already_processed = 0
    failed_files = 0
    total_segments = 0
    
    # Process each sensor folder
    for folder in sorted(input_path.iterdir()):
        if not folder.is_dir():
            continue
        
        sensor_id = folder.name
        print(f"Processing sensor {sensor_id}...")
        
        # Find all _clean.geojson files
        geojson_files = list(folder.glob("*_clean.geojson"))
        
        for idx, geojson_file in enumerate(geojson_files):
            total_files += 1
            
            # Parse filename to get trip ID
            filename = geojson_file.stem  # e.g., "602B3_Trip1_clean"
            trip_id = filename.replace("_clean", "")  # e.g., "602B3_Trip1"
            
            # Check if this trip should be skipped
            serial = trip_id.split("_")[0]
            trip = "_".join(trip_id.split("_")[1:])
            
            if serial in SKIP_TRIPS and trip in SKIP_TRIPS[serial]:
                print(f"  ⏩ Skipping {trip_id}")
                skipped_files += 1
                continue
            
            # Check if already processed
            sensor_output_dir = output_path / sensor_id
            output_file = sensor_output_dir / f"{trip_id}_processed.geojson"
            
            if output_file.exists():
                print(f"  ✓ {trip_id} already processed")
                already_processed += 1
                continue
            
            print(f"  🔄 Processing {trip_id}...")
            
            # Enable debug for first file only
            debug = (idx == 0 and processed_files == 0)
            
            # Process the file
            processed_data, metadata = process_geojson_file(
                geojson_file, trip_id, saved_metadata, debug=debug
            )
            
            if processed_data:
                # Save processed file in sensor subfolder
                sensor_output_dir.mkdir(exist_ok=True)
                
                with open(output_file, 'w') as f:
                    json.dump(processed_data, f)
                
                num_segments = len(processed_data['features'])
                total_segments += num_segments
                processed_files += 1
                print(f"  ✅ {num_segments} segments created")
            else:
                failed_files += 1
                print(f"  ❌ Failed to process")
        
        print(f"  ✅ Sensor complete\n")
    
    
    # Don't save metadata - just preserve what exists from CSV converter
    # Metadata file should only be modified by csv_to_geojson_converter
    
    # Summary
    print("=" * 60)
    print(f"✅ Processing complete!")
    print(f"   Total _clean files found: {total_files}")
    print(f"   Already processed: {already_processed}")
    print(f"   Newly processed: {processed_files}")
    print(f"   Skipped: {skipped_files}")
    print(f"   Failed: {failed_files}")
    print(f"   Total segments created: {total_segments}")
    print(f"   Output saved to: {output_path}")
    
    if saved_metadata:
        print(f"   Metadata preserved: {len(saved_metadata)} trips")
    
    # Calculate speed and road quality statistics
    all_speeds = []
    all_qualities = []
    
    # Walk through sensor subfolders
    for sensor_folder in output_path.iterdir():
        if not sensor_folder.is_dir():
            continue
        
        for processed_file in sensor_folder.glob("*_processed.geojson"):
            try:
                with open(processed_file, 'r') as f:
                    data = json.load(f)
                    for f in data['features']:
                        speed = f['properties'].get('Speed', 0)
                        quality = f['properties'].get('road_quality', 0)
                        
                        if speed > 0:
                            all_speeds.append(speed)
                        if quality > 0:
                            all_qualities.append(quality)
            except:
                pass
    
    if all_speeds:
        print(f"\n📊 Speed statistics (excluding stopped):")
        print(f"   Min: {min(all_speeds):.1f} km/h")
        print(f"   Max: {max(all_speeds):.1f} km/h")
        print(f"   Average: {sum(all_speeds)/len(all_speeds):.1f} km/h")
        print(f"   Median: {sorted(all_speeds)[len(all_speeds)//2]:.1f} km/h")
    
    if all_qualities:
        quality_counts = np.bincount(all_qualities, minlength=6)[1:]
        print(f"\n🛣️  Road quality statistics:")
        quality_labels = ['Perfect', 'Normal', 'Outdated', 'Bad', 'No road']
        for i, (label, count) in enumerate(zip(quality_labels, quality_counts), 1):
            percentage = (count / len(all_qualities)) * 100
            print(f"   {i} ({label}): {count} segments ({percentage:.1f}%)")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) >= 2:
        input_dir = sys.argv[1]
    else:
        input_dir = INPUT_ROOT
    
    if len(sys.argv) >= 3:
        output_dir = sys.argv[2]
    else:
        output_dir = OUTPUT_ROOT
    
    process_all_trips(input_dir, output_dir)