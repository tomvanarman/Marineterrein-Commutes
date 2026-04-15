import json
import glob
from collections import defaultdict
import math
from pathlib import Path

def haversine_distance(lon1, lat1, lon2, lat2):
    """Calculate distance between two points in meters"""
    R = 6371000
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    
    a = math.sin(dphi/2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    
    return R * c

def calculate_bearing(lon1, lat1, lon2, lat2):
    """Calculate bearing between two points in degrees"""
    dlon = math.radians(lon2 - lon1)
    lat1 = math.radians(lat1)
    lat2 = math.radians(lat2)
    
    y = math.sin(dlon) * math.cos(lat2)
    x = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(dlon)
    bearing = math.degrees(math.atan2(y, x))
    return (bearing + 360) % 360

def snap_to_grid(lon, lat, grid_size=0.001):
    """Snap coordinates to a coarser grid (0.001 = ~111m)"""
    return (round(lon / grid_size) * grid_size, 
            round(lat / grid_size) * grid_size)

def are_segments_similar(coord1a, coord1b, coord2a, coord2b, distance_threshold=50, bearing_threshold=15):
    """Check if two segments are similar enough to be merged"""
    # Calculate midpoints
    mid1_lon = (coord1a[0] + coord1b[0]) / 2
    mid1_lat = (coord1a[1] + coord1b[1]) / 2
    mid2_lon = (coord2a[0] + coord2b[0]) / 2
    mid2_lat = (coord2a[1] + coord2b[1]) / 2
    
    # Check if midpoints are close
    midpoint_dist = haversine_distance(mid1_lon, mid1_lat, mid2_lon, mid2_lat)
    if midpoint_dist > distance_threshold:
        return False
    
    # Check if bearings are similar
    bearing1 = calculate_bearing(coord1a[0], coord1a[1], coord1b[0], coord1b[1])
    bearing2 = calculate_bearing(coord2a[0], coord2a[1], coord2b[0], coord2b[1])
    
    bearing_diff = abs(bearing1 - bearing2)
    if bearing_diff > 180:
        bearing_diff = 360 - bearing_diff
    
    return bearing_diff < bearing_threshold

def merge_segments(segments_list):
    """Merge similar segments into consolidated ones"""
    if not segments_list:
        return []
    
    merged = []
    used = set()
    
    for i, seg1 in enumerate(segments_list):
        if i in used:
            continue
            
        # Start a merged group with this segment
        group = [seg1]
        used.add(i)
        
        # Find all similar segments
        for j, seg2 in enumerate(segments_list):
            if j in used or j <= i:
                continue
                
            if are_segments_similar(
                seg1['coords'][0], seg1['coords'][1],
                seg2['coords'][0], seg2['coords'][1]
            ):
                group.append(seg2)
                used.add(j)
        
        # Merge the group
        all_speeds = []
        all_qualities = []
        all_trips = set()
        
        for seg in group:
            all_speeds.extend(seg['speeds'])
            all_qualities.extend(seg['qualities'])
            all_trips.update(seg['trips'])
        
        # Use average coordinates
        avg_lon1 = sum(s['coords'][0][0] for s in group) / len(group)
        avg_lat1 = sum(s['coords'][0][1] for s in group) / len(group)
        avg_lon2 = sum(s['coords'][1][0] for s in group) / len(group)
        avg_lat2 = sum(s['coords'][1][1] for s in group) / len(group)
        
        merged.append({
            'coords': ([avg_lon1, avg_lat1], [avg_lon2, avg_lat2]),
            'speeds': all_speeds,
            'qualities': all_qualities,
            'trips': all_trips
        })
    
    return merged

def process_trip_files(input_pattern="processed_sensor_data/**/*_processed.geojson"):
    """Process all trip files and aggregate road segment data"""
    
    # Collect all segments first
    all_segments = []
    
    files = glob.glob(input_pattern, recursive=True)
    print(f"Found {len(files)} trip files to process")
    
    if len(files) == 0:
        print("\n❌ No files found! Trying alternatives...")
        alternatives = [
            "**/*_processed.geojson",
            "*/*_processed.geojson",
            "*/processed_sensor_data/**/*_processed.geojson"
        ]
        
        for alt in alternatives:
            files = glob.glob(alt, recursive=True)
            if files:
                print(f"✅ Found {len(files)} files with pattern: {alt}")
                break
        
        if not files:
            print("\n❌ No files found.")
            return None
    
    for file_path in files:
        trip_id = Path(file_path).stem
        print(f"Processing {trip_id}...")
        
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            features = data.get('features', [])
            
            for feature in features:
                if feature['geometry']['type'] != 'LineString':
                    continue
                    
                coords = feature['geometry']['coordinates']
                props = feature['properties']
                
                speed = props.get('Speed', props.get('speed', 0))
                quality = props.get('road_quality', 0)
                
                # Process each line segment
                for i in range(len(coords) - 1):
                    coord1 = coords[i]
                    coord2 = coords[i + 1]
                    
                    # Skip very short segments
                    dist = haversine_distance(coord1[0], coord1[1], coord2[0], coord2[1])
                    if dist < 5:  # Less than 5 meters
                        continue
                    
                    all_segments.append({
                        'coords': (coord1, coord2),
                        'speeds': [float(speed)],
                        'qualities': [int(quality)] if quality > 0 else [],
                        'trips': {trip_id}
                    })
                    
        except Exception as e:
            print(f"Error processing {file_path}: {e}")
    
    print(f"\nCollected {len(all_segments)} raw segments")
    print("Merging similar segments...")
    
    # Merge similar segments
    merged_segments = merge_segments(all_segments)
    
    print(f"Consolidated to {len(merged_segments)} segments")
    
    # Create output features
    features = []
    
    for seg_data in merged_segments:
        if len(seg_data['speeds']) < 2:
            continue
        
        avg_speed = sum(seg_data['speeds']) / len(seg_data['speeds'])
        min_speed = min(seg_data['speeds'])
        max_speed = max(seg_data['speeds'])
        
        avg_quality = sum(seg_data['qualities']) / len(seg_data['qualities']) if seg_data['qualities'] else 0
        
        coord1, coord2 = seg_data['coords']
        distance = haversine_distance(coord1[0], coord1[1], coord2[0], coord2[1])
        
        # Composite score
        speed_score = max(0, 100 - (avg_speed * 4))
        quality_score = (avg_quality - 1) * 25 if avg_quality > 0 else 50
        composite_score = (quality_score * 0.6) + (speed_score * 0.4)
        
        feature = {
            'type': 'Feature',
            'geometry': {
                'type': 'LineString',
                'coordinates': [coord1, coord2]
            },
            'properties': {
                'avg_speed': round(avg_speed, 2),
                'min_speed': round(min_speed, 2),
                'max_speed': round(max_speed, 2),
                'speed_variance': round(max_speed - min_speed, 2),
                'avg_quality': round(avg_quality, 2) if avg_quality > 0 else None,
                'observation_count': len(seg_data['speeds']),
                'trip_count': len(seg_data['trips']),
                'distance_m': round(distance, 2),
                'composite_score': round(composite_score, 2),
                'trips': list(seg_data['trips'])
            }
        }
        
        features.append(feature)
    
    print(f"Created {len(features)} final segments")
    
    if len(features) == 0:
        print("\n❌ No segments created")
        return None
    
    output = {
        'type': 'FeatureCollection',
        'features': features
    }
    
    output_file = 'road_segments_averaged.json'
    with open(output_file, 'w') as f:
        json.dump(output, f)
    
    print(f"\n✅ Saved to {output_file}")
    
    # Statistics
    all_speeds = [f['properties']['avg_speed'] for f in features]
    all_qualities = [f['properties']['avg_quality'] for f in features if f['properties']['avg_quality']]
    all_composites = [f['properties']['composite_score'] for f in features]
    
    if all_speeds:
        print(f"\nStatistics:")
        print(f"  Speed: {min(all_speeds):.1f} - {max(all_speeds):.1f} km/h (avg: {sum(all_speeds)/len(all_speeds):.1f})")
        if all_qualities:
            print(f"  Quality: {min(all_qualities):.1f} - {max(all_qualities):.1f} (avg: {sum(all_qualities)/len(all_qualities):.1f})")
        print(f"  Composite: {min(all_composites):.1f} - {max(all_composites):.1f}")
    
    return output

if __name__ == "__main__":
    result = process_trip_files()
    
    if result:
        print("\n🎉 Processing complete!")
    else:
        print("\n❌ Processing failed.")