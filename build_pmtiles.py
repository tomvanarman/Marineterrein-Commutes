"""
Build PMTiles from Processed GeoJSON Data
Unified script that creates PMTiles with speed and road quality data
"""

import subprocess
import sys
from pathlib import Path

def check_command(cmd):
    """Check if a command is available"""
    try:
        if cmd == 'pmtiles':
            subprocess.run([cmd], capture_output=True)
            return True
        else:
            subprocess.run([cmd, '--version'], capture_output=True, check=True)
            return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False

def main():
    print("🚴 Building PMTiles from Processed Data")
    print("=" * 60)
    
    # Check dependencies
    print("🔍 Checking dependencies...")
    
    if not check_command('tippecanoe'):
        print("❌ Error: tippecanoe not found")
        print("\nInstall instructions:")
        print("  macOS: brew install tippecanoe")
        print("  Linux: Build from https://github.com/felt/tippecanoe")
        return 1
    print("  ✅ tippecanoe found")
    
    if not check_command('pmtiles'):
        print("❌ Error: pmtiles not found")
        print("\nInstall instructions:")
        print("  brew install pmtiles")
        return 1
    print("  ✅ pmtiles found")
    
    # Set paths
    processed_dir = Path("processed_sensor_data")
    output_file = Path("trips.pmtiles")
    temp_mbtiles = Path("trips.mbtiles")
    
    # Check input directory
    if not processed_dir.exists():
        print(f"\n❌ Error: {processed_dir} directory not found")
        print("Run integrated_processor.py first to process your data")
        return 1
    
    # Count files 
    print(f"\n📂 Scanning {processed_dir}...")
    geojson_files = list(processed_dir.rglob("*_processed.geojson"))
    
    if len(geojson_files) == 0:
        print(f"❌ No processed files found in {processed_dir}")
        print("Run integrated_processor.py first to process your data")
        return 1
    
    print(f"📊 Found {len(geojson_files)} processed trip files")
    
    # Show breakdown by sensor
    sensors = {}
    for f in geojson_files:
        sensor = f.parent.name
        sensors[sensor] = sensors.get(sensor, 0) + 1
    
    for sensor, count in sorted(sensors.items()):
        print(f"   {sensor}: {count} trips")
    
    # Remove old files
    print("\n🗑️  Cleaning up old files...")
    if output_file.exists():
        output_file.unlink()
        print(f"   Removed old {output_file}")
    if temp_mbtiles.exists():
        temp_mbtiles.unlink()
        print(f"   Removed old {temp_mbtiles}")
    
    # Build with tippecanoe
    print("\n🔨 Building MBTiles with tippecanoe...")
    print("   This may take a few minutes...")
    
    cmd = [
        'tippecanoe',
        '--output', str(temp_mbtiles),
        '--force',
        '--maximum-zoom=16',
        '--minimum-zoom=10',
        '--drop-densest-as-needed',
        '--extend-zooms-if-still-dropping',
        # No --layer flag - let tippecanoe create layers from filenames
        '--include=Speed',
        '--include=road_quality',
        '--include=marker',
        '--include=trip_id'
    ]
    
    # Add all geojson files
    cmd.extend([str(f) for f in geojson_files])
    
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        print("   ✅ MBTiles created successfully")
    except subprocess.CalledProcessError as e:
        print(f"   ❌ Error: tippecanoe failed")
        print(f"\nError output:\n{e.stderr}")
        return 1
    
    # Convert to PMTiles
    print("\n📦 Converting MBTiles to PMTiles format...")
    try:
        subprocess.run([
            'pmtiles', 'convert',
            str(temp_mbtiles),
            str(output_file)
        ], check=True, capture_output=True)
        print("   ✅ PMTiles created successfully")
    except subprocess.CalledProcessError as e:
        print(f"   ❌ Error: PMTiles conversion failed")
        print(f"\nError output:\n{e.stderr}")
        return 1
    
    # Clean up temporary file
    print("\n🗑️  Cleaning up temporary files...")
    if temp_mbtiles.exists():
        temp_mbtiles.unlink()
        print(f"   Removed {temp_mbtiles}")
    
    # Show results
    size_mb = output_file.stat().st_size / (1024 * 1024)
    
    print("\n" + "=" * 60)
    print("✅ PMTiles build complete!")
    print(f"📦 Output: {output_file}")
    print(f"💾 Size: {size_mb:.2f} MB")
    print(f"📊 Contains {len(geojson_files)} trips from {len(sensors)} sensors")
    
    print("\n📋 Next steps:")
    print("1. Deploy bike_trips.pmtiles to your web server")
    print("2. Hard refresh your browser (Cmd+Shift+R)")
    print("3. Toggle 'Show Speed Colors' and 'Show Road Quality'")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())