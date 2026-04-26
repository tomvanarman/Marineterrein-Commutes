#!/usr/bin/env python3
"""
Master Pipeline for Reflector Ride Maps
Runs the complete data processing workflow:
1. (Optional) Fetch new trips from Supabase API
2. CSV to GeoJSON conversion
3. Speed calculation from sensor data
4. Road segment averaging and consolidation
5. PMTiles generation for web visualization
6. Cleanup of processed CSV files

Usage:
  python master_pipeline.py          # local CSVs only
  python master_pipeline.py --api    # fetch from Supabase first, then process
"""

import subprocess
import sys
import os
import json
from pathlib import Path
import time

# ─────────────────────────────────────────────────────────────────────────────
# ANSI colour helpers
# ─────────────────────────────────────────────────────────────────────────────

class Colors:
    HEADER    = '\033[95m'
    BLUE      = '\033[94m'
    CYAN      = '\033[96m'
    GREEN     = '\033[92m'
    YELLOW    = '\033[93m'
    RED       = '\033[91m'
    END       = '\033[0m'
    BOLD      = '\033[1m'
    UNDERLINE = '\033[4m'

def print_header(text):
    print(f"\n{Colors.HEADER}{Colors.BOLD}{'=' * 70}{Colors.END}")
    print(f"{Colors.HEADER}{Colors.BOLD}{text.center(70)}{Colors.END}")
    print(f"{Colors.HEADER}{Colors.BOLD}{'=' * 70}{Colors.END}\n")

def print_step(step_num, step_name):
    print(f"\n{Colors.CYAN}{Colors.BOLD}[STEP {step_num}] {step_name}{Colors.END}")
    print(f"{Colors.CYAN}{'─' * 70}{Colors.END}")

def print_success(text):  print(f"{Colors.GREEN}✅ {text}{Colors.END}")
def print_error(text):    print(f"{Colors.RED}❌ {text}{Colors.END}")
def print_warning(text):  print(f"{Colors.YELLOW}⚠️  {text}{Colors.END}")
def print_info(text):     print(f"{Colors.BLUE}ℹ️  {text}{Colors.END}")


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def count_files(directory, pattern):
    if not Path(directory).exists():
        return 0
    return len(list(Path(directory).rglob(pattern)))


def run_command(command, description):
    print_info(f"Running: {description}")
    print(f"{Colors.BOLD}Command:{Colors.END} {' '.join(str(c) for c in command)}\n")

    start_time = time.time()
    try:
        subprocess.run(command, check=True, text=True)
        elapsed = time.time() - start_time
        print_success(f"{description} completed in {elapsed:.2f}s")
        return True
    except subprocess.CalledProcessError as e:
        print_error(f"{description} failed! (exit code {e.returncode})")
        return False
    except FileNotFoundError:
        print_error(f"Command not found: {command[0]}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Prerequisites
# ─────────────────────────────────────────────────────────────────────────────

def check_python_packages(use_api):
    print_info("Checking Python packages…")
    required = ['numpy', 'geojson']
    if use_api:
        required.append('psycopg2')

    missing = []
    for pkg in required:
        try:
            __import__(pkg)
            print_success(f"Package '{pkg}' is installed")
        except ImportError:
            missing.append(pkg)
            print_error(f"Package '{pkg}' is NOT installed")

    if missing:
        print_error(f"\nMissing packages: {', '.join(missing)}")
        print_info(f"Install with: pip3 install {' '.join(missing)}")
        return False
    return True


def check_prerequisites(use_api):
    print_step("0", "Checking Prerequisites")
    issues = []

    # csv_data only required when not fetching from API
    csv_dir = Path("csv_data")
    if not use_api:
        if not csv_dir.exists():
            issues.append("csv_data/ directory not found")
            print_error("csv_data/ directory not found")
        else:
            csv_files = list(csv_dir.rglob("*.csv"))
            if not csv_files:
                issues.append("No CSV files found in csv_data/")
                print_warning("No CSV files found in csv_data/")
            else:
                print_success(f"Found {len(csv_files)} CSV file(s) in csv_data/")
    else:
        # csv_data will be populated by the API fetch; just make sure it exists
        csv_dir.mkdir(exist_ok=True)
        print_info("csv_data/ will be populated by the Supabase fetch step.")

    scripts = [
        "csv_to_geojson_converter.py",
        "integrated_processor.py",
        "road_averaging.py",
        "build_pmtiles.py",
    ]
    for script in scripts:
        if not Path(script).exists():
            issues.append(f"{script} not found")
            print_error(f"{script} not found")
        else:
            print_success(f"Found {script}")

    # tippecanoe
    try:
        result = subprocess.run(["tippecanoe", "--version"],
                                capture_output=True, text=True, check=False)
        if result.returncode == 0:
            print_success("tippecanoe is installed")
        else:
            raise FileNotFoundError
    except FileNotFoundError:
        issues.append("tippecanoe not found")
        print_error("tippecanoe not found")
        print_info("Install with: brew install tippecanoe (macOS)")

    return len(issues) == 0, issues


# ─────────────────────────────────────────────────────────────────────────────
# CSV cleanup
# ─────────────────────────────────────────────────────────────────────────────

def cleanup_csv_files():
    print_step("6", "Cleaning Up Processed CSV Files")

    csv_dir = Path("csv_data")
    if not csv_dir.exists():
        print_warning("csv_data/ directory not found")
        return

    csv_files = list(csv_dir.rglob("*.csv"))
    if not csv_files:
        print_info("No CSV files to clean up")
        return

    print_info(f"Found {len(csv_files)} CSV file(s) to delete:")
    for f in csv_files:
        print(f"  📄 {f}")

    try:
        response = input(
            f"\n{Colors.YELLOW}Delete these {len(csv_files)} CSV file(s)? (y/N): {Colors.END}"
        ).lower()
        if response != 'y':
            print_warning("CSV cleanup skipped")
            return
    except KeyboardInterrupt:
        print("\n")
        print_warning("CSV cleanup cancelled")
        return

    deleted, failed = 0, 0
    for f in csv_files:
        try:
            f.unlink()
            deleted += 1
            print_success(f"Deleted {f.name}")
        except Exception as e:
            failed += 1
            print_error(f"Failed to delete {f.name}: {e}")

    if deleted:
        print_success(f"Deleted {deleted} CSV file(s)")
    if failed:
        print_warning(f"Failed to delete {failed} file(s)")


# ─────────────────────────────────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────────────────────────────────

def print_summary(use_api):
    print_header("PIPELINE SUMMARY")

    csv_count              = count_files("csv_data", "*.csv")
    geojson_clean_count    = count_files("sensor_data", "*_clean.geojson")
    geojson_processed_count = count_files("processed_sensor_data", "*_processed.geojson")
    road_segments_exists   = Path("road_segments_averaged.json").exists()
    pmtiles_exists         = Path("trips.pmtiles").exists()

    # Metadata source breakdown
    meta_file = Path("trips_metadata.json")
    api_count, local_count = 0, 0
    if meta_file.exists():
        try:
            meta = json.loads(meta_file.read_text())
            for v in meta.values():
                if v.get("source") == "api":
                    api_count += 1
                elif v.get("source") == "local_csv":
                    local_count += 1
        except Exception:
            pass

    print(f"{Colors.BOLD}Input Files:{Colors.END}")
    print(f"  📄 CSV files remaining: {csv_count}")

    print(f"\n{Colors.BOLD}Trip Metadata (all-time):{Colors.END}")
    print(f"  🌐 Fetched via Supabase API : {api_count}")
    print(f"  📄 Loaded from local CSV    : {local_count}")

    print(f"\n{Colors.BOLD}Generated Files:{Colors.END}")
    print(f"  🗺️  Cleaned GeoJSON   : {geojson_clean_count}")
    print(f"  ⚡ Processed GeoJSON  : {geojson_processed_count}")
    print(f"  🛣️  Road Segments     : {'✅ Yes' if road_segments_exists else '❌ No'}")
    print(f"  📦 PMTiles           : {'✅ Yes' if pmtiles_exists else '❌ No'}")

    if road_segments_exists:
        try:
            data = json.loads(Path("road_segments_averaged.json").read_text())
            print(f"     Segments: {len(data.get('features', []))}")
        except Exception:
            pass

    if pmtiles_exists:
        size_mb = Path("trips.pmtiles").stat().st_size / (1024 * 1024)
        print(f"     Size: {size_mb:.2f} MB")

    print(f"\n{Colors.BOLD}Output Directories:{Colors.END}")
    print(f"  📁 sensor_data/")
    print(f"  📁 processed_sensor_data/")

    if pmtiles_exists:
        print(f"\n{Colors.GREEN}{Colors.BOLD}✅ Pipeline completed successfully!{Colors.END}")
        print(f"\n{Colors.CYAN}Next steps:{Colors.END}")
        print(f"  1. Commit: git add . && git commit -m 'Update trip data'")
        print(f"  2. Push  : git push")
        print(f"  3. View  : https://tomvanarman.github.io/Reflector-Ride-Maps/")
    else:
        print(f"\n{Colors.YELLOW}{Colors.BOLD}⚠️  Pipeline completed with issues{Colors.END}")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    use_api = "--api" in sys.argv

    print_header("REFLECTOR RIDE MAPS - MASTER PIPELINE")
    mode_label = "🌐 Supabase API + local CSVs" if use_api else "📄 Local CSVs only"
    print(f"{Colors.BOLD}Mode: {mode_label}{Colors.END}\n")

    print_info(f"Python: {sys.executable} ({sys.version.split()[0]})\n")

    if not check_python_packages(use_api):
        print_error("Required Python packages are missing — aborting.")
        sys.exit(1)

    prereqs_ok, issues = check_prerequisites(use_api)
    if not prereqs_ok:
        print_error("Prerequisites check failed!")
        for issue in issues:
            print(f"  • {issue}")
        sys.exit(1)

    print_success("All prerequisites met!\n")

    try:
        response = input(
            f"{Colors.YELLOW}Continue with pipeline? (y/N): {Colors.END}"
        ).lower()
        if response != 'y':
            print("\nPipeline cancelled.")
            sys.exit(0)
    except KeyboardInterrupt:
        print("\n\nPipeline cancelled.")
        sys.exit(0)

    total_start = time.time()

    # ── Step 1: CSV → GeoJSON (with optional Supabase fetch built in) ─────────
    print_step("1", "Converting CSV to GeoJSON" + (" (+ Supabase fetch)" if use_api else ""))
    converter_cmd = [sys.executable, "csv_to_geojson_converter.py"]
    if use_api:
        converter_cmd.append("--api")
    step1_ok = run_command(converter_cmd, "CSV to GeoJSON conversion")

    if not step1_ok:
        print_error("Step 1 failed. Aborting.")
        sys.exit(1)

    # ── Step 2: Speed calculation ─────────────────────────────────────────────
    print_step("2", "Calculating Speeds from Sensor Data")
    step2_ok = run_command(
        [sys.executable, "integrated_processor.py"],
        "Speed calculation"
    )
    if not step2_ok:
        print_error("Step 2 failed. Aborting.")
        sys.exit(1)

    # ── Step 3: Road segment averaging ───────────────────────────────────────
    print_step("3", "Averaging and Consolidating Road Segments")
    step3_ok = run_command(
        [sys.executable, "road_averaging.py"],
        "Road segment averaging"
    )
    if not step3_ok:
        print_warning("Step 3 failed — continuing anyway…")

    # ── Step 4: PMTiles ───────────────────────────────────────────────────────
    print_step("4", "Building PMTiles for Web")
    step4_ok = run_command(
        [sys.executable, "build_pmtiles.py"],
        "PMTiles generation"
    )
    if not step4_ok:
        print_error("Step 4 failed. Aborting.")
        sys.exit(1)

    # ── Step 5: Cleanup ───────────────────────────────────────────────────────
    if step1_ok and step2_ok and step4_ok:
        cleanup_csv_files()
    else:
        print_warning("Skipping CSV cleanup due to earlier errors.")

    total_elapsed = time.time() - total_start
    print(f"\n{Colors.BOLD}Total time: {total_elapsed:.2f}s{Colors.END}")
    print_summary(use_api)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n\n{Colors.YELLOW}Pipeline interrupted by user{Colors.END}")
        sys.exit(1)
    except Exception as e:
        print(f"\n{Colors.RED}Unexpected error: {e}{Colors.END}")
        raise
