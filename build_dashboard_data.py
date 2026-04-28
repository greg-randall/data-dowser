#!/usr/bin/env python3
"""
Build dashboard data from processed water quality JSONs and system metadata.
Merges contaminant data with location/population info for the dashboard.
"""

import json
import os
import re
import math
import multiprocessing
from pathlib import Path
from collections import defaultdict

from functools import lru_cache
import yaml
from tqdm import tqdm

from data_patches import apply_patch


def load_contaminant_categories():
    """Load contaminant categories from YAML file."""
    yaml_path = Path(__file__).parent / "contaminant_categories.yaml"
    with open(yaml_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def load_contaminant_limits():
    """Load authoritative MCLs from YAML file."""
    yaml_path = Path(__file__).parent / "contaminant_limits.yaml"
    if not yaml_path.exists():
        return {}
    with open(yaml_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


# Global config
CONTAMINANT_CATEGORIES = load_contaminant_categories()
CONTAMINANT_LIMITS = load_contaminant_limits()


def normalize_contaminant_name(name):
    """Normalize contaminant name for matching."""
    if not name:
        return ""
    # Remove newlines and extra whitespace
    name = re.sub(r'\s+', ' ', name).strip().lower()
    # Remove common suffixes/prefixes
    name = re.sub(r'\[.*?\]', '', name).strip()
    name = re.sub(r'\(.*?\)', '', name).strip()
    return name


@lru_cache(maxsize=None)
def categorize_contaminant(name):
    """Return list of categories a contaminant belongs to."""
    normalized = normalize_contaminant_name(name)
    categories = []
    for category, keywords in CONTAMINANT_CATEGORIES.items():
        for keyword in keywords:
            if keyword in normalized:
                categories.append(category)
                break
    return categories


def extract_coordinates(system_data):
    """Extract first valid lat/lon from system sources."""
    sources = system_data.get("sources", [])
    for source in sources:
        lat = source.get("latitude")
        lon = source.get("longitude")
        if lat and lon:
            try:
                lat_f = float(lat)
                lon_f = float(lon)
                # Basic sanity check for Texas coordinates
                if 25 < lat_f < 37 and -107 < lon_f < -93:
                    return lat_f, lon_f
            except (ValueError, TypeError):
                continue
    return None, None


def load_system_metadata(metadata_path):
    """Load water system metadata from get-ids.py output."""
    print(f"Loading metadata from {metadata_path}...")
    with open(metadata_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def process_system_dir(dir_path):
    """Worker function to process all JSONs in a single system directory."""
    systems = {}  # Use a regular dict for pickling compatibility
    contaminant_meta = {}
    file_count = 0

    try:
        for file_entry in os.scandir(dir_path):
            if not file_entry.name.endswith('.json') or not file_entry.name.startswith('TX'):
                continue

            try:
                with open(file_entry.path, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                system_id = data.get("system_id")
                year = data.get("year")
                if not system_id or not year:
                    continue

                if system_id not in systems:
                    systems[system_id] = {"years": {}}

                if data.get("system_name") and not systems[system_id].get("name"):
                    systems[system_id]["name"] = data["system_name"]
                if data.get("water_source") and not systems[system_id].get("water_source"):
                    systems[system_id]["water_source"] = data["water_source"]

                contaminants = data.get("contaminants", [])
                year_data = {"violations": [], "contaminants": {}}

                for c in contaminants:
                    name = c.get("name", "").strip()
                    if not name: continue
                    clean_name = re.sub(r'\s+', ' ', name).strip()
                    
                    level = c.get("highest_level")
                    level, dropped = apply_patch(system_id, year, clean_name, level)
                    if dropped: continue

                    # Use authoritative MCL if available, otherwise trust the report
                    limit_config = CONTAMINANT_LIMITS.get(clean_name, {})
                    mcl = limit_config.get("mcl", c.get("mcl"))
                    mclg = limit_config.get("mclg", c.get("mclg"))
                    units = limit_config.get("units", c.get("units"))

                    year_data["contaminants"][clean_name] = level
                    if clean_name not in contaminant_meta:
                        contaminant_meta[clean_name] = {
                            "mcl": mcl,
                            "mclg": mclg,
                            "units": units,
                            "category": c.get("category"),
                            "categories": categorize_contaminant(name)
                        }

                    is_violation = False
                    if level is not None and mcl is not None:
                        try:
                            if level > mcl:
                                is_violation = True
                        except (TypeError, ValueError):
                            pass
                    
                    # If we couldn't determine by level, fallback to raw flag
                    # but only if it doesn't look like a false positive.
                    if not is_violation and c.get("violation"):
                        # If level <= mcl, it's likely a false positive from the scraper
                        if level is not None and mcl is not None:
                            try:
                                if level <= mcl:
                                    is_violation = False
                                else:
                                    is_violation = True
                            except (TypeError, ValueError):
                                is_violation = True
                        else:
                            is_violation = True

                    if is_violation:
                        year_data["violations"].append(clean_name)

                systems[system_id]["years"][str(year)] = year_data
                file_count += 1
            except (json.JSONDecodeError, IOError):
                continue
    except OSError:
        pass

    return systems, contaminant_meta, file_count


def load_contaminant_data(downloads_dir, limit=None):
    """Load all processed contaminant JSONs using multiprocessing."""
    print(f"Loading contaminant data from {downloads_dir}...")
    
    downloads_path = Path(downloads_dir)
    try:
        all_dirs = [d.path for d in os.scandir(downloads_path) if d.is_dir()]
    except OSError as e:
        print(f"Error scanning {downloads_path}: {e}")
        return {}, {}

    if limit:
        all_dirs = all_dirs[:limit]  # Simplistic limit for testing

    total_dirs = len(all_dirs)
    print(f"  Processing {total_dirs} system directories using {multiprocessing.cpu_count()} cores...")

    merged_systems = defaultdict(lambda: {"years": {}})
    merged_meta = {}
    total_files = 0

    with multiprocessing.Pool() as pool:
        for systems, meta, count in tqdm(pool.imap_unordered(process_system_dir, all_dirs), total=total_dirs, desc="Loading Data"):
            total_files += count
            
            # Merge systems
            for sid, sdata in systems.items():
                if "name" in sdata and not merged_systems[sid].get("name"):
                    merged_systems[sid]["name"] = sdata["name"]
                if "water_source" in sdata and not merged_systems[sid].get("water_source"):
                    merged_systems[sid]["water_source"] = sdata["water_source"]
                if "years" in sdata:
                    merged_systems[sid]["years"].update(sdata["years"])
            
            # Merge meta
            for cname, cinfo in meta.items():
                if cname not in merged_meta:
                    merged_meta[cname] = cinfo

    print(f"  Loaded {total_files} files for {len(merged_systems)} systems")
    return dict(merged_systems), merged_meta


def compute_violation_status(years_data, recent_threshold):
    """Compute violation status: 0=none, 1=old, 2=recent."""
    has_recent = False
    has_old = False

    for year_str, year_data in years_data.items():
        if not year_data.get("violations"):
            continue
        try:
            year_int = int(year_str)
            if year_int >= recent_threshold:
                has_recent = True
            else:
                has_old = True
        except ValueError:
            pass

    if has_recent:
        return 2
    if has_old:
        return 1
    return 0


def build_dashboard_data(downloads_dir, metadata_path, output_path, limit=None):
    """Build split dashboard data files for progressive loading.

    Outputs:
    - dashboard_map.json: minimal data for instant map rendering
    - dashboard_details.json: full details loaded in background
    """

    # Load data
    metadata = load_system_metadata(metadata_path)
    contaminant_data, contaminant_meta = load_contaminant_data(downloads_dir, limit=limit)

    # Supplementary geocoded coords for systems TCEQ didn't publish a source
    # water location for. Produced by geocode_missing.py.
    geocoded = {}
    geocoded_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "geocoded_coordinates.json")
    if os.path.exists(geocoded_path):
        with open(geocoded_path) as f:
            geocoded = json.load(f)
        print(f"Loaded {len(geocoded)} supplementary geocoded coordinates")

    print("Merging data...")

    # Separate structures for map vs details
    map_systems = []  # Minimal data for markers
    details = {}      # Full data keyed by system_id

    stats = {
        "total_systems": 0,
        "systems_with_data": 0,
        "systems_with_violations": 0,
        "systems_with_coordinates": 0,
        "population_affected": 0,
        "total_violations": 0,
        "year_range": {"min": 9999, "max": 0}
    }

    # All unique contaminants found
    all_contaminants = set()

    # Merge metadata with contaminant data
    for system_id, meta in metadata.items():
        # Parse population
        pop_str = meta.get("meta", {}).get("population", "0")
        population = 0
        try:
            population = int(pop_str.replace(",", ""))
        except (ValueError, AttributeError):
            pass

        # Get coordinates — first try the TCEQ source-water inventory, then
        # fall back to the supplementary geocoded set.
        lat, lon = extract_coordinates(meta)
        coord_source = "source_water_inventory" if lat is not None else None
        if lat is None and system_id in geocoded:
            g = geocoded[system_id]
            lat = g["lat"]
            lon = g["lon"]
            coord_source = g.get("source")
        has_coords = lat is not None and lon is not None
        if has_coords:
            stats["systems_with_coordinates"] += 1

        # Build details entry
        name = meta.get("folder_name", "").replace(f"{system_id}_", "").replace("_", " ")
        detail_entry = {
            "n": name,  # name
            "c": meta.get("meta", {}).get("county", "Unknown"),  # county
            "t": meta.get("meta", {}).get("system_type", ""),  # system_type
        }
        if coord_source and coord_source != "source_water_inventory":
            detail_entry["cs"] = coord_source  # coord_source tag for fallback pins

        years_data = {}

        # Add contaminant data if available
        if system_id in contaminant_data:
            cdata = contaminant_data[system_id]

            # Use name from contaminant data if better
            if cdata.get("name"):
                detail_entry["n"] = cdata["name"]

            if cdata.get("water_source"):
                detail_entry["ws"] = cdata["water_source"]  # water_source

            # Convert years to short keys
            years_data = cdata.get("years", {})
            if years_data:
                detail_entry["y"] = {}  # years
                for year, year_data in years_data.items():
                    detail_entry["y"][year] = {
                        "v": year_data.get("violations", []),  # violations
                        "c": year_data.get("contaminants", {})  # contaminants (already just levels)
                    }

            # Update stats
            if years_data:
                stats["systems_with_data"] += 1

                has_violation = False
                for year, year_data in years_data.items():
                    try:
                        year_int = int(year)
                        stats["year_range"]["min"] = min(stats["year_range"]["min"], year_int)
                        stats["year_range"]["max"] = max(stats["year_range"]["max"], year_int)
                    except ValueError:
                        pass

                    if year_data.get("violations"):
                        has_violation = True
                        stats["total_violations"] += len(year_data["violations"])

                    for cname in year_data.get("contaminants", {}).keys():
                        all_contaminants.add(cname)

                if has_violation:
                    stats["systems_with_violations"] += 1
                    stats["population_affected"] += population

        # Only include systems with coordinates or data
        has_years = bool(years_data)
        if has_coords or has_years:
            stats["total_systems"] += 1

            # Store details
            details[system_id] = detail_entry

            # Build minimal map entry (only if has coordinates)
            if has_coords:
                # Compute violation status (threshold: max_year - 2)
                max_year = stats["year_range"]["max"] if stats["year_range"]["max"] > 0 else 2024
                recent_threshold = max_year - 2
                violation_status = compute_violation_status(years_data, recent_threshold)

                map_entry = {
                    "i": system_id,  # id
                    "la": round(lat, 4),  # lat (4 decimals = ~11m precision)
                    "lo": round(lon, 4),  # lon
                    "p": population,  # population
                    "v": violation_status  # violation status: 0=none, 1=old, 2=recent
                }
                map_systems.append(map_entry)

    # Fix year range if no data
    if stats["year_range"]["min"] == 9999:
        stats["year_range"]["min"] = 2015
    if stats["year_range"]["max"] == 0:
        stats["year_range"]["max"] = 2024

    print(f"  Total systems with data or coordinates: {stats['total_systems']}")
    print(f"  Systems with violations: {stats['systems_with_violations']}")
    print(f"  Population affected: {stats['population_affected']:,}")
    print(f"  Map markers: {len(map_systems)}")

    # Build normalized contaminant metadata with short keys
    # m -> {contaminant_name: {m: mcl, g: mclg, u: units, ca: category, cs: categories}}
    normalized_meta = {}
    for cname, meta_info in contaminant_meta.items():
        normalized_meta[cname] = {}
        if meta_info.get("mcl") is not None:
            normalized_meta[cname]["m"] = meta_info["mcl"]
        if meta_info.get("mclg") is not None:
            normalized_meta[cname]["g"] = meta_info["mclg"]
        if meta_info.get("units"):
            normalized_meta[cname]["u"] = meta_info["units"]
        if meta_info.get("category"):
            normalized_meta[cname]["ca"] = meta_info["category"]
        if meta_info.get("categories"):
            normalized_meta[cname]["cs"] = meta_info["categories"]

    # Build map output (small, loads first)
    map_output = {
        "s": map_systems,  # systems (minimal)
        "st": stats,  # stats
        "cat": CONTAMINANT_CATEGORIES,  # categories (for filtering)
    }

    # Build details output (larger, loads in background)
    details_output = {
        "d": details,  # details keyed by system_id
        "m": normalized_meta,  # contaminant_meta (deduplicated)
        "cl": sorted(list(all_contaminants)),  # contaminant_list
    }

    # Determine output paths
    base_path = output_path.replace('.json', '')
    map_path = f"{base_path}_map.json"
    details_path = f"{base_path}_details.json"

    print(f"Writing map data to {map_path}...")
    with open(map_path, 'w', encoding='utf-8') as f:
        json.dump(map_output, f, separators=(',', ':'))

    print(f"Writing details data to {details_path}...")
    with open(details_path, 'w', encoding='utf-8') as f:
        json.dump(details_output, f, separators=(',', ':'))

    # Also write a pretty version for debugging
    debug_path = output_path.replace('.json', '_debug.json')
    with open(debug_path, 'w', encoding='utf-8') as f:
        json.dump(stats, f, indent=2)

    # Report file sizes
    map_size = os.path.getsize(map_path)
    details_size = os.path.getsize(details_path)
    print("\nFile sizes:")
    print(f"  {map_path}: {map_size:,} bytes ({map_size/1024:.1f} KB)")
    print(f"  {details_path}: {details_size:,} bytes ({details_size/1024:.1f} KB)")

    print("Done!")
    return map_output, details_output


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Build dashboard data from water quality reports")
    parser.add_argument("--downloads", default="downloads", help="Downloads directory path")
    parser.add_argument("--metadata", default="water_system_data_full_profile.json",
                        help="Water system metadata JSON path")
    parser.add_argument("--output", default="dashboard_data.json", help="Output file path")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of JSON files to process (for testing)")

    args = parser.parse_args()

    build_dashboard_data(args.downloads, args.metadata, args.output, limit=args.limit)
