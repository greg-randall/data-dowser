#!/usr/bin/env python3
"""
Build dashboard data from processed water quality JSONs and system metadata.
Merges contaminant data with location/population info for the dashboard.
"""

import json
import os
import re
from pathlib import Path
from collections import defaultdict

from functools import lru_cache
import yaml


def load_contaminant_categories():
    """Load contaminant categories from YAML file."""
    yaml_path = Path(__file__).parent / "contaminant_categories.yaml"
    with open(yaml_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


# Contaminant categories for filtering (loaded from YAML)
CONTAMINANT_CATEGORIES = load_contaminant_categories()


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


def load_contaminant_data(downloads_dir, limit=None):
    """Load all processed contaminant JSONs from downloads directory.

    Returns:
        tuple: (systems dict, contaminant_meta dict)
            - systems: system_id -> {name, water_source, years: {year: {violations, contaminants}}}
            - contaminant_meta: contaminant_name -> {mcl, mclg, units, category, categories}
    """
    print(f"Loading contaminant data from {downloads_dir}...")
    systems = defaultdict(lambda: {"years": {}})
    contaminant_meta = {}  # Deduplicated metadata
    file_count = 0
    dir_count = 0

    downloads_path = Path(downloads_dir)

    # Use scandir for faster directory listing
    try:
        dirs = list(os.scandir(downloads_path))
    except OSError as e:
        print(f"Error scanning {downloads_path}: {e}")
        return {}, {}

    total_dirs = len([d for d in dirs if d.is_dir()])
    print(f"  Found {total_dirs} system directories")

    for entry in dirs:
        if not entry.is_dir():
            continue

        dir_count += 1
        if dir_count % 1000 == 0:
            print(f"  Scanning directory {dir_count}/{total_dirs} ({file_count} files loaded)...")

        # Look for TX*.json files in this directory
        try:
            for file_entry in os.scandir(entry.path):
                if not file_entry.name.endswith('.json'):
                    continue
                if not file_entry.name.startswith('TX'):
                    continue

                try:
                    with open(file_entry.path, 'r', encoding='utf-8') as f:
                        data = json.load(f)

                    system_id = data.get("system_id")
                    year = data.get("year")

                    if not system_id or not year:
                        continue

                    # Store system name if available
                    if data.get("system_name") and not systems[system_id].get("name"):
                        systems[system_id]["name"] = data["system_name"]

                    # Store water source if available
                    if data.get("water_source") and not systems[system_id].get("water_source"):
                        systems[system_id]["water_source"] = data["water_source"]

                    # Process contaminants
                    contaminants = data.get("contaminants", [])
                    year_data = {
                        "violations": [],
                        "contaminants": {}
                    }

                    for c in contaminants:
                        name = c.get("name", "").strip()
                        if not name:
                            continue

                        # Clean up the name
                        clean_name = re.sub(r'\s+', ' ', name).strip()

                        # Store only level per instance (normalized)
                        year_data["contaminants"][clean_name] = c.get("highest_level")

                        # Store metadata once per contaminant name
                        if clean_name not in contaminant_meta:
                            contaminant_meta[clean_name] = {
                                "mcl": c.get("mcl"),
                                "mclg": c.get("mclg"),
                                "units": c.get("units"),
                                "category": c.get("category"),
                                "categories": categorize_contaminant(name)
                            }

                        if c.get("violation"):
                            year_data["violations"].append(clean_name)

                    systems[system_id]["years"][str(year)] = year_data
                    file_count += 1

                    if limit and file_count >= limit:
                        print(f"  Hit limit of {limit} files")
                        return dict(systems), contaminant_meta

                except (json.JSONDecodeError, IOError) as e:
                    continue
        except OSError:
            continue

    print(f"  Loaded {file_count} files for {len(systems)} systems")
    return dict(systems), contaminant_meta


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

        # Get coordinates
        lat, lon = extract_coordinates(meta)
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
    print(f"\nFile sizes:")
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
