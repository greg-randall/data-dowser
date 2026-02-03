#!/usr/bin/env python3
"""
TCEQ Water Quality Report Scraper

Downloads Consumer Confidence Reports from TCEQ's portal for Texas water systems.
Supports resumable downloads with progress tracking.
"""

import argparse
import json
import subprocess
import sys
import time
import threading
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from datetime import datetime
import re

from bs4 import BeautifulSoup
from tqdm import tqdm

# Configuration
MAX_WORKERS = 2         # Concurrent download threads
MAX_RETRIES = 3         # Retry attempts per download
RETRY_DELAY = 30        # Seconds to wait before retry
TIMEOUT = 360           # Request timeout in seconds

# File paths
BASE_DIR = Path(__file__).parent
DOWNLOADS_DIR = BASE_DIR / "downloads"
WATER_SYSTEMS_FILE = BASE_DIR / "water_systems.json"
PROGRESS_FILE = BASE_DIR / "progress.json"
FAILED_LOG_FILE = BASE_DIR / "failed_downloads.log"

# URLs
DROPDOWN_URL = "https://dww2.tceq.texas.gov/CCR/JSP/SearchDispatch?action3=Review+Consumer+Confidence+Data"
REPORT_URL_TEMPLATE = "https://dww2.tceq.texas.gov/CCR/JSP/CCRReportDispatch?wsno={wsno}&ryear={year}&rptType=DOC&report=Generate+Report"


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='TCEQ Water Quality Report Scraper')
    parser.add_argument('--debug', action='store_true', help='Show curl commands')
    return parser.parse_args()


def load_json(filepath: Path, default=None):
    """Load JSON file or return default if not exists."""
    if filepath.exists():
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    return default if default is not None else {}


def save_json(filepath: Path, data):
    """Save data to JSON file."""
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)


def log_failure(system_id: str, year: int, error: str):
    """Log a failed download to the failure log."""
    timestamp = datetime.now().isoformat()
    with open(FAILED_LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(f"{timestamp} | {system_id} | {year} | {error}\n")


def scrape_page_data() -> tuple[list[dict], list[int]]:
    """
    Scrape the list of water systems and available years from the TCEQ dropdown page.

    Returns tuple of (systems list, years list sorted descending)
    """
    print("Fetching water systems and years from TCEQ...")

    try:
        result = subprocess.run(
            ['curl', '-s', '-f', '-L', '--max-time', str(TIMEOUT), DROPDOWN_URL],
            capture_output=True,
            text=True
        )
        if result.returncode != 0:
            print(f"Error fetching dropdown page: curl returned {result.returncode}")
            sys.exit(1)
        html_content = result.stdout
    except Exception as e:
        print(f"Error fetching dropdown page: {e}")
        sys.exit(1)

    soup = BeautifulSoup(html_content, 'html.parser')

    # Find the select element with water systems (name="wsno")
    ws_select = soup.find('select', {'name': 'wsno'})
    if not ws_select:
        print("Error: Could not find water system dropdown on page")
        sys.exit(1)

    systems = []
    for option in ws_select.find_all('option'):
        value = option.get('value', '')  # Don't strip - trailing space matters!
        text = option.get_text(strip=True)

        # Skip empty or placeholder options
        if not value.strip() or 'Select Water System' in text:
            continue

        # Value format: "TX1013549:1 MAVERICK DEVELOPMENT:Ground Water" or "TX1013549:NAME: "
        # Parts: system_id:name:water_source (water_source may be empty)
        parts = value.split(':')
        if len(parts) < 2:
            continue

        system_id = parts[0].strip()
        name = parts[1].strip() if len(parts) > 1 else ""
        water_source = parts[2].strip() if len(parts) > 2 else ""

        # Skip if no valid system ID
        if not system_id.startswith('TX'):
            continue

        systems.append({
            'system_id': system_id,
            'name': name,
            'water_source': water_source,
            'raw_value': value
        })

    # Find the select element with years (name="ryear")
    year_select = soup.find('select', {'name': 'ryear'})
    if not year_select:
        print("Error: Could not find year dropdown on page")
        sys.exit(1)

    years = []
    for option in year_select.find_all('option'):
        value = option.get('value', '').strip()
        if value and value.isdigit():
            years.append(int(value))

    # Sort years descending (newest first)
    years.sort(reverse=True)

    print(f"Found {len(systems)} water systems and {len(years)} years ({years[0]}-{years[-1]})")
    return systems, years


def get_cached_data() -> tuple[list[dict], list[int]]:
    """Get water systems and years, using cache if available."""
    cache = load_json(WATER_SYSTEMS_FILE, None)

    if cache and 'systems' in cache and 'years' in cache:
        systems = cache['systems']
        years = cache['years']
        print(f"Loaded {len(systems)} water systems and {len(years)} years from cache")
        return systems, years

    systems, years = scrape_page_data()
    save_json(WATER_SYSTEMS_FILE, {'systems': systems, 'years': years})
    print(f"Saved to {WATER_SYSTEMS_FILE}")
    return systems, years


def load_progress() -> set:
    """Load set of completed downloads."""
    data = load_json(PROGRESS_FILE, {'completed': []})
    return set(data.get('completed', []))


def save_progress(completed: set):
    """Save completed downloads to progress file."""
    save_json(PROGRESS_FILE, {'completed': sorted(completed)})


def make_download_key(system_id: str, year: int) -> str:
    """Create a unique key for a system/year combination."""
    return f"{system_id}_{year}"


def generate_download_queue(systems: list[dict], years: list[int], completed: set) -> list[tuple]:
    """
    Generate queue of (system, year) tuples to download.
    Sorted by year descending (newest first).
    """
    queue = []

    for system in systems:
        for year in years:
            key = make_download_key(system['system_id'], year)
            if key not in completed:
                queue.append((system, year))

    # Sort by year descending (newest first)
    queue.sort(key=lambda x: -x[1])

    return queue


def sanitize_filename(name: str) -> str:
    """Sanitize a string for use in a filename."""
    # Replace invalid characters with underscore
    sanitized = re.sub(r'[<>:"/\\|?*]', '_', name)
    # Replace multiple spaces/underscores with single underscore
    sanitized = re.sub(r'[\s_]+', '_', sanitized)
    # Remove leading/trailing underscores and spaces
    sanitized = sanitized.strip('_ ')
    # Truncate to reasonable length
    return sanitized[:50]


def get_system_folder_name(system: dict) -> str:
    """Generate a folder name for a water system."""
    system_id = system['system_id']
    name = system.get('name', '')

    if name:
        safe_name = sanitize_filename(name)
        return f"{system_id}_{safe_name}"
    return system_id


def is_valid_doc_file(filepath: Path) -> bool:
    """
    Check if a file is a valid .doc file (not an HTML error page).

    Real .doc files start with OLE2 magic bytes (D0 CF 11 E0).
    Error pages from TCEQ start with HTML content.
    """
    try:
        with open(filepath, 'rb') as f:
            header = f.read(256)

        # Check for OLE2 magic bytes (real .doc files)
        if header.startswith(b'\xd0\xcf\x11\xe0'):
            return True

        # Check for HTML error page signatures
        header_lower = header.lower()
        if (b'<!doctype' in header_lower or
            b'<html' in header_lower or
            b'page not found' in header_lower):
            return False

        # If it's not OLE2 and not clearly HTML, accept it cautiously
        # (some reports might be in other formats)
        return True
    except Exception:
        return False


def download_report(system: dict, year: int, debug: bool = False) -> str:
    """
    Download a single report using simple curl.

    Returns 'success', 'failed', or 'not_available'.
    """
    system_id = system['system_id']
    raw_value = system['raw_value']

    # Create system directory with descriptive name
    folder_name = get_system_folder_name(system)
    system_dir = DOWNLOADS_DIR / folder_name
    system_dir.mkdir(parents=True, exist_ok=True)

    # Output file path
    output_file = system_dir / f"{system_id}_{year}.doc"

    # Skip if file already exists with valid content
    if output_file.exists() and output_file.stat().st_size > 100:
        if is_valid_doc_file(output_file):
            return 'success'
        else:
            # Remove invalid file (HTML error page from previous run)
            if debug:
                tqdm.write(f"  Removing invalid cached file: {output_file.name}")
            output_file.unlink()

    # Build URL with encoded value (quote_plus uses + for spaces like the working curl)
    encoded_value = urllib.parse.quote_plus(raw_value)
    url = REPORT_URL_TEMPLATE.format(wsno=encoded_value, year=year)

    # Simple curl: curl {url} > file
    cmd = f'curl -s "{url}" > "{output_file}"'
    if debug:
        tqdm.write(f"  CMD: {cmd}")
    subprocess.run(cmd, shell=True, capture_output=True)

    # Check result - zero/small file means failed
    if not output_file.exists() or output_file.stat().st_size < 100:
        if output_file.exists():
            output_file.unlink()
        return 'failed'

    # Check if downloaded file is valid (not an HTML error page)
    if not is_valid_doc_file(output_file):
        if debug:
            tqdm.write(f"  Got HTML error page instead of .doc for {system_id}/{year}")
        output_file.unlink()
        return 'not_available'

    return 'success'


def download_with_retry(system: dict, year: int, debug: bool = False) -> str:
    """
    Attempt to download a report with retries.

    Returns 'success', 'failed', or 'not_available'.
    """
    system_id = system['system_id']

    for attempt in range(MAX_RETRIES):
        result = download_report(system, year, debug)
        if result == 'success':
            return 'success'

        # Report doesn't exist on TCEQ (got HTML error page) - don't retry
        if result == 'not_available':
            log_failure(system_id, year, "Report not available (404 page)")
            return 'not_available'

        # Failed (zero/small file), retry after sleep
        if attempt < MAX_RETRIES - 1:
            if debug:
                tqdm.write(f"  Attempt {attempt + 1} got empty file, retrying in {RETRY_DELAY}s...")
            time.sleep(RETRY_DELAY)

    log_failure(system_id, year, "Max retries exceeded - empty file")
    return 'failed'


def download_year_parallel(systems: list, year: int, completed: set, lock: threading.Lock,
                           debug: bool) -> tuple[int, int, int]:
    """
    Download all reports for a single year using thread pool.

    Returns tuple of (downloaded_count, failed_count, not_available_count).
    """
    # Filter to only incomplete systems for this year
    tasks = [s for s in systems if make_download_key(s['system_id'], year) not in completed]

    if not tasks:
        return 0, 0, 0

    downloaded = 0
    failed = 0
    not_available = 0
    save_counter = 0

    with tqdm(total=len(tasks), desc=f"{year}", unit="sys") as pbar:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(download_with_retry, s, year, debug): s for s in tasks}

            for future in as_completed(futures):
                system = futures[future]
                try:
                    result = future.result()
                except Exception as e:
                    result = 'failed'
                    if debug:
                        tqdm.write(f"  Exception for {system['system_id']}: {e}")

                with lock:
                    if result == 'success':
                        completed.add(make_download_key(system['system_id'], year))
                        downloaded += 1
                        save_counter += 1
                    elif result == 'not_available':
                        # Mark as completed so we don't retry missing reports
                        completed.add(make_download_key(system['system_id'], year))
                        not_available += 1
                        save_counter += 1
                    else:
                        failed += 1

                    # Batch progress saves (every 10 completions)
                    if save_counter >= 10:
                        save_progress(completed)
                        save_counter = 0

                pbar.update(1)

    # Final save for any remaining unsaved progress
    with lock:
        save_progress(completed)

    return downloaded, failed, not_available


def main():
    """Main entry point."""
    args = parse_args()

    print("=" * 60)
    print("TCEQ Water Quality Report Scraper")
    print("=" * 60)
    print()

    # Create downloads directory
    DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)

    # Get water systems and years
    systems, years = get_cached_data()
    if not systems:
        print("No water systems found. Exiting.")
        sys.exit(1)
    if not years:
        print("No years found. Exiting.")
        sys.exit(1)

    # Load progress
    completed = load_progress()
    print(f"Previously completed: {len(completed)} downloads")

    total_possible = len(systems) * len(years)

    # Count remaining by year
    remaining_by_year = {}
    for year in years:
        count = sum(1 for s in systems if make_download_key(s['system_id'], year) not in completed)
        if count > 0:
            remaining_by_year[year] = count

    total_remaining = sum(remaining_by_year.values())

    print(f"Years: {years[0]} - {years[-1]} ({len(years)} years)")
    print(f"Total downloads remaining: {total_remaining}")
    print(f"Total possible downloads: {total_possible}")
    print()

    if total_remaining == 0:
        print("All downloads complete!")
        return

    print(f"Configuration:")
    print(f"  Concurrent workers: {MAX_WORKERS}")
    print(f"  Max retries: {MAX_RETRIES}")
    print(f"  Request timeout: {TIMEOUT} seconds")
    if args.debug:
        print(f"  Debug mode: ON")
    print()
    print("Starting downloads (newest years first)...")
    print("-" * 60)

    # Thread lock for progress tracking
    lock = threading.Lock()

    downloaded_count = 0
    failed_count = 0
    not_available_count = 0

    # Process year by year (newest first - years already sorted descending)
    for year in years:
        if year not in remaining_by_year:
            continue

        year_downloaded, year_failed, year_not_available = download_year_parallel(
            systems, year, completed, lock, args.debug
        )

        downloaded_count += year_downloaded
        failed_count += year_failed
        not_available_count += year_not_available

        if year_failed > 0 or year_not_available > 0:
            parts = []
            if year_failed > 0:
                parts.append(f"{year_failed} failed")
            if year_not_available > 0:
                parts.append(f"{year_not_available} not available")
            print(f"  {year}: {', '.join(parts)} (logged to {FAILED_LOG_FILE.name})")

    print()
    print("-" * 60)
    print("Summary:")
    print(f"  Downloaded: {downloaded_count}")
    print(f"  Not available: {not_available_count}")
    print(f"  Failed: {failed_count}")
    print(f"  Total completed: {len(completed)}/{total_possible}")


if __name__ == "__main__":
    main()
