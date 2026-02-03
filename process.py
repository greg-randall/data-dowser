#!/usr/bin/env python3
"""
TCEQ Water Quality Report Processor

Converts .doc files to HTML via Word, then extracts structured JSON data.
Full pipeline: .doc → .html → .json

Usage:
    python process.py --input downloads/
    python process.py --input downloads/ --keep-html  # Don't delete HTML after extraction
"""

import argparse
import json
import multiprocessing
import os
import re
import shutil
import subprocess
import sys
import time
import select
from pathlib import Path
from bs4 import BeautifulSoup
from tqdm import tqdm


# =============================================================================
# Path Utilities
# =============================================================================

def wsl_to_windows_path(path: Path) -> str:
    """Convert WSL path to Windows path."""
    path_str = str(path.resolve())
    if path_str.startswith("/mnt/") and len(path_str) > 6:
        drive_letter = path_str[5].upper()
        rest = path_str[6:].replace("/", "\\")
        return f"{drive_letter}:{rest}"
    return path_str


# =============================================================================
# Logging & Error Handling
# =============================================================================

FAILED_LOG = Path("failed_conversions.log")

def log_failure(doc_path: Path, reason: str = "Timeout"):
    """Log a failed conversion to avoid infinite retries."""
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    with open(FAILED_LOG, "a", encoding="utf-8") as f:
        f.write(f"{timestamp} | {doc_path.name} | {reason}\n")

def get_failed_files() -> set[str]:
    """Return set of filenames that previously failed."""
    if not FAILED_LOG.exists():
        return set()
    failed = set()
    with open(FAILED_LOG, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.split("|")
            if len(parts) >= 2:
                failed.add(parts[1].strip())
    return failed


# =============================================================================
# Word Conversion
# =============================================================================

def convert_worker(args: tuple[list[Path], int, bool]) -> list[Path]:
    """
    Worker that converts .doc files to HTML using Word COM.
    Returns list of successfully created HTML files.
    """
    file_list, worker_id, force_regenerate = args

    if not file_list:
        return []

    # Build file paths for PowerShell
    conversions = []
    skipped_html = []
    
    for doc_file in file_list:
        html_path = doc_file.with_suffix('.html')
        
        # If HTML exists and we aren't forcing regeneration, skip conversion
        if html_path.exists() and not force_regenerate:
            skipped_html.append(html_path)
            continue
            
        win_doc = wsl_to_windows_path(doc_file).replace("'", "''")
        win_html = wsl_to_windows_path(html_path).replace("'", "''")
        conversions.append((win_doc, win_html, html_path))

    # If everything was skipped, return early
    if not conversions:
        return skipped_html

    # Build PowerShell script
    files_array = "@(" + ",".join(
        f"@('{doc}','{html}')" for doc, html, _ in conversions
    ) + ")"

    ps_script = f"""
$ErrorActionPreference = 'Stop'
$conversions = {files_array}
$word = $null
try {{
    $before = @(Get-Process WINWORD -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Id)
    $word = New-Object -ComObject Word.Application
    $after = @(Get-Process WINWORD -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Id)
    $wordPid = $after | Where-Object {{ $before -notcontains $_ }} | Select-Object -First 1
    if ($wordPid) {{ Write-Output "PID::$wordPid" }}
    
    $word.Visible = $false
    $word.DisplayAlerts = 0

    foreach ($pair in $conversions) {{
        $docPath = $pair[0]
        $htmlPath = $pair[1]
        Write-Output "START::$docPath"
        $doc = $null
        try {{
            $doc = $word.Documents.Open($docPath, $false, $true)
            $doc.SaveAs([ref]$htmlPath, [ref]8)
            Write-Output "DONE::$htmlPath"
        }} catch {{
            Write-Error "Failed: $docPath - $_"
        }} finally {{
            if ($doc) {{
                $doc.Close([ref]0)
                [System.Runtime.InteropServices.Marshal]::ReleaseComObject($doc) | Out-Null
            }}
        }}
    }}
}} finally {{
    if ($word) {{
        $word.Quit()
        [System.Runtime.InteropServices.Marshal]::ReleaseComObject($word) | Out-Null
    }}
    [System.GC]::Collect()
    [System.GC]::WaitForPendingFinalizers()
}}
"""

    # Run PowerShell with watchdog
    proc = subprocess.Popen(
        ["powershell.exe", "-NoProfile", "-NonInteractive", "-Command", ps_script],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    )

    last_activity = time.time()
    TIMEOUT = 45  # Increased slightly
    current_processing_file = None
    word_pid = None

    # Map Windows paths back to original Path objects for logging
    win_path_map = {win_doc: orig_path for win_doc, _, orig_path in conversions}

    while True:
        # Check if process finished
        if proc.poll() is not None:
            break

        # Check for output with 1s timeout
        reads, _, _ = select.select([proc.stdout], [], [], 1.0)
        
        if reads:
            line = proc.stdout.readline()
            if not line:
                break
            
            # Capture Word PID
            if "PID::" in line:
                try:
                    word_pid = line.split("PID::", 1)[1].strip()
                except Exception:
                    pass

            # Reset timer on significant progress and track file
            if "START::" in line:
                last_activity = time.time()
                win_path = line.split("START::", 1)[1].strip()
                current_processing_file = win_path 
            elif "DONE::" in line:
                last_activity = time.time()
                current_processing_file = None

        # Check watchdog
        if time.time() - last_activity > TIMEOUT:
            proc.kill()
            print(f"\nWorker {worker_id}: Timeout processing {current_processing_file or 'startup'}", file=sys.stderr)
            
            # Kill the specific Word process if we have its PID
            if word_pid:
                try:
                    subprocess.run(["taskkill.exe", "/F", "/PID", word_pid], capture_output=True)
                except Exception:
                    pass
            
            # Try to find the Path object for the stuck file
            failed_path = None
            if current_processing_file:
                failed_path = win_path_map.get(current_processing_file)
                if not failed_path:
                    for wp, op in win_path_map.items():
                        if str(op.name) in current_processing_file:
                            failed_path = op
                            break
            
            if failed_path:
                log_failure(failed_path, "Timeout (>45s)")
            break

    # Ensure process is dead
    if proc.poll() is None:
        proc.kill()
        if word_pid:
            try:
                subprocess.run(["taskkill.exe", "/F", "/PID", word_pid], capture_output=True)
            except Exception:
                pass

    # Return list of HTML files (both newly created and skipped ones)
    newly_created = [html_path for _, _, html_path in conversions if html_path.exists()]
    return skipped_html + newly_created


# =============================================================================
# HTML Parsing / JSON Extraction
# =============================================================================

def parse_system_info(soup: BeautifulSoup, filename: str) -> dict:
    """Extract system ID, name, year, and water source from report."""
    info = {
        'system_id': None,
        'system_name': None,
        'year': None,
        'water_source': None
    }

    match = re.match(r'(TX\d+)_(\d{4})\.html', filename)
    if match:
        info['system_id'] = match.group(1)
        info['year'] = int(match.group(2))

    text = soup.get_text()

    header_match = re.search(
        r'(\d{4})\s+Consumer\s+Confidence\s+Report\s+for\s+Public\s+Water\s+System\s+(.+?)(?:\s+This|\s+provides|\n)',
        text, re.IGNORECASE | re.DOTALL
    )
    if header_match:
        if info['year'] is None:
            info['year'] = int(header_match.group(1))
        name = header_match.group(2).strip()
        name = re.sub(r'\s+', ' ', name)
        if name and name.lower() != 'null':
            info['system_name'] = name

    source_match = re.search(
        r'provides\s+(Ground\s+Water|Surface\s+Water)\s+from',
        text, re.IGNORECASE
    )
    if source_match:
        info['water_source'] = source_match.group(1).title()

    return info


def categorize_contaminant(name: str) -> str:
    """Determine the category of a contaminant."""
    name_lower = name.lower()

    if 'lead' in name_lower or 'copper' in name_lower:
        return 'Lead and Copper'
    if 'coliform' in name_lower or 'e. coli' in name_lower or 'e.coli' in name_lower:
        return 'Coliform Bacteria'
    if 'turbidity' in name_lower:
        return 'Turbidity'
    if 'organic carbon' in name_lower or 'toc' in name_lower:
        return 'Total Organic Carbon'
    if any(x in name_lower for x in ['haa5', 'haloacetic', 'tthm', 'trihalomethane', 'chlorite', 'bromate']):
        return 'Disinfection By-Products'
    if any(x in name_lower for x in ['radium', 'uranium', 'alpha', 'beta', 'gross']):
        return 'Radioactive Contaminants'
    if any(x in name_lower for x in ['benzene', 'toluene', 'xylene', 'ethylbenzene', 'styrene',
                                      'tetrachloroethylene', 'trichloroethylene', 'vinyl chloride',
                                      'carbon tetrachloride', 'dichloromethane', 'chlorobenzene']):
        return 'Volatile Organic Contaminants'
    if any(x in name_lower for x in ['barium', 'fluoride', 'nitrate', 'nitrite', 'arsenic', 'selenium',
                                      'cadmium', 'chromium', 'mercury', 'antimony', 'beryllium',
                                      'thallium', 'cyanide']):
        return 'Inorganic Contaminants'

    return 'Other'


def parse_numeric(value: str) -> float | None:
    """Parse a numeric value from a cell."""
    if not value:
        return None
    value = value.strip()
    if value.lower() in ['', 'na', 'n/a', '-']:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def parse_range(value: str) -> tuple:
    """Parse a range value like '0.012 - 0.049' into (low, high)."""
    if not value:
        return (None, None)
    match = re.match(r'([\d.]+)\s*-\s*([\d.]+)', value.strip())
    if match:
        try:
            return (float(match.group(1)), float(match.group(2)))
        except ValueError:
            pass
    return (None, None)


def is_contaminant_table(header_row: list) -> str | None:
    """Check if a table header indicates a contaminant data table."""
    header_text = ' '.join(header_row).lower()

    if 'lead and copper' in header_text:
        return 'lead_copper'
    if 'collection date' in header_text and ('highest level' in header_text or 'range' in header_text):
        return 'standard'
    if 'date sampled' in header_text and '90th percentile' in header_text:
        return 'lead_copper'

    return None


def parse_standard_row(cells: list, section_name: str) -> dict | None:
    """Parse a standard contaminant table row."""
    if len(cells) < 7:
        return None

    contaminant = cells[0].strip()
    if not contaminant or contaminant.lower() in ['', 'contaminant', 'contamination']:
        return None

    if any(x in contaminant.lower() for x in ['inorganic contaminants', 'disinfection by-products',
                                               'volatile organic', 'radioactive', 'coliform']):
        return None

    result = {
        'name': contaminant,
        'category': section_name if section_name else categorize_contaminant(contaminant),
        'collection_date': cells[1].strip() if len(cells) > 1 else None,
        'highest_level': parse_numeric(cells[2]) if len(cells) > 2 else None,
        'range_low': None,
        'range_high': None,
        'mclg': None,
        'mcl': None,
        'units': None,
        'violation': None,
        'source': None
    }

    if len(cells) > 3:
        low, high = parse_range(cells[3])
        result['range_low'] = low
        result['range_high'] = high

    if len(cells) > 4:
        mclg_text = cells[4].strip().lower()
        if 'no goal' not in mclg_text:
            result['mclg'] = parse_numeric(cells[4])

    if len(cells) > 5:
        result['mcl'] = parse_numeric(cells[5])

    if len(cells) > 6:
        units = cells[6].strip().lower()
        if units in ['ppm', 'ppb', 'pci/l', 'ntu', 'mrem', 'mfl', 'ppt', 'ppq', 'mg/l']:
            result['units'] = units

    if len(cells) > 7:
        viol = cells[7].strip().upper()
        if viol in ['Y', 'N']:
            result['violation'] = viol == 'Y'

    if len(cells) > 8:
        source = cells[8].strip()
        source = re.sub(r'\s+', ' ', source)
        if len(source) > 10:
            result['source'] = source

    return result


def parse_lead_copper_row(cells: list) -> dict | None:
    """Parse a Lead and Copper table row."""
    if len(cells) < 7:
        return None

    contaminant = cells[0].strip()
    if not contaminant or contaminant.lower() in ['', 'lead and copper']:
        return None

    result = {
        'name': contaminant,
        'category': 'Lead and Copper',
        'collection_date': cells[1].strip() if len(cells) > 1 else None,
        'highest_level': None,
        'range_low': None,
        'range_high': None,
        'mclg': parse_numeric(cells[2]) if len(cells) > 2 else None,
        'mcl': parse_numeric(cells[3]) if len(cells) > 3 else None,
        'units': None,
        'violation': None,
        'source': None
    }

    if len(cells) > 4:
        result['highest_level'] = parse_numeric(cells[4])

    if len(cells) > 6:
        units = cells[6].strip().lower()
        if units in ['ppm', 'ppb']:
            result['units'] = units

    if len(cells) > 7:
        viol = cells[7].strip().upper()
        if viol in ['Y', 'N']:
            result['violation'] = viol == 'Y'

    if len(cells) > 8:
        source = cells[8].strip()
        source = re.sub(r'\s+', ' ', source)
        if len(source) > 10:
            result['source'] = source

    return result


def is_data_row(cells: list) -> bool:
    """Check if a row contains contaminant data (not a header)."""
    if not cells or len(cells) < 5:
        return False

    first = cells[0].lower()
    contaminant_patterns = [
        'barium', 'fluoride', 'nitrate', 'nitrite', 'arsenic', 'selenium',
        'cadmium', 'chromium', 'mercury', 'antimony', 'beryllium', 'thallium',
        'cyanide', 'copper', 'lead', 'haa5', 'haloacetic', 'tthm', 'trihalomethane',
        'chlorite', 'bromate', 'benzene', 'toluene', 'xylene', 'ethylbenzene',
        'styrene', 'tetrachloroethylene', 'trichloroethylene', 'vinyl chloride',
        'radium', 'uranium', 'alpha', 'beta', 'gross', 'coliform', 'e. coli',
        'turbidity', 'carbon tetrachloride', 'dichloromethane', 'chlorobenzene'
    ]

    return any(p in first for p in contaminant_patterns)


def parse_contaminants(soup: BeautifulSoup) -> list:
    """Extract all contaminant data from HTML tables."""
    contaminants = []
    tables = soup.find_all('table')

    for table in tables:
        rows = table.find_all('tr')
        if not rows:
            continue

        first_row_cells = [td.get_text(strip=True) for td in rows[0].find_all(['td', 'th'])]
        if not first_row_cells:
            continue

        table_type = is_contaminant_table(first_row_cells)
        start_idx = 1

        if not table_type:
            if is_data_row(first_row_cells):
                table_type = 'standard'
                start_idx = 0
            else:
                continue

        section_name = None
        first_cell = first_row_cells[0].lower() if first_row_cells else ''
        if 'inorganic' in first_cell:
            section_name = 'Inorganic Contaminants'
        elif 'disinfection' in first_cell:
            section_name = 'Disinfection By-Products'
        elif 'volatile' in first_cell:
            section_name = 'Volatile Organic Contaminants'
        elif 'radioactive' in first_cell:
            section_name = 'Radioactive Contaminants'
        elif 'lead' in first_cell or 'copper' in first_cell:
            section_name = 'Lead and Copper'

        for row in rows[start_idx:]:
            cells = [td.get_text(strip=True) for td in row.find_all(['td', 'th'])]

            if not cells or all(c == '' for c in cells):
                continue

            if table_type == 'lead_copper':
                parsed = parse_lead_copper_row(cells)
            else:
                parsed = parse_standard_row(cells, section_name)

            if parsed and parsed['name']:
                is_dup = any(
                    c['name'] == parsed['name'] and c['collection_date'] == parsed['collection_date']
                    for c in contaminants
                )
                if not is_dup:
                    contaminants.append(parsed)

    return contaminants


def extract_html_to_json(html_path: Path) -> bool:
    """Extract data from HTML and save as JSON. Returns success."""
    try:
        html = html_path.read_text(encoding='utf-8', errors='ignore')
        soup = BeautifulSoup(html, 'html.parser')

        info = parse_system_info(soup, html_path.name)
        contaminants = parse_contaminants(soup)

        data = {
            'system_id': info['system_id'],
            'system_name': info['system_name'],
            'year': info['year'],
            'water_source': info['water_source'],
            'contaminants': contaminants,
        }

        json_path = html_path.with_suffix('.json')
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        return True
    except Exception as e:
        print(f"Extract failed {html_path.name}: {e}", file=sys.stderr)
        return False


# =============================================================================
# Main Pipeline
# =============================================================================

def clean_html_artifacts(folder: Path, doc_files: list[Path]) -> None:
    """Remove HTML files and *_files/ directories for given doc files."""
    for doc_file in doc_files:
        html_file = doc_file.with_suffix('.html')
        if html_file.exists():
            html_file.unlink()
        files_dir = doc_file.with_suffix('') / '_files'
        # Actually it's name_files not name/_files
        files_dir = folder / (doc_file.stem + '_files')
        if files_dir.exists() and files_dir.is_dir():
            shutil.rmtree(files_dir)


def main():
    parser = argparse.ArgumentParser(
        description="Process .doc files: convert to HTML, extract to JSON"
    )
    parser.add_argument(
        "--input", "-i",
        type=Path,
        required=True,
        help="Folder containing .doc files"
    )
    parser.add_argument(
        "--delete-html",
        action="store_true",
        help="Delete intermediate HTML files after extraction (default: keep)"
    )
    parser.add_argument(
        "--force-html-regenerate",
        action="store_true",
        help="Regenerate HTML even if it already exists"
    )
    parser.add_argument(
        "--threads", "-t",
        type=int,
        default=2,
        help="Number of parallel Word workers (default: 2)"
    )
    parser.add_argument(
        "--batch-size", "-b",
        type=int,
        default=50,
        help="Files per batch (default: 50)"
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Show processing statistics and exit"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of files to process (for testing)"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force regeneration of HTML and JSON (ignore existing JSON)"
    )
    parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="Retry files that previously failed (ignore failed_conversions.log)"
    )

    args = parser.parse_args()

    if not args.input.exists():
        print(f"Error: Folder '{args.input}' does not exist", file=sys.stderr)
        sys.exit(1)

    # Get file counts (search recursively)
    doc_files = []
    json_files = []
    html_files = []

    print("Scanning files...", file=sys.stderr)
    input_str = str(args.input)
    
    for root, dirs, files in os.walk(input_str):
        # Prune directories to skip: _files folders and hidden folders
        dirs[:] = [d for d in dirs if not d.endswith('_files') and not d.startswith('.')]
        
        for name in files:
            lower_name = name.lower()
            if lower_name.endswith('.doc'):
                doc_files.append(Path(root) / name)
            elif lower_name.endswith('.json'):
                json_files.append(Path(root) / name)
            elif lower_name.endswith('.html'):
                html_files.append(Path(root) / name)

    def extract_year_from_filename(path: Path) -> int:
        """Extract year from filename like TX1234567_2024.doc"""
        match = re.search(r'_(\d{4})\.doc$', path.name, re.IGNORECASE)
        return int(match.group(1)) if match else 0

    # Sort by year descending (newest first), then by path
    doc_files.sort(key=lambda f: (-extract_year_from_filename(f), str(f)))
    
    # Load failed files
    failed_files = set()
    if not args.retry_failed:
        failed_files = get_failed_files()

    if args.stats:
        print(f"Total .doc files:  {len(doc_files)}")
        print(f"Existing .json:    {len(json_files)}")
        print(f"Existing .html:    {len(html_files)}")
        print(f"Previously failed: {len(failed_files)}")
        remaining = len([f for f in doc_files if f.stem not in {j.stem for j in json_files} and f.name not in failed_files])
        print(f"Remaining:         {remaining}")
        return

    # Find docs that need processing (no corresponding JSON and not failed)
    existing_json_stems = {f.stem for f in json_files}
    pending_docs = [
        f for f in doc_files 
        if f.stem not in existing_json_stems 
        and (args.retry_failed or f.name not in failed_files)
    ]

    # Apply limit if specified
    if args.limit:
        pending_docs = pending_docs[:args.limit]

    if not pending_docs:
        print("All files already processed (or skipped due to previous failures)!")
        print(f"Total JSON files: {len(json_files)}")
        if not args.retry_failed and failed_files:
            print(f"Skipped {len(failed_files)} failed files. Use --retry-failed to try them again.")
        return

    print(f"Processing {len(pending_docs)} .doc files → JSON")
    print(f"Threads: {args.threads}, Batch size: {args.batch_size}")
    print()

    start_time = time.time()
    total_converted = 0
    total_extracted = 0

    # Split into chunks for workers
    chunk_size = args.batch_size
    chunks = [pending_docs[i:i + chunk_size] for i in range(0, len(pending_docs), chunk_size)]
    worker_args = [(chunk, i, args.force_html_regenerate) for i, chunk in enumerate(chunks)]

    with multiprocessing.Pool(args.threads) as pool:
        # Use imap_unordered for better performance as we can process results as they come in
        for html_created in tqdm(pool.imap_unordered(convert_worker, worker_args), total=len(chunks), desc="Processing Batches"):
            total_converted += len(html_created)

            # Extract .html → .json
            for html_path in html_created:
                if extract_html_to_json(html_path):
                    total_extracted += 1

                    # Clean up HTML and _files directory
                    if args.delete_html:
                        html_path.unlink(missing_ok=True)
                        files_dir = html_path.parent / (html_path.stem + '_files')
                        if files_dir.exists():
                            shutil.rmtree(files_dir)

    # Final summary
    elapsed = time.time() - start_time
    print()
    print("=" * 50)
    print(f"Converted: {total_converted} HTML files")
    print(f"Extracted: {total_extracted} JSON files")
    print(f"Time: {elapsed/60:.1f} minutes ({elapsed/3600:.1f} hours)")
    print(f"Rate: {total_extracted/elapsed:.2f} files/second")
    print("=" * 50)


if __name__ == "__main__":
    main()
