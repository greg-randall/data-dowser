import os
import json
import re
from collections import defaultdict
from pathlib import Path
from tqdm import tqdm
import multiprocessing

def process_system_dir(dir_path):
    """Worker function to process all JSONs in a single system directory."""
    results = []
    try:
        for file_entry in os.scandir(dir_path):
            if not file_entry.name.endswith('.json') or not file_entry.name.startswith('TX'):
                continue

            try:
                with open(file_entry.path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                year = data.get("year")
                if not year: continue
                
                for c in data.get("contaminants", []):
                    name = c.get("name", "").strip()
                    if not name: continue
                    clean_name = re.sub(r'\s+', ' ', name).strip()
                    mcl = c.get("mcl")
                    units = c.get("units")
                    results.append((clean_name, year, mcl, units))
            except (json.JSONDecodeError, IOError):
                continue
    except OSError:
        pass
    return results

def main():
    downloads_path = Path("downloads")
    print(f"Scanning {downloads_path} for system directories...")
    
    try:
        all_dirs = [d.path for d in os.scandir(downloads_path) if d.is_dir()]
    except OSError as e:
        print(f"Error: {e}")
        return

    total_dirs = len(all_dirs)
    print(f"Probing {total_dirs} directories using {multiprocessing.cpu_count()} cores...")
    
    # contaminant -> year -> mcl -> count
    stats = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    # contaminant -> units -> count
    unit_stats = defaultdict(lambda: defaultdict(int))

    with multiprocessing.Pool() as pool:
        for file_results in tqdm(pool.imap_unordered(process_system_dir, all_dirs), total=total_dirs, desc="Probing MCLs"):
            for name, year, mcl, units in file_results:
                stats[name][year][mcl] += 1
                unit_stats[name][units] += 1

    print("\nWriting report to mcl_analysis.txt...")
    # Sort by most common contaminants
    sorted_names = sorted(stats.keys(), key=lambda x: sum(sum(y.values()) for y in stats[x].values()), reverse=True)
    
    with open("mcl_analysis.txt", "w") as f:
        for name in sorted_names:
            f.write(f"\nCONTAMINANT: {name}\n")
            f.write(f"Common Units: {dict(unit_stats[name])}\n")
            years = sorted(stats[name].keys())
            for yr in years:
                # Format: mcl: count, sorted by count desc
                mcl_counts = sorted(stats[name][yr].items(), key=lambda x: x[1], reverse=True)
                mcl_str = ", ".join([f"{m}: {c}" for m, c in mcl_counts])
                f.write(f"  {yr}: {mcl_str}\n")

    print("Analysis complete.")

if __name__ == "__main__":
    main()
