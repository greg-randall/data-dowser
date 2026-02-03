#!/usr/bin/env python3
"""List all contaminants and their categorization status."""

import json
import os
import sys
from build_dashboard_data import CONTAMINANT_CATEGORIES, categorize_contaminant

def main():
    # Load data
    # Try the details file first (new format), then fallback to legacy
    paths = ['dashboard_data_details.json', 'dashboard_data.json']
    data = None
    
    for p in paths:
        if os.path.exists(p):
            try:
                with open(p, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                print(f"Loaded data from {p}")
                break
            except Exception:
                continue
    
    if not data:
        print("Error: Could not find dashboard data file. Run build_dashboard_data.py first.")
        sys.exit(1)

    # Handle different formats
    if 'cl' in data:
        contaminants = data['cl']
    elif 'contaminant_list' in data:
        contaminants = data['contaminant_list']
    else:
        print("Error: Could not find contaminant list in data file.")
        sys.exit(1)

    contaminants = sorted(contaminants)

    # Group by categorization status
    categorized = {}
    uncategorized = []

    for name in contaminants:
        cats = categorize_contaminant(name)
        if cats:
            for cat in cats:
                categorized.setdefault(cat, []).append(name)
        else:
            uncategorized.append(name)

    # Print results
    print("=== CATEGORIZED ===")
    for cat, names in sorted(categorized.items()):
        print(f"\n{cat}:")
        for n in names:
            print(f"  {n}")

    print(f"\n=== UNCATEGORIZED ({len(uncategorized)}) ===")
    for n in uncategorized:
        print(f"  {n}")

if __name__ == "__main__":
    main()
