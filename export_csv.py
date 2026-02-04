import json
import csv
import re
from pathlib import Path


def clean_field(text):
    """Normalize whitespace and remove newlines from text fields."""
    if not isinstance(text, str):
        return text
    # Replace newlines and tabs with space
    text = text.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
    # Collapse multiple spaces
    return re.sub(r'\s+', ' ', text).strip()


def main():
    print("Loading JSON data...")
    # Load dashboard data (already aggregated)
    with open('dashboard_data_map.json') as f:
        map_data = json.load(f)
    with open('dashboard_data_details.json') as f:
        details = json.load(f)

    print(f"Loaded {len(map_data['s'])} map systems and {len(details['d'])} detail records.")

    # Build system lookup from map data
    systems = {s['i']: s for s in map_data['s']}

    # Get contaminant metadata
    contaminant_meta = details['m']

    row_count = 0
    print("Writing CSV...")
    with open('texas_water_quality.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'system_id', 'system_name', 'county', 'latitude', 'longitude',
            'population', 'year', 'water_source', 'contaminant', 'category',
            'highest_level', 'mcl', 'mclg', 'units', 'violation'
        ])

        for sys_id, sys_details in details['d'].items():
            map_sys = systems.get(sys_id, {})

            for year, year_data in sys_details.get('y', {}).items():
                violations = set(year_data.get('v', []))

                for contaminant, level in year_data.get('c', {}).items():
                    meta = contaminant_meta.get(contaminant, {})

                    writer.writerow([
                        sys_id,
                        clean_field(sys_details.get('n', '')),
                        clean_field(sys_details.get('c', '')),
                        map_sys.get('la', ''),
                        map_sys.get('lo', ''),
                        map_sys.get('p', ''),
                        year,
                        clean_field(sys_details.get('ws', '')),
                        clean_field(contaminant),
                        clean_field(meta.get('ca', '')),
                        level,
                        meta.get('m', ''),
                        meta.get('g', ''),
                        clean_field(meta.get('u', '')),
                        contaminant in violations
                    ])
                    row_count += 1

    print(f"Exported {row_count:,} rows to texas_water_quality.csv")


if __name__ == "__main__":
    main()
