"""Known data-entry errors in the source TCEQ CCRs.

Corrections are stored in data_patches.yaml for transparency.
This script loads those patches and applies them during data processing.
"""

import yaml
from pathlib import Path

def load_patches():
    """Load data patches from YAML file."""
    yaml_path = Path(__file__).parent / "data_patches.yaml"
    if not yaml_path.exists():
        return {}
    
    with open(yaml_path, 'r', encoding='utf-8') as f:
        data = yaml.safe_load(f)
        if not data:
            return {}
            
    # Convert list of dicts to the internal (sid, year, name) -> (action, note) map
    patches_map = {}
    for p in data:
        key = (p['system_id'], p['year'], p['contaminant'])
        patches_map[key] = (p['action'], p['note'])
    return patches_map


# Global lookup table
PATCHES = load_patches()


def apply_patch(system_id, year, contaminant_name, highest_level):
    """Return (patched_level, should_drop, note). If should_drop is True, caller
    must skip this row entirely. Otherwise use patched_level."""
    try:
        year_int = int(year) if year is not None else None
    except (TypeError, ValueError):
        year_int = None
        
    key = (system_id, year_int, contaminant_name)
    if key not in PATCHES:
        return highest_level, False, None
        
    action, note = PATCHES[key]
    if action == "drop":
        return None, True, note
        
    return float(action), False, note
