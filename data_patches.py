"""Known data-entry errors in the source TCEQ CCRs.

The original Word documents themselves contain the bad values, so the .doc
→ HTML → JSON pipeline can't catch these — a human has to verify and list
them here. Keep entries short and cite the surrounding years.

Actions:
  "drop"           — skip this row entirely
  float(x)         — replace highest_level with x
"""

# (system_id, year, contaminant_name) -> action
PATCHES = {
    # 45262.0 ppb arsenic in 2016; surrounding years were 5-22 ppb.
    # Confirmed typo in the CCR itself. Capping to the max of other years
    # preserves the "was in violation" signal without the 2500x spike.
    ("TX0750044", 2016, "Arsenic"): 22.0,
}


def apply_patch(system_id, year, contaminant_name, highest_level):
    """Return (patched_level, should_drop). If should_drop is True, caller
    must skip this row entirely. Otherwise use patched_level."""
    try:
        year_int = int(year) if year is not None else None
    except (TypeError, ValueError):
        year_int = None
    key = (system_id, year_int, contaminant_name)
    if key not in PATCHES:
        return highest_level, False
    action = PATCHES[key]
    if action == "drop":
        return None, True
    return float(action), False
