"""Known data-entry errors in the source TCEQ CCRs.

The original Word documents themselves contain the bad values, so the .doc
→ HTML → JSON pipeline can't catch these — a human has to verify and list
them here. Keep entries short and cite the surrounding years.

Actions:
  "drop"           — skip this row entirely
  float(x)         — replace highest_level with x
"""

# (system_id, year, contaminant_name) -> (action, note)
PATCHES = {
    # 45262.0 ppb arsenic in 2016; surrounding years were 5-22 ppb.
    # Confirmed typo in the CCR itself. Capping to the max of other years
    # preserves the "was in violation" signal without the 2500x spike.
    ("TX0750044", 2016, "Arsenic"): (22.0, "The original report listed 45,262 ppb—a level that would be immediately fatal. Surrounding years show ~20 ppb, indicating a massive data-entry typo. We capped this at 22 ppb to preserve the violation signal while removing the impossible spike."),

    # Chlorite spikes (ppb misread as ppm in source reports)
    # Mineral Wells: 705 ppm is impossible for drinking water (MCL is 1.0)
    ("TX1820001", 2020, "Chlorite"): (0.705, "The source report lists 705 ppm, but also marks 'Violation: N'. Since the limit is 1.0 ppm, 705 is physically impossible for drinking water. This is a common unit error where 705 ppb (0.705 ppm) was entered as ppm."),
    ("TX1820069", 2021, "Chlorite"): (0.658, "Unit Error: Source lists 658 ppm but marks 'Violation: False'. The actual value is almost certainly 658 ppb (0.658 ppm)."),
    ("TX2350002", 2024, "Chlorite"): (0.440, "Unit Error: Source lists 440 ppm but marks 'Violation: False'. The actual value is almost certainly 440 ppb (0.440 ppm)."),
    ("TX0540015", 2021, "Chlorite"): (0.304, "Unit Error: Source lists 304 ppm but marks 'Violation: False'. The actual value is almost certainly 304 ppb (0.304 ppm)."),
    ("TX1080001", 2021, "Chlorite"): (0.286, "Unit Error: Source lists 286 ppm but marks 'Violation: False'. The actual value is almost certainly 286 ppb (0.286 ppm)."),
    ("TX1080003", 2021, "Chlorite"): (0.132, "Unit Error: Source lists 132 ppm but marks 'Violation: False'. The actual value is almost certainly 132 ppb (0.132 ppm)."),
    ("TX1440001", 2021, "Chlorite"): (0.0299, "Unit Error: Source lists 29.9 ppm but marks 'Violation: False'. The actual value is almost certainly 29.9 ppb (0.0299 ppm)."),
}


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
