"""Build a worst-to-best water quality leaderboard from the flat CSV.

Primary sort: severity_sum (sum of level/mcl across all violation rows).
Tiebreakers: violation_count DESC, avg_severity DESC.
"""
import argparse
import csv
import json
import math
import re
import sys
from pathlib import Path
from titlecase import titlecase

from data_patches import apply_patch

CSV_PATH = Path("raw-data/texas_water_quality.csv")
OUT_PATH = Path("leaderboard.json")

# Composite score weights (must sum to 1.0). Adjust here to retune.
SEVERITY_WEIGHT = 0.7
IMPACT_WEIGHT = 0.3

CITY_RE = re.compile(r"^\s*(?:CITY|TOWN|VILLAGE)\s+OF\s+(.+?)\s*$", re.IGNORECASE)


def acronym_callback(word, **kwargs):
    # Specific water industry acronyms to keep capped
    acronyms = {"WSC", "WCID", "MUD", "SUD", "FWSD", "PWS", "ISD", "VFD"}
    if word.upper() in acronyms:
        return word.upper()
    # If it's a single letter like 'A', 'B', etc. (common in unit names)
    if len(word) == 1 and word.isupper():
        return word
    return None


def smart_title(text):
    if not text:
        return ""
    # titlecase handles prepositions; callback handles technical acronyms
    return titlecase(text, callback=acronym_callback)


def extract_city(system_name):
    if not system_name:
        return ""
    m = CITY_RE.match(system_name)
    if not m:
        return ""
    return smart_title(m.group(1).strip())


def to_float(s):
    if s is None or s == "":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", default=str(CSV_PATH), help="Path to flat CSV")
    parser.add_argument("--out", default=str(OUT_PATH), help="Output JSON path")
    parser.add_argument("--metadata", default="water_system_data_full_profile.json",
                        help="Supplementary system metadata JSON path")
    parser.add_argument("--min-year", type=int, default=None,
                        help="Inclusive lower bound on reporting year")
    parser.add_argument("--max-year", type=int, default=None,
                        help="Inclusive upper bound on reporting year")
    args = parser.parse_args()

    if args.min_year is not None and args.max_year is not None and args.min_year > args.max_year:
        print(f"ERROR: --min-year ({args.min_year}) > --max-year ({args.max_year})", file=sys.stderr)
        sys.exit(1)

    csv_path = Path(args.csv)
    if not csv_path.exists():
        print(f"ERROR: CSV not found at {csv_path}", file=sys.stderr)
        sys.exit(1)

    # Load supplementary metadata if available
    metadata_path = Path(args.metadata)
    supplementary_meta = {}
    if metadata_path.exists():
        with open(metadata_path, 'r', encoding='utf-8') as f:
            supplementary_meta = json.load(f)
        print(f"Loaded supplementary metadata from {metadata_path}")

    systems = {}
    total_rows = 0
    total_violations = 0
    rows_skipped_by_year = 0

    print(f"Reading {csv_path}...")
    if args.min_year is not None or args.max_year is not None:
        lo = args.min_year if args.min_year is not None else "-inf"
        hi = args.max_year if args.max_year is not None else "+inf"
        print(f"Year filter: [{lo}, {hi}]")
    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if args.min_year is not None or args.max_year is not None:
                year_raw = row.get("year")
                try:
                    year = int(year_raw) if year_raw else None
                except ValueError:
                    year = None
                if year is None:
                    rows_skipped_by_year += 1
                    continue
                if args.min_year is not None and year < args.min_year:
                    rows_skipped_by_year += 1
                    continue
                if args.max_year is not None and year > args.max_year:
                    rows_skipped_by_year += 1
                    continue
            total_rows += 1
            sid = row["system_id"]
            if sid not in systems:
                # Use supplementary metadata for initial values if available
                meta = supplementary_meta.get(sid, {}).get("meta", {})
                pop_supp = to_float(meta.get("population", "").replace(",", ""))
                county_supp = meta.get("county", "").upper() if meta.get("county") else None

                systems[sid] = {
                    "system_id": sid,
                    "system_name": smart_title(row["system_name"]),
                    "city": extract_city(row["system_name"]),
                    "county": smart_title(row["county"]) if row["county"] and "NO SITE" not in row["county"].upper() else smart_title(county_supp or row["county"]),
                    "latitude": to_float(row["latitude"]),
                    "longitude": to_float(row["longitude"]),
                    "coord_source": None,
                    "population": pop_supp,
                    "violation_count": 0,
                    "severity_sum": 0.0,
                    "violations_missing_mcl": 0,
                    "worst_violation": None,
                    "worst_severity": 0.0,
                    "active_years": set(),
                }
            s = systems[sid]

            year_raw = row.get("year")
            if year_raw:
                s["active_years"].add(year_raw)

            pop = to_float(row["population"])
            if pop is not None and (s["population"] is None or pop > s["population"]):
                s["population"] = pop

            if row["violation"] != "True":
                continue

            level = to_float(row["highest_level"])
            level, dropped = apply_patch(sid, row.get("year"), row.get("contaminant"), level)
            if dropped:
                continue

            total_violations += 1
            s["violation_count"] += 1

            mcl = to_float(row["mcl"])
            if level is None or mcl is None or mcl <= 0:
                s["violations_missing_mcl"] += 1
                continue

            severity = level / mcl
            s["severity_sum"] += severity
            if severity > s["worst_severity"]:
                s["worst_severity"] = severity
                s["worst_violation"] = {
                    "contaminant": smart_title(row["contaminant"]),
                    "year": int(row["year"]) if row["year"] else None,
                    "level": level,
                    "mcl": mcl,
                    "units": row["units"],
                    "severity": severity,
                }

    # Merge supplementary coordinates for systems without them in the CSV.
    # geocoded_coordinates.json is produced by geocode_missing.py.
    geocoded_path = Path("geocoded_coordinates.json")
    geocoded_count = 0
    if geocoded_path.exists():
        geocoded = json.load(geocoded_path.open())
        for sid, coords in geocoded.items():
            if sid in systems and systems[sid]["latitude"] is None:
                systems[sid]["latitude"] = coords["lat"]
                systems[sid]["longitude"] = coords["lon"]
                systems[sid]["coord_source"] = coords.get("source")
                geocoded_count += 1
        print(f"Merged {geocoded_count} geocoded coordinates from {geocoded_path}")

    for s in systems.values():
        scored = s["violation_count"] - s["violations_missing_mcl"]
        s["avg_severity"] = (s["severity_sum"] / scored) if scored > 0 else None
        
        # Years this system has been reporting
        y_count = len(s["active_years"])
        s["active_years_count"] = y_count
        s["annual_severity"] = (s["severity_sum"] / y_count) if y_count > 0 else 0
        # Clean up the set before JSON export
        s.pop("active_years", None)

        s["impact_score"] = (
            s["population"] * s["severity_sum"]
            if s["population"] and s["severity_sum"] > 0
            else None
        )

    # Log-normalized composite score. Log tames the right-skew (e.g. a single
    # data-error reading with severity=4500 doesn't crush everyone else to ~0).
    # Log-normalized composite score. Log tames the right-skew (e.g. a single
    # data-error reading with severity=4500 doesn't crush everyone else to ~0).
    # composite_score is null when population is missing — honest about coverage
    # rather than silently treating unknown-pop as 0 impact.
    sev_log_max = max(
        (math.log1p(s["severity_sum"]) for s in systems.values() if s["severity_sum"] > 0),
        default=0,
    )
    imp_log_max = max(
        (math.log1p(s["impact_score"]) for s in systems.values() if s["impact_score"]),
        default=0,
    )

    for s in systems.values():
        s["severity_norm"] = (
            math.log1p(s["severity_sum"]) / sev_log_max
            if sev_log_max > 0 and s["severity_sum"] > 0
            else 0.0
        )
        s["impact_norm"] = (
            math.log1p(s["impact_score"]) / imp_log_max
            if imp_log_max > 0 and s["impact_score"]
            else None
        )
        if s["impact_norm"] is None:
            s["composite_score"] = None
        else:
            s["composite_score"] = (
                SEVERITY_WEIGHT * s["severity_norm"]
                + IMPACT_WEIGHT * s["impact_norm"]
            )

    ranked = sorted(
        systems.values(),
        key=lambda s: (
            -(s["composite_score"] if s["composite_score"] is not None else (SEVERITY_WEIGHT * s["severity_norm"])),
            -s["severity_sum"],
            -s["violation_count"],
        ),
    )
    for i, s in enumerate(ranked, 1):
        s["rank"] = i
        # drop internal helper
        s.pop("worst_severity", None)

    systems_with_violations = sum(1 for s in ranked if s["violation_count"] > 0)
    systems_with_missing_mcl = sum(1 for s in ranked if s["violations_missing_mcl"] > 0)

    out = {
        "meta": {
            "source_csv": str(csv_path),
            "min_year": args.min_year,
            "max_year": args.max_year,
            "rows_skipped_by_year_filter": rows_skipped_by_year,
            "total_rows": total_rows,
            "total_violations": total_violations,
            "systems_total": len(ranked),
            "systems_with_violations": systems_with_violations,
            "systems_with_missing_mcl_violations": systems_with_missing_mcl,
            "sort": "composite_score DESC (fallback to severity_norm), severity_sum DESC",
            "severity_formula": "highest_level / mcl per violation row",
            "impact_formula": "population * severity_sum (null if population missing)",
            "composite_formula": (
                f"{SEVERITY_WEIGHT} * log-normalized severity_sum "
                f"+ {IMPACT_WEIGHT} * log-normalized impact_score "
                "(null if population missing)"
            ),
        },
        "systems": ranked,
    }

    out_path = Path(args.out)
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, default=str)

    if rows_skipped_by_year:
        print(f"Skipped {rows_skipped_by_year:,} rows outside year filter")
    print(f"Read {total_rows:,} rows, {total_violations:,} violations")
    print(f"Systems: {len(ranked):,} total, "
          f"{systems_with_violations:,} with >=1 violation, "
          f"{systems_with_missing_mcl:,} with missing-MCL violations")
    print(f"Wrote {out_path}")
    if ranked and ranked[0]["violation_count"] > 0:
        top = ranked[0]
        print(f"Worst: {top['system_id']} ({top['system_name']}) - "
              f"severity_sum={top['severity_sum']:.2f}, "
              f"violations={top['violation_count']}")


if __name__ == "__main__":
    main()
