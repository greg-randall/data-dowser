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

CSV_PATH = Path("raw-data/texas_water_quality.csv")
OUT_PATH = Path("leaderboard.json")

# Composite score weights (must sum to 1.0). Adjust here to retune.
SEVERITY_WEIGHT = 0.7
IMPACT_WEIGHT = 0.3

CITY_RE = re.compile(r"^\s*(?:CITY|TOWN|VILLAGE)\s+OF\s+(.+?)\s*$", re.IGNORECASE)


def extract_city(system_name):
    if not system_name:
        return ""
    m = CITY_RE.match(system_name)
    if not m:
        return ""
    return m.group(1).strip().title()


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
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        print(f"ERROR: CSV not found at {csv_path}", file=sys.stderr)
        sys.exit(1)

    systems = {}
    total_rows = 0
    total_violations = 0

    print(f"Reading {csv_path}...")
    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            total_rows += 1
            sid = row["system_id"]
            if sid not in systems:
                systems[sid] = {
                    "system_id": sid,
                    "system_name": row["system_name"],
                    "city": extract_city(row["system_name"]),
                    "county": row["county"],
                    "latitude": to_float(row["latitude"]),
                    "longitude": to_float(row["longitude"]),
                    "population": to_float(row["population"]),
                    "violation_count": 0,
                    "severity_sum": 0.0,
                    "violations_missing_mcl": 0,
                    "worst_violation": None,
                    "worst_severity": 0.0,
                }
            s = systems[sid]

            pop = to_float(row["population"])
            if pop is not None and (s["population"] is None or pop > s["population"]):
                s["population"] = pop

            if row["violation"] != "True":
                continue

            total_violations += 1
            s["violation_count"] += 1

            level = to_float(row["highest_level"])
            mcl = to_float(row["mcl"])
            if level is None or mcl is None or mcl <= 0:
                s["violations_missing_mcl"] += 1
                continue

            severity = level / mcl
            s["severity_sum"] += severity
            if severity > s["worst_severity"]:
                s["worst_severity"] = severity
                s["worst_violation"] = {
                    "contaminant": row["contaminant"],
                    "year": int(row["year"]) if row["year"] else None,
                    "level": level,
                    "mcl": mcl,
                    "units": row["units"],
                    "severity": severity,
                }

    for s in systems.values():
        scored = s["violation_count"] - s["violations_missing_mcl"]
        s["avg_severity"] = (s["severity_sum"] / scored) if scored > 0 else None
        s["impact_score"] = (
            s["population"] * s["severity_sum"]
            if s["population"] and s["severity_sum"] > 0
            else None
        )

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
            -s["severity_sum"],
            -s["violation_count"],
            -(s["avg_severity"] or 0),
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
            "total_rows": total_rows,
            "total_violations": total_violations,
            "systems_total": len(ranked),
            "systems_with_violations": systems_with_violations,
            "systems_with_missing_mcl_violations": systems_with_missing_mcl,
            "sort": "severity_sum DESC, violation_count DESC, avg_severity DESC",
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
