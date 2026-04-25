"""Geocode water systems that lack coordinates.

Three-tier strategy:
  1. Census street-address geocoder (no API key, free)
  2. Nominatim city-level lookup for PO-box-only systems
  3. County centroid + phyllotactic spiral for anything still unresolved

Caches every API call in geocode_cache.json; outputs geocoded_coordinates.json.
"""
import argparse
import json
import math
import os
import re
import statistics
import sys
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from bs4 import BeautifulSoup

LEADERBOARD = Path("leaderboard.json")
PROFILE = Path("water_system_data_full_profile.json")
CACHE_DIR = Path("cache_html")
CACHE_PATH = Path("geocode_cache.json")
OUT_PATH = Path("geocoded_coordinates.json")

CENSUS_URL = "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress"
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
GEOAPIFY_URL = "https://api.geoapify.com/v1/geocode/search"
USER_AGENT = "TexasWaterQualityAnalysis/1.0 (gregrr@gmail.com)"

ROLE_PRIORITY = ["PWS", "OW", "EC", "ECS", "FC", "AC", "LCC"]
PO_BOX_RE = re.compile(r"\bP\.?\s*O\.?\s*BOX\b", re.IGNORECASE)
PO_BOX_STRIP_RE = re.compile(r"^\s*P\.?\s*O\.?\s*BOX\s+\w+\s+", re.IGNORECASE)

CENSUS_SLEEP = 0.25
NOMINATIM_SLEEP = 1.2
GEOAPIFY_SLEEP = 0.2  # Geoapify is generally faster/higher limit than Nominatim
GOLDEN_ANGLE = math.pi * (3 - math.sqrt(5))
SPIRAL_BASE_RADIUS = 0.015  # degrees; ~1.5 km at TX latitudes

GEOAPIFY_KEY = os.getenv("GEOAPIFY_API_KEY")

JUNK_COUNTIES = {"UNKNOWN", "NO SITE VISIT DATA", ""}


# ---------- HTTP helpers --------------------------------------------------

def http_get_json(url, params, timeout=30):
    full = url + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(full, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.load(r)


def census_geocode(address):
    try:
        res = http_get_json(CENSUS_URL, {
            "address": address,
            "benchmark": "Public_AR_Current",
            "format": "json",
        })
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as e:
        return {"status": "error", "error": str(e)}
    matches = res.get("result", {}).get("addressMatches") or []
    if not matches:
        return {"status": "not_found"}
    m = matches[0]
    state = (m.get("addressComponents", {}) or {}).get("state", "").upper()
    return {
        "status": "ok",
        "lat": float(m["coordinates"]["y"]),
        "lon": float(m["coordinates"]["x"]),
        "matched_address": m.get("matchedAddress", ""),
        "state": state,
    }


def nominatim_search(query):
    try:
        res = http_get_json(NOMINATIM_URL, {
            "q": query,
            "format": "json",
            "limit": 1,
            "addressdetails": 1,
            "countrycodes": "us",
        })
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as e:
        return {"status": "error", "error": str(e)}
    if not res:
        return {"status": "not_found"}
    r = res[0]
    addr = r.get("address", {}) or {}
    county = addr.get("county", "").replace(" County", "").strip().upper() or None
    state = (addr.get("state") or "").upper() or None
    return {
        "status": "ok",
        "lat": float(r["lat"]),
        "lon": float(r["lon"]),
        "matched_address": r.get("display_name", ""),
        "county": county,
        "state": state,
        "result_type": r.get("type"),
    }


def geoapify_geocode(address):
    if not GEOAPIFY_KEY:
        return {"status": "error", "error": "GEOAPIFY_API_KEY not set"}
    try:
        res = http_get_json(GEOAPIFY_URL, {
            "text": address,
            "format": "json",
            "apiKey": GEOAPIFY_KEY,
        })
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as e:
        return {"status": "error", "error": str(e)}
    
    results = res.get("results")
    if not results:
        return {"status": "not_found"}
    
    r = results[0]
    # Geoapify returns state code or name. 
    # Let's try to normalize it to "TX" or "TEXAS"
    state = (r.get("state") or "").upper()
    state_code = (r.get("state_code") or "").upper()
    
    county = (r.get("county") or "").replace(" County", "").strip().upper() or None
    
    return {
        "status": "ok",
        "lat": float(r["lat"]),
        "lon": float(r["lon"]),
        "matched_address": r.get("formatted", ""),
        "county": county,
        "state": state,
        "state_code": state_code,
        "result_type": r.get("result_type"),
    }


# ---------- Address parsing ----------------------------------------------

def normalize_text(s):
    return unicodedata.normalize("NFKC", s).strip()


def extract_survey_county(soup):
    """Pull county from the 'Last Survey Date / ... / County' table, skipping
    the header row and 'No Site Visit Data' placeholder rows. Returns upper-
    case county name, or None."""
    header_cell = soup.find(
        lambda tag: tag.name in ("td", "th")
        and tag.get_text(strip=True) == "Last Survey Date"
    )
    if not header_cell:
        return None
    table = header_cell.find_parent("table")
    if not table:
        return None
    for row in table.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) >= 5:
            county = cells[4].get_text(strip=True).upper()
            if county and county != "COUNTY" and "NO SITE" not in county:
                return county
    return None


def parse_addresses(soup):
    """Return dict {normalized_address: sorted list of role codes}."""
    out = {}
    
    # 1. Primary: All Water System Contacts table
    header = soup.find(string=lambda t: t and "All Water System Contacts" in t)
    if header:
        table = header.find_parent("table")
        if table:
            for row in table.find_all("tr"):
                cells = row.find_all("td")
                if len(cells) < 2:
                    continue
                role = cells[0].get_text(strip=True).split("-")[0].strip()
                raw = cells[1].get_text(separator="|", strip=True)
                parts = [p for p in raw.split("|") if p]
                if len(parts) < 2:
                    continue
                addr = normalize_text(" ".join(" ".join(parts[1:]).strip().split()))
                # Must contain digit + comma to look like an address
                if "," not in addr or not any(c.isdigit() for c in addr):
                    continue
                out.setdefault(addr, set()).add(role)

    # 2. Secondary: Sources tables (Active and Inactive/Offline)
    sources_headers = soup.find_all(string=lambda t: t and ("Active Sources" in t or "Inactive/Offline Sources" in t))
    for s_header in sources_headers:
        s_table = s_header.find_parent("table")
        if not s_table:
            continue
        for row in s_table.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 2:
                continue
            name_text = cells[1].get_text(strip=True)
            clean_addr = re.sub(r"^\d+\s*-\s*", "", name_text).strip()
            if len(clean_addr) > 5 and any(c.isdigit() for c in clean_addr) and any(c.isalpha() for c in clean_addr):
                out.setdefault(clean_addr, set()).add("SOURCE")

    return {a: sorted(r) for a, r in out.items()}


def rank_addresses(addresses):
    """Return list of (addr, roles) sorted by best role priority ascending."""
    scored = []
    for addr, roles in addresses.items():
        # SOURCE role is least preferred
        if roles == ["SOURCE"]:
            best = 100
        else:
            best = min((ROLE_PRIORITY.index(r) if r in ROLE_PRIORITY else 99
                        for r in roles), default=99)
        scored.append((best, addr, roles))
    scored.sort()
    return [(a, r) for _, a, r in scored]


def po_box_city(addr):
    """Strip the PO BOX N prefix, leaving CITY, TX ZIP."""
    stripped = PO_BOX_STRIP_RE.sub("", addr).strip()
    return stripped if stripped and "," in stripped else None


# ---------- County bbox + centroid --------------------------------------

def derive_county_geography(profile):
    """From already-mapped systems, derive per-county bbox and centroid."""
    per_county = {}
    for sid, rec in profile.items():
        meta = rec.get("meta") or {}
        county = (meta.get("county") or "").strip().upper()
        if not county or county in ("UNKNOWN", "NO SITE VISIT DATA"):
            continue
        for src in rec.get("sources") or []:
            try:
                lat = float(src.get("latitude"))
                lon = float(src.get("longitude"))
            except (TypeError, ValueError):
                continue
            if not (25 < lat < 37 and -107 < lon < -93):
                continue
            per_county.setdefault(county, []).append((lat, lon))
            break

    geography = {}
    for county, pts in per_county.items():
        if len(pts) < 2:
            lat = pts[0][0]
            lon = pts[0][1]
            geography[county] = {
                "centroid": (lat, lon),
                "bbox": (lat - 0.25, lat + 0.25, lon - 0.25, lon + 0.25),
                "count": 1,
            }
            continue
        lats = [p[0] for p in pts]
        lons = [p[1] for p in pts]
        mlat = statistics.mean(lats)
        mlon = statistics.mean(lons)
        slat = statistics.pstdev(lats) or 0.2
        slon = statistics.pstdev(lons) or 0.2
        geography[county] = {
            "centroid": (mlat, mlon),
            "bbox": (mlat - 1.5 * slat, mlat + 1.5 * slat,
                     mlon - 1.5 * slon, mlon + 1.5 * slon),
            "count": len(pts),
        }
    return geography


def in_bbox(lat, lon, bbox):
    la_min, la_max, lo_min, lo_max = bbox
    return la_min <= lat <= la_max and lo_min <= lon <= lo_max


# ---------- Cache --------------------------------------------------------

def load_cache():
    if CACHE_PATH.exists():
        with CACHE_PATH.open() as f:
            return json.load(f)
    return {}


def save_cache(cache):
    with CACHE_PATH.open("w") as f:
        json.dump(cache, f, indent=2)


def cached(cache, provider, query, fetch_fn, sleep_after):
    key = f"{provider}:{query}"
    if key in cache:
        return cache[key]
    result = fetch_fn(query)
    cache[key] = result
    time.sleep(sleep_after)
    return result


# ---------- Main pipeline -----------------------------------------------

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=None,
                        help="Only process N systems (for dry-runs)")
    parser.add_argument("--only", type=str, default=None,
                        help="Comma-separated system IDs to restrict processing (for debugging)")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Log every system's tier decision")
    args = parser.parse_args()

    if not LEADERBOARD.exists():
        sys.exit(f"ERROR: {LEADERBOARD} not found. Run build_leaderboard.py first.")
    if not PROFILE.exists():
        sys.exit(f"ERROR: {PROFILE} not found.")

    lb = json.load(LEADERBOARD.open())
    profile = json.load(PROFILE.open())
    geography = derive_county_geography(profile)
    print(f"Derived geography for {len(geography)} counties")

    targets = [s for s in lb["systems"]
               if s["violation_count"] > 0 and s["latitude"] is None]
    if args.only:
        only_ids = {sid.strip() for sid in args.only.split(",") if sid.strip()}
        targets = [s for s in targets if s["system_id"] in only_ids]
    if args.limit:
        targets = targets[:args.limit]
    print(f"Targets: {len(targets)} systems")

    cache = load_cache()
    out = {}
    buckets = {"census": 0, "census_out_of_county": 0,
               "nominatim_street": 0, "nominatim_city": 0,
               "geoapify_street": 0,
               "pending_centroid": 0, "no_address": 0, "no_centroid": 0}

    def try_census(address, county_geo):
        result = cached(cache, "census", address, census_geocode, CENSUS_SLEEP)
        if result["status"] != "ok":
            return None
        # Reject non-TX matches outright — contact addresses often sit in
        # another state (registered agents, HQ offices) and using them would
        # place the pin thousands of miles from the actual facility.
        # Missing state field also rejects, which invalidates pre-patch cache
        # entries and forces a re-query.
        if result.get("state") != "TX":
            return None
        lat, lon = result["lat"], result["lon"]
        if county_geo and in_bbox(lat, lon, county_geo["bbox"]):
            return (lat, lon, result["matched_address"], "ok")
        return (lat, lon, result["matched_address"], "out_of_county")

    def _query_variants(query, hint):
        """Build ordered list of query variants to try against a geocoder."""
        variants = []
        if hint:
            variants.append(f"{hint}, {query}")
        variants.append(query)
        # If query doesn't already mention Texas/TX, append ", Texas" as a
        # disambiguator. Helps when address has no city or is otherwise
        # geographically ambiguous.
        if not re.search(r"\b(TX|TEXAS)\b", query, re.IGNORECASE):
            variants.append(f"{query}, Texas")
        # De-duplicate while preserving order
        seen = set()
        return [v for v in variants if not (v in seen or seen.add(v))]

    def try_nominatim(query, system_county, hint=None):
        """Return ((lat, lon, matched_address), reason) or (None, reason).

        Tries each query variant and keeps going past non-Texas matches so
        an unqualified query that first resolves to another state still gets
        a shot at its ", Texas" variant."""
        variants = _query_variants(query, hint)
        last_reason = f"not_found (tried {len(variants)} variants)"
        for v in variants:
            r = cached(cache, "nominatim", v, nominatim_search, NOMINATIM_SLEEP)
            if r["status"] != "ok":
                continue
            if r.get("state") and r["state"] != "TEXAS":
                last_reason = f"not_texas (state={r.get('state')}, matched={r.get('matched_address')})"
                continue
            matched_county = r.get("county")
            if system_county not in JUNK_COUNTIES:
                if matched_county and matched_county != system_county:
                    last_reason = (f"county_mismatch (wanted={system_county}, got={matched_county}, "
                                   f"matched={r.get('matched_address')})")
                    continue
            return (r["lat"], r["lon"], r["matched_address"]), "ok"
        return None, last_reason

    def try_geoapify(address, system_county, hint=None):
        """Return ((lat, lon, matched_address), reason) or (None, reason)."""
        if not GEOAPIFY_KEY:
            return None, "no_api_key"

        variants = _query_variants(address, hint)
        last_reason = f"not_found (tried {len(variants)} variants)"
        for v in variants:
            r = cached(cache, "geoapify", v, geoapify_geocode, GEOAPIFY_SLEEP)
            if r["status"] != "ok":
                continue
            if "TX" not in [r.get("state"), r.get("state_code")] and \
               "TEXAS" not in [r.get("state")]:
                last_reason = f"not_texas (state={r.get('state')}, matched={r.get('matched_address')})"
                continue
            matched_county = r.get("county")
            if system_county not in JUNK_COUNTIES:
                if matched_county and matched_county != system_county:
                    last_reason = (f"county_mismatch (wanted={system_county}, got={matched_county}, "
                                   f"matched={r.get('matched_address')})")
                    continue
            return (r["lat"], r["lon"], r["matched_address"]), "ok"
        return None, last_reason

    def get_county_centroid(county):
        """Look up a TX county centroid via Nominatim when it's missing from
        derived geography. Cached so we don't hammer OSM on repeat runs."""
        if not county or county in JUNK_COUNTIES:
            return None
        query = f"{county} County, Texas, United States"
        result = cached(cache, "nominatim_county", query, nominatim_search, NOMINATIM_SLEEP)
        if result["status"] != "ok":
            return None
        return {
            "centroid": (result["lat"], result["lon"]),
            "bbox": (result["lat"] - 0.5, result["lat"] + 0.5,
                    result["lon"] - 0.5, result["lon"] + 0.5),
            "count": 0,
        }

    pending_centroid = []

    for i, s in enumerate(targets, 1):
        sid = s["system_id"]
        # Use profile metadata for county as primary, fallback to leaderboard
        p_meta = profile.get(sid, {}).get("meta", {})
        county = (p_meta.get("county") or s.get("county") or "").strip().upper()

        if args.verbose:
            print(f"[{i}/{len(targets)}] {sid} ({s.get('system_name')}) County: {county}")

        html_path = CACHE_DIR / f"{sid}_detail.html"
        if not html_path.exists():
            if args.verbose:
                print(f"  !! No cache HTML found at {html_path}")
            buckets["no_address"] += 1
            # Still try to use county from profile even if HTML is missing
            if county in JUNK_COUNTIES:
                continue
        else:
            with html_path.open() as f:
                soup = BeautifulSoup(f.read(), "html.parser")

            # If county is still junk, try to recover from HTML survey table
            if county in JUNK_COUNTIES:
                survey_county = extract_survey_county(soup)
                if survey_county:
                    if args.verbose:
                        print(f"  Recovered county from HTML survey table: {survey_county}")
                    county = survey_county

        # Look up derived geography; if absent, fall back to Nominatim
        county_geo = geography.get(county)
        if county_geo is None and county and county not in JUNK_COUNTIES:
            county_geo = get_county_centroid(county)
            if county_geo and args.verbose:
                c = county_geo["centroid"]
                print(f"  Got {county} centroid via Nominatim: {c[0]:.4f},{c[1]:.4f}")

        addresses = parse_addresses(soup)
        ranked = rank_addresses(addresses)

        if not ranked:
            if args.verbose:
                print(f"  !! No valid addresses parsed from HTML")

        # Split into street + po-box-city buckets preserving priority
        street_candidates = [(a, r) for a, r in ranked if not PO_BOX_RE.search(a)]
        po_box_candidates = [(a, r) for a, r in ranked if PO_BOX_RE.search(a)]

        if args.verbose and ranked:
            print(f"  Parsed {len(street_candidates)} street addrs, {len(po_box_candidates)} PO boxes")

        resolved = None

        # Tier 1: Street addresses (Census -> Geoapify -> Nominatim)
        # Nominatim rate-limits to ~1 req/sec so we try it last.
        best_out_of_county = None
        for addr, roles in street_candidates:
            if args.verbose:
                print(f"  Trying street: {addr}")

            # Census
            res = try_census(addr, county_geo)
            if res is not None:
                lat, lon, matched, conf = res
                if conf == "ok":
                    resolved = {"lat": lat, "lon": lon, "source": "census", "confidence": "ok", "matched_address": matched, "tried_address": addr, "roles": roles}
                    buckets["census"] += 1
                    if args.verbose: print(f"    ✓ Census Match: {matched}")
                    break
                if best_out_of_county is None:
                    best_out_of_county = {"lat": lat, "lon": lon, "source": "census", "confidence": "out_of_county", "matched_address": matched, "tried_address": addr, "roles": roles}
                    if args.verbose: print(f"    ? Census OOC: {matched}")
                continue

            # Hint for non-Census geocoders
            hint = None
            hint_parts = []
            if county and county not in JUNK_COUNTIES: hint_parts.append(county)
            if s.get("system_name"): hint_parts.append(s["system_name"])
            if hint_parts: hint = ", ".join(hint_parts)

            # Geoapify (fast, large quota)
            geo, geo_reason = try_geoapify(addr, county, hint=hint)
            if geo is not None:
                lat, lon, matched = geo
                resolved = {"lat": lat, "lon": lon, "source": "geoapify_street", "confidence": "ok", "matched_address": matched, "tried_address": addr, "roles": roles}
                buckets["geoapify_street"] += 1
                if args.verbose: print(f"    ✓ Geoapify Match: {matched}")
                break
            if args.verbose: print(f"    - Geoapify rejected: {geo_reason}")

            # Nominatim (rate-limited, last resort)
            nom, nom_reason = try_nominatim(addr, county, hint=hint)
            if nom is not None:
                lat, lon, matched = nom
                resolved = {"lat": lat, "lon": lon, "source": "nominatim_street", "confidence": "ok", "matched_address": matched, "tried_address": addr, "roles": roles}
                buckets["nominatim_street"] += 1
                if args.verbose: print(f"    ✓ Nominatim Match: {matched}")
                break
            if args.verbose: print(f"    - Nominatim rejected: {nom_reason}")

            if args.verbose: print(f"    ✗ All street geocoders failed")

        # Tier 2: PO Box cities (if no street match)
        # Geoapify first (faster); Nominatim last (rate-limited).
        if resolved is None:
            for addr, roles in po_box_candidates:
                city_q = po_box_city(addr)
                if not city_q: continue
                if args.verbose: print(f"  Trying PO Box city: {city_q}")

                hint = None
                hint_parts = []
                if county and county not in JUNK_COUNTIES: hint_parts.append(county)
                if s.get("system_name"): hint_parts.append(s["system_name"])
                if hint_parts: hint = ", ".join(hint_parts)

                geo, geo_reason = try_geoapify(city_q, county, hint=hint)
                if geo:
                    lat, lon, matched = geo
                    resolved = {"lat": lat, "lon": lon, "source": "geoapify_city", "confidence": "ok", "matched_address": matched, "tried_address": city_q, "roles": roles}
                    buckets["geoapify_street"] += 1
                    if args.verbose: print(f"    ✓ Geoapify city match: {matched}")
                    break
                if args.verbose: print(f"    - Geoapify city rejected: {geo_reason}")

                res, reason = try_nominatim(city_q, county, hint=hint)
                if res:
                    lat, lon, matched = res
                    resolved = {"lat": lat, "lon": lon, "source": "nominatim_city", "confidence": "ok", "matched_address": matched, "tried_address": city_q, "roles": roles}
                    buckets["nominatim_city"] += 1
                    if args.verbose: print(f"    ✓ Nominatim city match: {matched}")
                    break
                if args.verbose: print(f"    ✗ Nominatim city rejected: {reason}")

        # Fall back to out-of-county Census hit if we have one
        if resolved is None and best_out_of_county is not None:
            resolved = best_out_of_county
            buckets["census_out_of_county"] += 1

        if resolved is not None:
            out[sid] = resolved
            if args.verbose:
                print(f"[{i}/{len(targets)}] {sid}  {resolved['source']}/{resolved['confidence']}  "
                      f"{resolved['lat']:.4f},{resolved['lon']:.4f}  {resolved['matched_address'][:60]}")
        else:
            # Queue for centroid spiral
            if county_geo is None:
                buckets["no_centroid"] += 1
                if args.verbose:
                    print(f"[{i}/{len(targets)}] {sid}  no county geo ({county})")
                continue
            pending_centroid.append((sid, county, county_geo))
            buckets["pending_centroid"] += 1
            if args.verbose:
                print(f"[{i}/{len(targets)}] {sid}  queued for centroid ({county})")

        # Periodic cache flush so long runs don't lose progress
        if i % 25 == 0:
            save_cache(cache)

    save_cache(cache)

    # Tier 3: phyllotactic spiral around derived county centroid
    by_county = {}
    for sid, county, geo in pending_centroid:
        by_county.setdefault(county, []).append((sid, geo))
    for county, entries in by_county.items():
        entries.sort(key=lambda e: e[0])  # stable by system_id
        centroid = entries[0][1]["centroid"]
        clat, clon = centroid
        lon_scale = 1.0 / max(math.cos(math.radians(clat)), 0.1)
        for i, (sid, geo) in enumerate(entries):
            angle = i * GOLDEN_ANGLE
            radius = SPIRAL_BASE_RADIUS * math.sqrt(i + 1)
            d_lat = radius * math.sin(angle)
            d_lon = radius * math.cos(angle) * lon_scale
            out[sid] = {
                "lat": clat + d_lat,
                "lon": clon + d_lon,
                "source": "centroid_spiral",
                "confidence": "centroid",
                "matched_address": None,
                "tried_address": None,
                "roles": [],
                "county_used": county,
                "spiral_index": i,
            }

    # When --only is used, merge with existing output instead of overwriting
    if args.only and OUT_PATH.exists():
        with OUT_PATH.open() as f:
            existing = json.load(f)
        existing.update(out)
        out = existing

    with OUT_PATH.open("w") as f:
        json.dump(out, f, indent=2)

    total = len(targets)
    centroid_placed = sum(1 for v in out.values() if v["source"] == "centroid_spiral")
    print(f"\nWrote {OUT_PATH} with {len(out)} entries")
    print(f"  Census (in county):    {buckets['census']}")
    print(f"  Census (out of county):{buckets['census_out_of_county']}")
    print(f"  Nominatim street:      {buckets['nominatim_street']}")
    print(f"  Geoapify street:       {buckets['geoapify_street']}")
    print(f"  Nominatim city:        {buckets['nominatim_city']}")
    print(f"  Centroid spiral:       {centroid_placed}")
    print(f"  No address found:      {buckets['no_address']}")
    print(f"  No county geo:         {buckets['no_centroid']}")
    resolved = len(out)
    print(f"\nResolved: {resolved}/{total} ({100*resolved/max(total,1):.1f}%)")


if __name__ == "__main__":
    main()
