"""Geocode water systems that lack coordinates.

Three-tier strategy:
  1. Census street-address geocoder (no API key, free)
  2. Mapbox v6 geocoder (structured input where parseable; MAPBOX_TOKEN env var)
  3. County centroid + phyllotactic spiral for anything still unresolved

Caches every API call in geocode_cache.json; outputs geocoded_coordinates.json.
"""
import argparse
import json
import math
import os
import random
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
from tqdm import tqdm

LEADERBOARD = Path("leaderboard.json")
PROFILE = Path("water_system_data_full_profile.json")
CACHE_DIR = Path("cache_html")
CACHE_PATH = Path("geocode_cache.json")
OUT_PATH = Path("geocoded_coordinates.json")

CENSUS_URL = "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress"
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
MAPBOX_URL = "https://api.mapbox.com/search/geocode/v6/forward"
USER_AGENT = "TexasWaterQualityAnalysis/1.0 (gregrr@gmail.com)"

ROLE_PRIORITY = ["PWS", "OW", "EC", "ECS", "FC", "AC", "LCC"]
PO_BOX_RE = re.compile(r"\bP\.?\s*O\.?\s*BOX\b", re.IGNORECASE)
PO_BOX_STRIP_RE = re.compile(r"^\s*P\.?\s*O\.?\s*BOX\s+\w+\s+", re.IGNORECASE)

CENSUS_SLEEP = 0.25
NOMINATIM_SLEEP = 1.2
MAPBOX_SLEEP = 0.05  # 1000 req/min rate limit — minimal sleep needed
GMAPS_SLEEP = 0.05   # Google allows 50 req/s
GOLDEN_ANGLE = math.pi * (3 - math.sqrt(5))
SPIRAL_BASE_RADIUS = 0.015  # degrees; ~1.5 km at TX latitudes

MAPBOX_TOKEN = os.getenv("MAPBOX_TOKEN")
GMAPS_API_KEY = os.getenv("GMAPS_API_KEY")
GMAPS_URL = "https://maps.googleapis.com/maps/api/geocode/json"

# Matches "123 Main St, City, TX [zip]" for structured Mapbox geocoding input
ADDR_COMPONENTS_RE = re.compile(
    r'^(\d[\w\s.\-#/]*?),\s*([^,]+?),\s*(?:TX|TEXAS)\s*(\d{5})?\s*$',
    re.IGNORECASE
)

JUNK_COUNTIES = {"UNKNOWN", "NO SITE VISIT DATA", ""}


# ---------- HTTP helpers --------------------------------------------------

def http_get_json(url, params, timeout=30):
    full = url + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(full, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        body = r.read()
    if not body.strip():
        raise json.JSONDecodeError("empty response", "", 0)
    return json.loads(body)


def census_geocode(address):
    try:
        res = http_get_json(CENSUS_URL, {
            "address": address,
            "benchmark": "Public_AR_Current",
            "format": "json",
        })
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, json.JSONDecodeError) as e:
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
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, json.JSONDecodeError) as e:
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


def mapbox_geocode(address):
    if not MAPBOX_TOKEN:
        return {"status": "error", "error": "MAPBOX_TOKEN not set"}

    # Use structured input if address parses as "street, city, TX [zip]"
    m = ADDR_COMPONENTS_RE.match(address.strip())
    if m:
        params = {
            "address_line1": m.group(1).strip(),
            "place": m.group(2).strip(),
            "region": "TX",
            "country": "us",
            "autocomplete": "false",
            "types": "address,street",
            "limit": 3,
            "access_token": MAPBOX_TOKEN,
        }
        if m.group(3):
            params["postcode"] = m.group(3)
    else:
        params = {
            "q": address,
            "country": "us",
            "autocomplete": "false",
            "types": "address,street",
            "limit": 3,
            "access_token": MAPBOX_TOKEN,
        }

    try:
        res = http_get_json(MAPBOX_URL, params)
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, json.JSONDecodeError) as e:
        return {"status": "error", "error": str(e)}

    for f in (res.get("features") or []):
        props = f.get("properties", {})
        mc = props.get("match_code", {})
        confidence = mc.get("confidence", "")

        # Reject low-confidence results outright; medium only if region matched
        if confidence == "low":
            continue
        if confidence == "medium" and mc.get("region") != "matched":
            continue

        coords = props.get("coordinates", {})
        lat = coords.get("latitude")
        lon = coords.get("longitude")
        if lat is None or lon is None:
            continue

        context = props.get("context", {})
        region_code = context.get("region", {}).get("region_code", "")
        if region_code and region_code != "TX":
            continue

        county_str = context.get("district", {}).get("name", "")
        county = county_str.replace(" County", "").strip().upper() or None

        return {
            "status": "ok",
            "lat": float(lat),
            "lon": float(lon),
            "matched_address": props.get("full_address", ""),
            "county": county,
            "state": "TEXAS",
            "state_code": "TX",
            "result_type": props.get("feature_type"),
            "mapbox_confidence": confidence,
        }

    return {"status": "not_found"}


def mapbox_name_geocode(name, expected_county=None):
    """Geocode by system name; accepts place-level results.

    Queries just the system name (no county appended — county words in the
    query match street names like 'Travis County Way'). Uses region=TX as a
    parameter constraint instead. Enforces county match when expected_county
    is provided.
    """
    if not MAPBOX_TOKEN:
        return {"status": "error", "error": "MAPBOX_TOKEN not set"}
    params = {
        "q": name,
        "country": "us",
        "region": "TX",
        "autocomplete": "false",
        "types": "address,street,place",
        "limit": 5,
        "access_token": MAPBOX_TOKEN,
    }
    try:
        res = http_get_json(MAPBOX_URL, params)
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, json.JSONDecodeError) as e:
        return {"status": "error", "error": str(e)}

    for f in (res.get("features") or []):
        props = f.get("properties", {})
        mc = props.get("match_code", {})
        confidence = mc.get("confidence", "")
        if confidence == "low":
            continue
        if confidence == "medium" and mc.get("region") != "matched":
            continue
        coords = props.get("coordinates", {})
        lat = coords.get("latitude")
        lon = coords.get("longitude")
        if lat is None or lon is None:
            continue
        context = props.get("context", {})
        region_code = context.get("region", {}).get("region_code", "")
        if region_code and region_code != "TX":
            continue
        county_str = context.get("district", {}).get("name", "")
        result_county = county_str.replace(" County", "").strip().upper() or None
        if expected_county and expected_county not in JUNK_COUNTIES:
            if result_county and result_county != expected_county:
                continue
        return {
            "status": "ok",
            "lat": float(lat),
            "lon": float(lon),
            "matched_address": props.get("full_address", ""),
            "county": result_county,
            "state": "TEXAS",
            "state_code": "TX",
            "result_type": props.get("feature_type"),
            "mapbox_confidence": confidence,
        }

    return {"status": "not_found"}


def google_name_geocode(name, expected_county=None):
    """Geocode by system name using Google Maps Geocoding API.

    Appends 'County, TX' to the query — test data showed this helps Google
    find roads and businesses in the right county vs. fuzzy phonetic matches.
    Validates that the result county matches expected_county when provided.
    """
    if not GMAPS_API_KEY:
        return {"status": "error", "error": "GMAPS_API_KEY not set"}
    query = name
    if expected_county and expected_county not in JUNK_COUNTIES:
        query = f"{name}, {expected_county.title()} County, TX"
    params = {
        "address": query,
        "components": "country:US|administrative_area:TX",
        "key": GMAPS_API_KEY,
    }
    try:
        res = http_get_json(GMAPS_URL, params)
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, json.JSONDecodeError) as e:
        return {"status": "error", "error": str(e)}

    if res.get("status") not in ("OK", "ZERO_RESULTS"):
        return {"status": "error", "error": res.get("status")}

    for r in (res.get("results") or []):
        comps = r.get("address_components", [])

        def _comp(kind):
            for c in comps:
                if kind in c.get("types", []):
                    return c.get("long_name", "")
            return ""

        state = _comp("administrative_area_level_1")
        if state and state.upper() not in ("TEXAS", "TX"):
            continue
        county_raw = _comp("administrative_area_level_2")
        result_county = county_raw.replace(" County", "").strip().upper() or None
        if expected_county and expected_county not in JUNK_COUNTIES:
            if result_county and result_county != expected_county:
                continue
        loc = r.get("geometry", {}).get("location", {})
        lat, lon = loc.get("lat"), loc.get("lng")
        if lat is None or lon is None:
            continue
        # Reject results that are just the state or county centroid (too coarse)
        result_types = r.get("types", [])
        if "administrative_area_level_1" in result_types:
            continue
        if "administrative_area_level_2" in result_types:
            continue
        return {
            "status": "ok",
            "lat": float(lat),
            "lon": float(lon),
            "matched_address": r.get("formatted_address", ""),
            "county": result_county,
            "state": "TEXAS",
            "state_code": "TX",
            "result_type": result_types[0] if result_types else None,
        }

    return {"status": "not_found"}


# ---------- Coordinate extraction ----------------------------------------

def extract_coordinates(system_data):
    """Return (lat, lon) from TCEQ source-water inventory, or (None, None)."""
    for source in (system_data.get("sources") or []):
        lat = source.get("latitude")
        lon = source.get("longitude")
        if lat and lon:
            try:
                lat_f = float(lat)
                lon_f = float(lon)
                if 25 < lat_f < 37 and -107 < lon_f < -93:
                    return lat_f, lon_f
            except (ValueError, TypeError):
                pass
    return None, None


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
        json.dump(cache, f, separators=(',', ':'))


def cached(cache, provider, query, fetch_fn, sleep_after, dirty_flag, timings=None):
    key = f"{provider}:{query}"
    if key in cache:
        if timings is not None:
            timings[provider + "_hits"] = timings.get(provider + "_hits", 0) + 1
        return cache[key]
    t0 = time.perf_counter()
    result = fetch_fn(query)
    elapsed = time.perf_counter() - t0
    cache[key] = result
    dirty_flag[0] = True
    time.sleep(sleep_after)
    if timings is not None:
        k = provider + "_live"
        timings[k] = timings.get(k, 0) + 1
        timings[provider + "_time"] = timings.get(provider + "_time", 0.0) + elapsed + sleep_after
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
    parser.add_argument("--enable-nominatim", action="store_true",
                        help="Allow Nominatim fallback (1.2s/call rate limit; off by default, use Geoapify instead)")
    parser.add_argument("--reprocess-centroids", action="store_true",
                        help="Clear existing centroid_spiral entries and re-geocode them through all tiers")
    args = parser.parse_args()

    if not PROFILE.exists():
        sys.exit(f"ERROR: {PROFILE} not found.")

    profile = json.load(PROFILE.open())
    geography = derive_county_geography(profile)
    print(f"Derived geography for {len(geography)} counties")

    # Load already-geocoded output so we can skip completed systems
    existing_out = {}
    if OUT_PATH.exists():
        with OUT_PATH.open() as f:
            existing_out = json.load(f)

    if args.reprocess_centroids:
        reprocess_sources = {"centroid_spiral", "mapbox_name", "google_name"}
        reprocess_sids = [sid for sid, v in existing_out.items()
                          if v.get("source") in reprocess_sources]
        for sid in reprocess_sids:
            del existing_out[sid]
        print(f"Cleared {len(reprocess_sids)} centroid_spiral/mapbox_name entries for reprocessing")

    # Load TCEQ county lookup (from fetch_tceq_counties.py) as a county source
    # for systems whose profile county is "NO SITE VISIT DATA"
    tceq_counties = {}
    tceq_counties_path = Path("tceq_counties.json")
    if tceq_counties_path.exists():
        with tceq_counties_path.open() as f:
            tceq_counties = json.load(f)
        print(f"Loaded {len(tceq_counties)} TCEQ county records")

    # Target every system that has no TCEQ source-water coordinates and
    # hasn't already been geocoded into geocoded_coordinates.json.
    targets = []
    for sid, rec in profile.items():
        if sid in existing_out:
            continue
        lat, lon = extract_coordinates(rec)
        if lat is not None:
            continue
        meta = rec.get("meta", {}) or {}
        folder = rec.get("folder_name", "")
        name = folder.replace(f"{sid}_", "").replace("_", " ").strip()
        targets.append({
            "system_id": sid,
            "system_name": name,
            "county": (meta.get("county") or "").strip().upper(),
        })

    if args.only:
        only_ids = {sid.strip() for sid in args.only.split(",") if sid.strip()}
        targets = [s for s in targets if s["system_id"] in only_ids]
    if args.limit:
        targets = random.sample(targets, min(args.limit, len(targets)))
    print(f"Targets: {len(targets)} systems")

    cache = load_cache()
    out = existing_out
    out_start_size = len(out)

    # Track spiral index per county so resumed runs continue the spiral correctly
    centroid_indices = {}
    for entry in existing_out.values():
        if entry.get("source") == "centroid_spiral":
            c = entry.get("county_used", "")
            if c:
                centroid_indices[c] = max(centroid_indices.get(c, 0), entry.get("spiral_index", 0) + 1)

    def save_out():
        with OUT_PATH.open("w") as f:
            json.dump(out, f, indent=2)
    buckets = {"census": 0, "census_out_of_county": 0,
               "nominatim_street": 0, "nominatim_city": 0,
               "mapbox_street": 0, "mapbox_name": 0, "google_name": 0,
               "centroid": 0, "no_address": 0, "no_centroid": 0}

    dirty = [False]  # mutable container so closures can set it
    timings = {}

    def try_census(address, county_geo):
        result = cached(cache, "census", address, census_geocode, CENSUS_SLEEP, dirty, timings)
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
            r = cached(cache, "nominatim", v, nominatim_search, NOMINATIM_SLEEP, dirty, timings)
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

    def try_mapbox(address, system_county, hint=None):
        """Return ((lat, lon, matched_address), reason) or (None, reason)."""
        if not MAPBOX_TOKEN:
            return None, "no_api_key"

        variants = _query_variants(address, hint)
        last_reason = f"not_found (tried {len(variants)} variants)"
        for v in variants:
            r = cached(cache, "mapbox", v, mapbox_geocode, MAPBOX_SLEEP, dirty, timings)
            if r["status"] != "ok":
                continue
            if "TX" not in [r.get("state_code"), r.get("state")] and \
               "TEXAS" not in [r.get("state", "")]:
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
        result = cached(cache, "nominatim_county", query, nominatim_search, NOMINATIM_SLEEP, dirty, timings)
        if result["status"] != "ok":
            return None
        return {
            "centroid": (result["lat"], result["lon"]),
            "bbox": (result["lat"] - 0.5, result["lat"] + 0.5,
                    result["lon"] - 0.5, result["lon"] + 0.5),
            "count": 0,
        }

    t_html = t_geo = t_save = 0.0

    for i, s in enumerate(tqdm(targets, desc="Geocoding"), 1):
        sid = s["system_id"]
        soup = None
        # Use profile metadata for county as primary, fallback to leaderboard
        p_meta = profile.get(sid, {}).get("meta", {})
        county = (p_meta.get("county") or s.get("county") or "").strip().upper()

        if args.verbose:
            print(f"[{i}/{len(targets)}] {sid} ({s.get('system_name')}) County: {county}")

        _t = time.perf_counter()
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
        t_html += time.perf_counter() - _t

        # Last-resort county recovery: TCEQ SearchResults API data
        if county in JUNK_COUNTIES and sid in tceq_counties:
            county = tceq_counties[sid].strip().upper()
            if args.verbose:
                print(f"  Recovered county from TCEQ API: {county}")

        # Look up derived geography; if absent, fall back to Nominatim
        _t = time.perf_counter()
        county_geo = geography.get(county)
        if county_geo is None and county and county not in JUNK_COUNTIES:
            county_geo = get_county_centroid(county)
            if county_geo and args.verbose:
                c = county_geo["centroid"]
                print(f"  Got {county} centroid via Nominatim: {c[0]:.4f},{c[1]:.4f}")

        addresses = parse_addresses(soup) if soup is not None else {}
        ranked = rank_addresses(addresses)

        if not ranked:
            if args.verbose:
                print(f"  !! No valid addresses parsed from HTML")

        # Split into street + po-box-city buckets preserving priority
        street_candidates = [(a, r) for a, r in ranked if not PO_BOX_RE.search(a)]
        po_box_candidates = [(a, r) for a, r in ranked if PO_BOX_RE.search(a)]

        if args.verbose and ranked:
            print(f"  Parsed {len(street_candidates)} street addrs, {len(po_box_candidates)} PO boxes")

        t_geo += time.perf_counter() - _t
        _t = time.perf_counter()
        resolved = None

        # Tier 1: Street addresses (Census -> Mapbox -> Nominatim)
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

            # Mapbox (fast, 1000 req/min)
            geo, geo_reason = try_mapbox(addr, county, hint=hint)
            if geo is not None:
                lat, lon, matched = geo
                resolved = {"lat": lat, "lon": lon, "source": "mapbox_street", "confidence": "ok", "matched_address": matched, "tried_address": addr, "roles": roles}
                buckets["mapbox_street"] += 1
                if args.verbose: print(f"    ✓ Mapbox Match: {matched}")
                break
            if args.verbose: print(f"    - Mapbox rejected: {geo_reason}")

            # Nominatim (rate-limited, last resort; disabled unless --enable-nominatim)
            if not args.enable_nominatim:
                if args.verbose: print(f"    - Nominatim skipped (use --enable-nominatim)")
                continue
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
        # Mapbox first (faster); Nominatim last (rate-limited).
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

                geo, geo_reason = try_mapbox(city_q, county, hint=hint)
                if geo:
                    lat, lon, matched = geo
                    resolved = {"lat": lat, "lon": lon, "source": "mapbox_city", "confidence": "ok", "matched_address": matched, "tried_address": city_q, "roles": roles}
                    buckets["mapbox_street"] += 1
                    if args.verbose: print(f"    ✓ Mapbox city match: {matched}")
                    break
                if args.verbose: print(f"    - Mapbox city rejected: {geo_reason}")

                if not args.enable_nominatim:
                    if args.verbose: print(f"    - Nominatim skipped (use --enable-nominatim)")
                    continue
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

        # Tier 2.5: Name-based Mapbox query — place-level, last resort before centroid.
        # Query is just the system name (county appended to query matches street names).
        # County enforcement happens inside the fetch function via closure.
        if resolved is None and s.get("system_name") and county not in JUNK_COUNTIES:
            system_name = s["system_name"]
            _expected_county = county

            def _fetch_name(q):
                return mapbox_name_geocode(q, expected_county=_expected_county)

            r = cached(cache, "mapbox_name", system_name, _fetch_name, MAPBOX_SLEEP, dirty, timings)
            if r["status"] == "ok":
                resolved = {
                    "lat": r["lat"], "lon": r["lon"],
                    "source": "mapbox_name", "confidence": "name_match",
                    "matched_address": r["matched_address"],
                    "tried_address": system_name, "roles": [],
                }
                buckets["mapbox_name"] += 1
                if args.verbose:
                    print(f"    ✓ Mapbox name match: {r['matched_address']}")
            elif args.verbose:
                print(f"    - Mapbox name rejected: {r.get('status')}")

        # Tier 2.6: Google Maps name geocoding — appends county to query for better targeting.
        if resolved is None and s.get("system_name") and county not in JUNK_COUNTIES:
            system_name = s["system_name"]
            _expected_county = county

            def _fetch_google(q):
                return google_name_geocode(q, expected_county=_expected_county)

            r = cached(cache, "google_name", system_name, _fetch_google, GMAPS_SLEEP, dirty, timings)
            if r["status"] == "ok":
                resolved = {
                    "lat": r["lat"], "lon": r["lon"],
                    "source": "google_name", "confidence": "name_match",
                    "matched_address": r["matched_address"],
                    "tried_address": system_name, "roles": [],
                }
                buckets["google_name"] += 1
                if args.verbose:
                    print(f"    ✓ Google name match: {r['matched_address']}")
            elif args.verbose:
                print(f"    - Google name rejected: {r.get('status')}")

        if resolved is not None:
            out[sid] = resolved
            if args.verbose:
                print(f"[{i}/{len(targets)}] {sid}  {resolved['source']}/{resolved['confidence']}  "
                      f"{resolved['lat']:.4f},{resolved['lon']:.4f}  {resolved['matched_address'][:60]}")
        else:
            # Tier 3: place on county centroid spiral immediately
            if county_geo is None:
                buckets["no_centroid"] += 1
                if args.verbose:
                    print(f"[{i}/{len(targets)}] {sid}  no county geo ({county})")
                continue
            idx = centroid_indices.get(county, 0)
            centroid_indices[county] = idx + 1
            clat, clon = county_geo["centroid"]
            lon_scale = 1.0 / max(math.cos(math.radians(clat)), 0.1)
            angle = idx * GOLDEN_ANGLE
            radius = SPIRAL_BASE_RADIUS * math.sqrt(idx + 1)
            out[sid] = {
                "lat": clat + radius * math.sin(angle),
                "lon": clon + radius * math.cos(angle) * lon_scale,
                "source": "centroid_spiral",
                "confidence": "centroid",
                "matched_address": None,
                "tried_address": None,
                "roles": [],
                "county_used": county,
                "spiral_index": idx,
            }
            buckets["centroid"] += 1
            if args.verbose:
                print(f"[{i}/{len(targets)}] {sid}  centroid spiral idx={idx} ({county})")

        t_geo += time.perf_counter() - _t

        # Batch saves: flush every 25 systems to avoid hammering the WSL filesystem
        if i % 25 == 0:
            _t = time.perf_counter()
            if dirty[0]:
                save_cache(cache)
                dirty[0] = False
            save_out()
            t_save += time.perf_counter() - _t

            def _tc(p):
                live = timings.get(p + "_live", 0)
                hits = timings.get(p + "_hits", 0)
                t = timings.get(p + "_time", 0.0)
                return f"{p}={t:.2f}s({live}live/{hits}hit)"
            tqdm.write(
                f"  [timing/25] html={t_html:.2f}s  save={t_save:.2f}s  "
                + "  ".join(_tc(p) for p in (
                    "census", "nominatim", "nominatim_county", "mapbox", "mapbox_name", "google_name"
                ))
            )
            t_html = t_geo = t_save = 0.0
            timings.clear()

    if dirty[0]:
        save_cache(cache)
    save_out()

    total = len(targets)
    print(f"\nWrote {OUT_PATH} with {len(out)} entries")
    print(f"  Census (in county):    {buckets['census']}")
    print(f"  Census (out of county):{buckets['census_out_of_county']}")
    print(f"  Nominatim street:      {buckets['nominatim_street']}")
    print(f"  Mapbox street:         {buckets['mapbox_street']}")
    print(f"  Mapbox name:           {buckets['mapbox_name']}")
    print(f"  Google name:           {buckets['google_name']}")
    print(f"  Nominatim city:        {buckets['nominatim_city']}")
    print(f"  Centroid spiral:       {buckets['centroid']}")
    print(f"  No address found:      {buckets['no_address']}")
    print(f"  No county geo:         {buckets['no_centroid']}")
    newly_resolved = len(out) - out_start_size
    print(f"\nResolved this run: {newly_resolved}/{total} ({100*newly_resolved/max(total,1):.1f}%)")
    print(f"Total in output:   {len(out)}")


if __name__ == "__main__":
    main()
