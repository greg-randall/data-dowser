#!/usr/bin/env python3
"""Quick Mapbox geocoding API response probe."""

import json
import os
import urllib.parse
import urllib.request

TOKEN = os.environ.get("MAPBOX_TOKEN", "")
if not TOKEN:
    raise SystemExit("MAPBOX_TOKEN not set")

# Texas bounding box: roughly SW corner to NE corner
TEXAS_BBOX = "-106.645646,25.837377,-93.508292,36.500704"

ADDRESSES = [
    ("2 Lincoln Memorial Cir NW, Washington DC", None),
    ("1600 Pennsylvania Ave NW, Washington DC", None),
    # Texas water-system style addresses — test with and without bbox
    ("123 Main St, Seguin, TX 78155", None),
    ("123 Main St, Seguin, TX 78155", TEXAS_BBOX),   # same query, bbox constrained
    ("456 Oak Ave, Hondo, TX 78861", TEXAS_BBOX),
    ("1000 FT N OF STORE, Pecos, TX", TEXAS_BBOX),   # garbage address
]

# Dump raw props for the first result of this query so we can see all fields
RAW_DUMP_QUERY = "1600 Pennsylvania Ave NW, Washington DC"


def geocode(address, bbox=None):
    params = {"q": address, "access_token": TOKEN}
    if bbox:
        params["bbox"] = bbox
    qs = urllib.parse.urlencode(params)
    url = f"https://api.mapbox.com/search/geocode/v6/forward?{qs}"
    with urllib.request.urlopen(url, timeout=10) as r:
        return json.loads(r.read())


for addr, bbox in ADDRESSES:
    label = f"{addr}" + (" [+bbox]" if bbox else "")
    print(f"\n{'='*60}")
    print(f"QUERY: {label}")
    try:
        data = geocode(addr, bbox)
        features = data.get("features", [])
        print(f"Results: {len(features)}")

        if addr == RAW_DUMP_QUERY and not bbox:
            print("  RAW props[0]:", json.dumps(features[0].get("properties", {}), indent=4) if features else "(none)")

        for i, f in enumerate(features[:3]):
            props = f.get("properties", {})
            coords = f.get("geometry", {}).get("coordinates", [])
            context = props.get("context", {})
            mc = props.get("match_code", {})
            print(f"  [{i}] name:        {props.get('name')}")
            print(f"       full_addr:   {props.get('full_address')}")
            print(f"       place_type:  {props.get('feature_type')}")
            print(f"       mapbox_id:   {props.get('mapbox_id', '')[:40]}")
            print(f"       match_code:  {mc}")
            print(f"       coords:      {coords}")
            print(f"       region:      {context.get('region', {}).get('name')}")
            print(f"       country:     {context.get('country', {}).get('name')}")
        if not features:
            print("  (no results)")
    except Exception as e:
        print(f"  ERROR: {e}")
