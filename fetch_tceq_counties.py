#!/usr/bin/env python3
"""Fetch principal county for every TX water system from the TCEQ SearchResults OData API.

Outputs tceq_counties.json: {system_id: "COUNTY NAME"} for all systems where
D_PRIN_CNTY_SVD_NM is non-empty.  Used by geocode_missing.py to resolve the
~2,575 systems whose profile county is "NO SITE VISIT DATA".

Saves incrementally after each page so a crash doesn't lose progress.
Re-running resumes from where it left off.
"""
import json
import time
from pathlib import Path

from camoufox.sync_api import Camoufox
from curl_cffi import requests as cffi_requests
from tqdm import tqdm

PAGE_URL = "https://dwv.tceq.texas.gov/"
SEARCH_URL = "https://dwv.tceq.texas.gov/sdwis/SearchResults"
OUT_PATH = Path("tceq_counties.json")
PAGE_SIZE = 1000
JUNK = {"", "UNKNOWN", "NO SITE VISIT DATA"}


def get_session():
    print("Launching browser to get session cookies...")
    with Camoufox(headless=True) as fox:
        page = fox.new_page()
        page.goto(PAGE_URL, wait_until="load", timeout=60000)
        page.wait_for_timeout(3000)
        cookies = fox.contexts[0].cookies()
        page.close()

    xsrf = next((c["value"] for c in cookies if c["name"] == "XSRF-TOKEN"), None)
    if not xsrf:
        raise RuntimeError("Could not find XSRF-TOKEN cookie")

    session = cffi_requests.Session(impersonate="edge101")
    for c in cookies:
        session.cookies.set(c["name"], c["value"], domain=c.get("domain", "dwv.tceq.texas.gov"))
    session.headers.update({
        "accept": "application/json",
        "x-xsrf-token": xsrf,
    })
    print(f"Session ready ({len(cookies)} cookies)")
    return session


def save(counties):
    with OUT_PATH.open("w") as f:
        json.dump(counties, f, indent=2, sort_keys=True)


def main():
    # Load any existing output so we can resume
    counties = {}
    if OUT_PATH.exists():
        with OUT_PATH.open() as f:
            counties = json.load(f)
        print(f"Resuming: {len(counties)} systems already saved")

    session = get_session()

    # Get total count
    r = session.get(SEARCH_URL, params={
        "$orderby": "NUMBER0",
        "$skip": 0,
        "$top": 1,
        "$count": "true",
    })
    r.raise_for_status()
    total = r.json().get("@odata.count", 0)
    print(f"Total systems reported: {total}")

    # Resume from the page corresponding to how many we already have.
    # Since we're ordered by NUMBER0 and save incrementally, len(counties)
    # is a safe lower bound for where to resume (may re-fetch the last page).
    skip = (len(counties) // PAGE_SIZE) * PAGE_SIZE
    fetched = 0

    with tqdm(total=total, initial=skip, unit="sys") as pbar:
        while True:
            r = session.get(SEARCH_URL, params={
                "$orderby": "NUMBER0",
                "$skip": skip,
                "$top": PAGE_SIZE,
            })
            r.raise_for_status()
            batch = r.json().get("value", [])
            if not batch:
                break

            for rec in batch:
                sid = rec.get("NUMBER0", "").strip()
                county = (rec.get("D_PRIN_CNTY_SVD_NM") or "").strip().upper()
                if sid and county and county not in JUNK:
                    counties[sid] = county

            fetched += len(batch)
            pbar.update(len(batch))
            save(counties)

            skip += len(batch)
            if len(batch) < PAGE_SIZE:
                break
            time.sleep(0.5)

    print(f"\nFetched {fetched} records this run")
    print(f"Total systems with county: {len(counties)}")

    # Spot-check
    for check_id in ["TX1010762", "TX0980005"]:
        print(f"  {check_id} → {counties.get(check_id, '(not found)')}")

    print(f"Wrote {OUT_PATH}")


if __name__ == "__main__":
    main()
