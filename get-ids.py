import os
import json
import time
import re
import unicodedata
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

# --- CONFIGURATION ---
TARGET_FOLDER = '/mnt/e/Google Drive/futureheist/city/water-quality/downloads'
OUTPUT_FILE = 'water_system_data_full_profile.json'
CACHE_DIR = 'cache_html'

# Setup the browser session
session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Origin': 'https://dww2.tceq.texas.gov',
    'Referer': 'https://dww2.tceq.texas.gov/DWW/'
})


def clean_html(raw_html):
    if not raw_html:
        return ""
    
    # 1. Nuke HTML specific spacing issues manually first
    # Replace <br> with a space so "Pop<br>Served" becomes "Pop Served"
    text = re.sub(r'<br\s*/?>', ' ', raw_html, flags=re.IGNORECASE)
    
    # 2. The Nuclear Option (NFKC Normalization)
    # This turns &nbsp; (\xa0) into space, m² into m2, ½ into 1/2
    text = unicodedata.normalize("NFKC", text)
    
    # 3. Collapse multiple spaces (The cleanup crew)
    # This turns "Population    Served" into "Population Served"
    text = re.sub(r'\s+', ' ', text)
    
    return text.strip()


def load_data():
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE, 'r') as f:
                return json.load(f)
        except json.JSONDecodeError:
            print("Warning: JSON file corrupted. Starting fresh.")
            return {}
    return {}

def save_data(data):
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(data, f, indent=4)

def get_detail_page_content(pws_id):
    if not os.path.exists(CACHE_DIR):
        os.makedirs(CACHE_DIR)
    cache_filename = os.path.join(CACHE_DIR, f"{pws_id}_detail.html")

    if os.path.exists(cache_filename):
        with open(cache_filename, 'r', encoding='utf-8') as f:
            return f.read()

    search_url = "https://dww2.tceq.texas.gov/DWW/JSP/SearchDispatch"
    search_params = {
        "number": pws_id,
        "ActivityStatusCD": "All",
        "county": "All",
        "WaterSystemType": "All",
        "SourceWaterType": "All",
        "action": "Search For Water Systems"
    }

    try:
        search_resp = session.get(search_url, params=search_params, timeout=20)
        search_soup = BeautifulSoup(search_resp.text, 'html.parser')
        link = search_soup.find('a', href=lambda href: href and 'DataSheet.jsp' in href)
        if not link:
            return None 

        detail_url = "https://dww2.tceq.texas.gov/DWW/JSP/" + link['href']
        detail_resp = session.get(detail_url, timeout=20)
        with open(cache_filename, 'w', encoding='utf-8') as f:
            f.write(detail_resp.text)
        time.sleep(1)
        return detail_resp.text
    except Exception as e:
        print(f" [Network Error: {e}]", end='')
        return None

def scrape_pws_data(pws_id):
    result_data = {
        "meta": {},       # NEW: Population, County, etc.
        "addresses": [],
        "sources": []
    }

    detail_html = get_detail_page_content(pws_id)
    detail_html = clean_html(detail_html)
    if not detail_html:
        result_data["error"] = "System Not Found"
        return result_data

    soup = BeautifulSoup(detail_html, 'html.parser')
    text_content = soup.get_text(separator=' ', strip=True)

    # --- PART A: METADATA (POPULATION, COUNTY, TYPE) ---
    meta = {
        "population": "0",
        "connections": "0",
        "county": "Unknown",
        "system_type": "Unknown"
    }

    # 1. Population & Connections
    # Looking for table row with "Residential" or "Total" usually
    pop_header = soup.find(string=lambda t: t and "Population" in t and "Served" in t)
    if pop_header:
        pop_table = pop_header.find_parent('table')
        if pop_table:
            # Usually the data row is after the header. We look for a number in the 2nd column.
            for row in pop_table.find_all('tr'):
                cells = row.find_all('td')
                if len(cells) >= 3:
                    # Check if 2nd cell is a digit (Pop Served)
                    pop_val = cells[1].get_text(strip=True)
                    if pop_val.isdigit():
                        meta["population"] = pop_val
                        meta["connections"] = cells[2].get_text(strip=True)
                        break

    # 2. County (Usually in "Last Survey Date" table or near top)
    # We can use a regex fallback for "County Map of TX" followed by the county name in the table
    # Or look for the 'Region' 'County' table headers
    county_header = soup.find(string=lambda t: t and "County" in t)
    if county_header:
        # This is tricky because "County" appears in many places. 
        # The specific table has headers: Last Survey Date | Surveyor | ... | Region | County
        # Let's look for that specific structure
        survey_table = soup.find('td', string="Last Survey Date")
        if survey_table:
            # Navigate to the row
            table = survey_table.find_parent('table')
            if table:
                rows = table.find_all('tr')
                if len(rows) > 1:
                    # Get first data row, last column usually
                    cols = rows[1].find_all('td')
                    if cols:
                        meta["county"] = cols[-1].get_text(strip=True)

    # 3. System Type
    sys_type_header = soup.find(string=lambda t: t and "System Type Options" in t)
    if sys_type_header:
        sys_table = sys_type_header.find_parent('table')
        if sys_table:
            rows = sys_table.find_all('tr')
            if len(rows) > 1:
                meta["system_type"] = rows[1].get_text(strip=True)

    result_data["meta"] = meta

    # --- PART B: ADDRESSES ---
    address_groups = {}
    header_text = soup.find(string=lambda t: t and "All Water System Contacts" in t)
    if header_text:
        contact_table = header_text.find_parent('table')
        if contact_table:
            for row in contact_table.find_all('tr'):
                cells = row.find_all('td')
                if len(cells) >= 2:
                    type_text = cells[0].get_text(strip=True)
                    role_code = type_text.split('-')[0].strip()
                    raw_contact_info = cells[1].get_text(separator='|', strip=True)
                    parts = raw_contact_info.split('|')
                    if len(parts) > 1:
                        clean_address = " ".join(" ".join(parts[1:]).strip().split())
                        if clean_address not in address_groups:
                            address_groups[clean_address] = {"address": clean_address, "seen_count": 0, "roles": []}
                        address_groups[clean_address]["seen_count"] += 1
                        if role_code not in address_groups[clean_address]["roles"]:
                            address_groups[clean_address]["roles"].append(role_code)
    result_data["addresses"] = list(address_groups.values())

    # --- PART C: SOURCES (With Elevation) ---
    source_header = soup.find(string=lambda t: t and "(Active Sources)" in t)
    if source_header:
        source_table = source_header.find_parent('table')
        if source_table:
            rows = source_table.find_all('tr')
            current_source = None
            next_row_is_summary = False
            next_row_is_gps = False
            
            for row in rows:
                cells = row.find_all('td')
                text = row.get_text(strip=True)
                
                # Main Info Row
                if cells and re.match(r'^[SG]\d+[A-Z]?$', cells[0].get_text(strip=True)):
                    if current_source: result_data["sources"].append(current_source)
                    raw_type = cells[3].get_text(strip=True)
                    type_map = {"G": "Groundwater", "S": "Surface Water", "GU": "Groundwater u/ Influence"}
                    current_source = {
                        "id": cells[0].get_text(strip=True),
                        "name": cells[1].get_text(strip=True),
                        "type_code": raw_type,
                        "type_desc": type_map.get(raw_type, "Unknown"),
                        "status": cells[2].get_text(strip=True),
                        "drill_date": None, "aquifer_or_river": None, "latitude": None, "longitude": None, "elevation": None
                    }
                    continue

                if "Source Summary" in text and "Drill Date" in text:
                    next_row_is_summary = True
                    continue
                if next_row_is_summary and current_source and len(cells) >= 2:
                    current_source["drill_date"] = cells[0].get_text(strip=True)
                    current_source["aquifer_or_river"] = cells[1].get_text(strip=True)
                    next_row_is_summary = False
                    continue

                if "GPS Latitude" in text:
                    next_row_is_gps = True
                    continue
                if next_row_is_gps and current_source and len(cells) >= 3:
                    lat = cells[0].get_text(strip=True)
                    lon = cells[1].get_text(strip=True)
                    elev = cells[2].get_text(strip=True)
                    if lat and lon and lat != "0.0" and lon != "0.0":
                        current_source["latitude"] = lat
                        current_source["longitude"] = lon
                        current_source["elevation"] = elev
                    next_row_is_gps = False
                    continue

            if current_source: result_data["sources"].append(current_source)
                                
    return result_data

# --- MAIN LOOP ---
if __name__ == "__main__":
    data_store = load_data()
    print(f"Loaded {len(data_store)} existing records.")
    
    if not os.path.exists(CACHE_DIR): os.makedirs(CACHE_DIR)

    try: items = sorted(os.listdir(TARGET_FOLDER))
    except FileNotFoundError: items = []

    skipped = 0
    processed = 0
    tx_items = [item for item in items if item.startswith("TX")]
    already_done = sum(1 for item in tx_items if item.split('_')[0] in data_store and "meta" in data_store[item.split('_')[0]])

    print(f"Found {len(tx_items)} items")
    print(f"Already processed: {already_done}")
    print(f"Remaining: {len(tx_items) - already_done}\n")

    for item in tqdm(tx_items, desc="Processing", unit="system"):
        pws_id = item.split('_')[0]
        # Skip if we already have the new 'meta' field
        if pws_id in data_store and "meta" in data_store[pws_id]:
            skipped += 1
            continue

        processed += 1
        scraped_data = scrape_pws_data(pws_id)

        data_store[pws_id] = {
            "folder_name": item,
            "meta": scraped_data.get("meta", {}),
            "addresses": scraped_data.get("addresses", []),
            "sources": scraped_data.get("sources", []),
            "error": scraped_data.get("error", None)
        }
        save_data(data_store)

    print(f"\nAll done! Processed: {processed}, Skipped: {skipped}")