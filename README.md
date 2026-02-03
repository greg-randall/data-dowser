# Texas Water Quality Dashboard

This set of scripts downloads, processes, and maps public water quality data from the Texas Commission on Environmental Quality (TCEQ).

![Dashboard Preview](readme/dashboard-sample.png)

## Overview

Texas publishes water quality reports (CCRs) as individual Microsoft Word documents behind a search form. This format makes it difficult to analyze trends or identify contamination hotspots. This project scrapes the TCEQ portal, converts the `.doc` files to JSON, adds geolocation data, and displays the results on an interactive map.

## Workflow

### 1. Acquisition (`scraper.py`)
This script downloads the water system list from the state portal, then downloads all available Consumer Confidence Reports (CCRs). It outputs a directory structure of `.doc` files (e.g., `downloads/TX12345_CityName/TX12345_2023.doc`).

### 2. Processing (`process.py`)
Since the data is in binary Word files, this script converts them to HTML and parses the tables to extract to JSON files containing contaminant levels, violations, and limits.

### 3. Enrichment (`get-ids.py`)
This script scrapes the physical address, latitude and longitude, and population served for every system from the state portal. It outputs `water_system_data_full_profile.json`.

### 4. Categorization (`contaminant_categories.yaml`)
This configuration file maps specific chemical names (e.g., "Trihalomethanes", "TTHM") to broader categories (e.g., "Disinfection Byproducts"). `list_contaminants.py` can be used to audit these mappings against the scraped data.

### 5. Visualization (`build_dashboard_data.py` & `dashboard.html`)
This script merges the chemical data, location metadata, and category configuration. It splits the output into two optimized files (`dashboard_data_map.json` and `dashboard_data_details.json`) to allow the dashboard to load the map instantly while fetching details in the background.

## Technical Implementation

### Scraper
`scraper.py` parses the JSP dropdowns on `dww2.tceq.texas.gov` to build a catalog of Water System Numbers (`wsno`). It uses `subprocess` to call `curl` directly, which handles the server's connection resets better than Python's `requests` library. A `ThreadPoolExecutor` downloads reports in parallel, prioritizing recent years. State is saved to `progress.json` to allow resuming.

### Processor
The state uses the Word 97-2003 format (`.doc`), which `python-docx` cannot read. `process.py` embeds a PowerShell script that uses the COM interface to open the file in a local installation of Microsoft Word and save it as HTML. It then uses `BeautifulSoup` to identify tables containing contaminant data, normalizing units (ppb vs ppm) and detecting violations.

### Metadata
`get-ids.py` visits the specific detail page for each ID. It uses `unicodedata.normalize("NFKC", text)` to fix encoding issues common in the source HTML. It extracts coordinates from the "Source Water Inventory" tables rather than the mailing address.

### Dashboard
The dashboard runs client-side. `build_dashboard_data.py` compiles the data into two JSON artifacts using short-keys (minification) to reduce file size. The frontend uses Leaflet with Canvas markers to render the points. D3.js handles the sparkline charts in the popup panels. Filtering logic runs in the browser using the categories defined in the YAML configuration.

## Setup & Usage

**Requirements:**
*   Python 3.10+
*   Windows (for `process.py` Word conversion) or a VM with MS Word.
*   `pip install requests beautifulsoup4 tqdm pyyaml`

**Running the Pipeline:**

```bash
# 1. Download the data (Long running process)
python scraper.py

# 2. Convert .doc to .json (Requires MS Word)
python process.py --input downloads/

# 3. Fetch location metadata
python get-ids.py

# 4. Compile the dashboard data (Uses contaminant_categories.yaml)
python build_dashboard_data.py

# 5. View the dashboard
# Open dashboard.html in any modern browser
```
