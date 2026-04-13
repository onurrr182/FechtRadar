"""
FechtRadar Scraper v2.1 — Precise Address Edition
Uses requests + BeautifulSoup (no browser needed).
Extracts city from event pages AND tries invitation pages for precise street addresses.
"""

import requests
from bs4 import BeautifulSoup
import json
import re
import time
import urllib.parse
import urllib.request

CALENDAR_URL = "https://fencing.ophardt.online/en/calendar?date-from=2025-01-01&date-to=2028-12-31&nation=GER"
BASE_URL = "https://fencing.ophardt.online"

# Months to include (set to None to include all)
INCLUDE_MONTHS = None  # All months

# Country filter
COUNTRY_CODE = "GER"  # Set to None for all countries

# Session for reusing connections
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,de;q=0.8"
})

# ─── NOISE WORDS to strip from city names ─────────────────────────────────
CITY_STOP_WORDS = [
    "Invitation", "Entries", "Results", "Competitions", "Other dates",
    "View", "Live", "Pre-entries", "Official", "website", "Homepage",
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    "Equipment", "Referee", "Participants", "Created", "Modified",
    "Provided", "Ophardt", "fencingworldwide", "Published"
]

# ─── IOC COUNTRY MAPPING ──────────────────────────────────────────────────
IOC_COUNTRY_MAP = {
    "AFG": "Afghanistan", "ALB": "Albania", "ALG": "Algeria", "AND": "Andorra", 
    "ANG": "Angola", "ANT": "Antigua and Barbuda", "ARG": "Argentina", "ARM": "Armenia", 
    "ARU": "Aruba", "ASA": "American Samoa", "AUS": "Australia", "AUT": "Austria", 
    "AZE": "Azerbaijan", "BAH": "Bahamas", "BAN": "Bangladesh", "BAR": "Barbados", 
    "BDI": "Burundi", "BEL": "Belgium", "BEN": "Benin", "BER": "Bermuda", 
    "BHU": "Bhutan", "BIH": "Bosnia and Herzegovina", "BIZ": "Belize", "BLR": "Belarus", 
    "BOL": "Bolivia", "BOT": "Botswana", "BRA": "Brazil", "BRN": "Bahrain", 
    "BRU": "Brunei", "BUL": "Bulgaria", "BUR": "Burkina Faso", "CAF": "Central African Republic", 
    "CAM": "Cambodia", "CAN": "Canada", "CAY": "Cayman Islands", "CGO": "Congo", 
    "CHA": "Chad", "CHI": "Chile", "CHN": "China", "CIV": "Ivory Coast", 
    "CMR": "Cameroon", "COD": "D.R. Congo", "COK": "Cook Islands", "COL": "Colombia", 
    "COM": "Comoros", "CPV": "Cape Verde", "CRC": "Costa Rica", "CRO": "Croatia", 
    "CUB": "Cuba", "CYP": "Cyprus", "CZE": "Czech Republic", "DEN": "Denmark", 
    "DJI": "Djibouti", "DMA": "Dominica", "DOM": "Dominican Republic", "ECU": "Ecuador", 
    "EGY": "Egypt", "ERI": "Eritrea", "ESA": "El Salvador", "ESP": "Spain", 
    "EST": "Estonia", "ETH": "Ethiopia", "FIJ": "Fiji", "FIN": "Finland", 
    "FRA": "France", "FSM": "Micronesia", "GAB": "Gabon", "GAM": "Gambia", 
    "GBR": "United Kingdom", "GBS": "Guinea-Bissau", "GEO": "Georgia", "GEQ": "Equatorial Guinea", 
    "GER": "Germany", "GHA": "Ghana", "GRE": "Greece", "GRN": "Grenada", 
    "GUA": "Guatemala", "GUI": "Guinea", "GUM": "Guam", "GUY": "Guyana", 
    "HAI": "Haiti", "HKG": "Hong Kong", "HON": "Honduras", "HUN": "Hungary", 
    "INA": "Indonesia", "IND": "India", "IRI": "Iran", "IRL": "Ireland", 
    "IRQ": "Iraq", "ISL": "Iceland", "ISR": "Israel", "ISV": "Virgin Islands", 
    "ITA": "Italy", "IVB": "British Virgin Islands", "JAM": "Jamaica", "JOR": "Jordan", 
    "JPN": "Japan", "KAZ": "Kazakhstan", "KEN": "Kenya", "KGZ": "Kyrgyzstan", 
    "KIR": "Kiribati", "KOR": "South Korea", "KSA": "Saudi Arabia", "KUW": "Kuwait", 
    "LAO": "Laos", "LAT": "Latvia", "LBA": "Libya", "LBN": "Lebanon", 
    "LBR": "Liberia", "LCA": "Saint Lucia", "LES": "Lesotho", "LIE": "Liechtenstein", 
    "LTU": "Lithuania", "LUX": "Luxembourg", "MAD": "Madagascar", "MAR": "Morocco", 
    "MAS": "Malaysia", "MAW": "Malawi", "MDA": "Moldova", "MDV": "Maldives", 
    "MEX": "Mexico", "MGL": "Mongolia", "MHL": "Marshall Islands", "MKD": "North Macedonia", 
    "MLI": "Mali", "MLT": "Malta", "MNE": "Montenegro", "MON": "Monaco", 
    "MOZ": "Mozambique", "MRI": "Mauritius", "MTN": "Mauritania", "MYA": "Myanmar", 
    "NAM": "Namibia", "NCA": "Nicaragua", "NED": "Netherlands", "NEP": "Nepal", 
    "NGR": "Nigeria", "NIG": "Niger", "NOR": "Norway", "NRU": "Nauru", 
    "NZL": "New Zealand", "OMA": "Oman", "PAK": "Pakistan", "PAN": "Panama", 
    "PAR": "Paraguay", "PER": "Peru", "PHI": "Philippines", "PLE": "Palestine", 
    "PLW": "Palau", "PNG": "Papua New Guinea", "POL": "Poland", "POR": "Portugal", 
    "PRK": "North Korea", "PUR": "Puerto Rico", "QAT": "Qatar", "ROU": "Romania", 
    "RSA": "South Africa", "RUS": "Russia", "RWA": "Rwanda", "SAM": "Samoa", 
    "SEN": "Senegal", "SEY": "Seychelles", "SGP": "Singapore", "SKN": "Saint Kitts and Nevis", 
    "SLE": "Sierra Leone", "SLO": "Slovenia", "SMR": "San Marino", "SOL": "Solomon Islands", 
    "SOM": "Somalia", "SRB": "Serbia", "SRI": "Sri Lanka", "SSD": "South Sudan", 
    "STP": "Sao Tome and Principe", "SUD": "Sudan", "SUI": "Switzerland", "SUR": "Suriname", 
    "SVK": "Slovakia", "SWE": "Sweden", "SWZ": "Swaziland", "SYR": "Syria", 
    "TAN": "Tanzania", "TGA": "Tonga", "THA": "Thailand", "TJK": "Tajikistan", 
    "TKM": "Turkmenistan", "TLS": "Timor-Leste", "TOG": "Togo", "TPE": "Taiwan", 
    "TTO": "Trinidad and Tobago", "TUN": "Tunisia", "TUR": "Turkey", "TUV": "Tuvalu", 
    "UAE": "United Arab Emirates", "UGA": "Uganda", "UKR": "Ukraine", "URU": "Uruguay", 
    "USA": "USA", "UZB": "Uzbekistan", "VAN": "Vanuatu", "VEN": "Venezuela", 
    "VIE": "Vietnam", "VIN": "Saint Vincent", "YEM": "Yemen", "ZAM": "Zambia", "ZIM": "Zimbabwe"
}

# ─── GEOCODING CACHE ─────────────────────────────────────────────────────────
_geocode_cache = {}

def geocode_city(city_name, country="Germany"):
    """Use Nominatim (OpenStreetMap) to geocode a city name to lat/lng.
    
    If country is a 3-letter IOC code, it is mapped to a full name first.
    """
    if not city_name or city_name.strip() == "":
        return None, None
    
    city_name = city_name.strip()
    
    # Map IOC codes (like GER, USA, FRA) to full names
    if len(country) == 3 and country.isupper():
        country = IOC_COUNTRY_MAP.get(country, country)
    
    cache_key = f"{city_name}, {country}"
    
    if cache_key in _geocode_cache:
        return _geocode_cache[cache_key]
    
    try:
        # Check if the query contains a German postal code (5 digits)
        zip_match = re.match(
            r'^(?:(.+?),\s*)?(\d{5})\s+(.+?)(?:,\s*Germany)?$',
            city_name
        )
        
        if zip_match:
            # Use structured query for precise postal-code-based lookup
            street = zip_match.group(1)
            postalcode = zip_match.group(2)
            city_part = zip_match.group(3).strip().rstrip(',')
            
            params = {
                "postalcode": postalcode,
                "city": city_part,
                "country": country,
                "format": "json",
                "limit": "1",
            }
            if street:
                params["street"] = street
            
            qs = urllib.parse.urlencode(params)
            url = f"https://nominatim.openstreetmap.org/search?{qs}"
        else:
            # Fallback: free-text search
            # We explicitly add the country to the query string to help disambiguation
            clean_query = f"{city_name}, {country}"
            query = urllib.parse.quote(clean_query)
            url = f"https://nominatim.openstreetmap.org/search?q={query}&format=json&limit=1"
            
            # Use countrycodes filter only if we are absolutely sure it's Germany (to avoid Munster Ireland bug)
            if country.lower() in ["germany", "deutschland"]:
                url += "&countrycodes=de"
        
        req = urllib.request.Request(url, headers={
            "User-Agent": "FechtRadar/2.1 (fencing-tournament-map)"
        })
        
        with urllib.request.urlopen(req, timeout=10) as response:
            data = json.loads(response.read().decode())
        
        if data and len(data) > 0:
            lat = float(data[0]["lat"])
            lng = float(data[0]["lon"])
            _geocode_cache[cache_key] = (lat, lng)
            time.sleep(1.1)  # Nominatim rate limit
            return lat, lng
        
        # If structured query returned nothing, retry with free-text as fallback
        if zip_match:
            query = urllib.parse.quote(f"{city_name}, {country}")
            url = f"https://nominatim.openstreetmap.org/search?q={query}&format=json&limit=1&countrycodes=de"
            req = urllib.request.Request(url, headers={
                "User-Agent": "FechtRadar/2.1 (fencing-tournament-map)"
            })
            with urllib.request.urlopen(req, timeout=10) as response:
                data = json.loads(response.read().decode())
            if data and len(data) > 0:
                lat = float(data[0]["lat"])
                lng = float(data[0]["lon"])
                _geocode_cache[cache_key] = (lat, lng)
                time.sleep(1.1)
                return lat, lng
        
    except Exception as e:
        print(f"  ⚠️  Geocoding failed for '{city_name}': {e}")
    
    _geocode_cache[cache_key] = (None, None)
    time.sleep(1.1)
    return None, None


def detect_weapon(text):
    """Detect fencing weapon from tournament text."""
    text_lower = text.lower()
    
    weapons = []
    if any(w in text_lower for w in ["degen", "epee", "épée"]):
        weapons.append("Epee")
    if any(w in text_lower for w in ["florett", "foil"]):
        weapons.append("Foil")
    if any(w in text_lower for w in ["säbel", "sabel", "sabre", "saber"]):
        weapons.append("Sabre")
    
    if len(weapons) > 0:
        return weapons
    return ["Mixed"]


def detect_age_group(text):
    """Detect age group(s) from tournament text using Ophardt's classification.
    Returns a list of age categories."""
    text_lower = text.lower()
    groups = []
    
    # Specific U-categories (Ophardt standard)
    import re as _re
    u_matches = _re.findall(r'\bU\s?(\d+)\b', text, _re.IGNORECASE)
    for m in u_matches:
        age = int(m)
        if age in [9, 11, 13, 15, 17, 20]:
            tag = f"U{age}"
            if tag not in groups:
                groups.append(tag)
    
    # Named categories
    if any(w in text_lower for w in ["veteran", "ak40", "ak50", "ak60", "ak70"]):
        if "Veterans" not in groups:
            groups.append("Veterans")
    if any(w in text_lower for w in ["senior", "masters", "aktive"]):
        if "Seniors" not in groups:
            groups.append("Seniors")
    if any(w in text_lower for w in ["kids", "kinder", "youngster"]):
        # Kids usually means U9/U11 level
        if not any(g.startswith("U") for g in groups):
            if "U11" not in groups:
                groups.append("U11")
    if any(w in text_lower for w in ["jugend", "junior", "youth", "cadets"]):
        # Junior/Youth is typically U15-U20 range
        if not any(g.startswith("U") for g in groups):
            if "U17" not in groups:
                groups.append("U17")
    
    return groups if groups else ["Seniors"]


def clean_city_name(raw_city):
    """Clean noise words from a raw city extraction."""
    if not raw_city:
        return None
    
    city = raw_city.strip()
    
    # Split on any noise word boundary
    for stop in CITY_STOP_WORDS:
        # Remove the stop word and anything after it
        idx = city.find(stop)
        if idx > 0:
            city = city[:idx].strip()
    
    # Also split on double spaces, digits that look like dates (e.g., "2026")
    city = re.split(r'\s{2,}|\s+\d{4}\b', city)[0].strip()
    
    # Remove trailing punctuation, numbers, whitespace
    city = re.sub(r'[\d\s,\.\-]+$', '', city).strip()
    
    # Sanity check
    if len(city) < 2:
        return None
    
    return city


def fetch_page(url):
    """Fetch a page with retries."""
    for attempt in range(3):
        try:
            resp = SESSION.get(url, timeout=15)
            resp.raise_for_status()
            return BeautifulSoup(resp.text, 'html.parser')
        except Exception as e:
            if attempt == 2:
                print(f"  ⚠️  Failed to fetch {url}: {e}")
                return None
            time.sleep(2)
    return None

def fetch_page_playwright(url):
    """Fetch a page using Playwright, auto-scrolling to the bottom to trigger lazy loads, and return a BeautifulSoup object."""
    from playwright.sync_api import sync_playwright
    import time
    print(f"  --> Launching Playwright to crawl {url} (this might take a couple minutes)")
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=90000)
            
            last_height = page.evaluate("document.body.scrollHeight")
            while True:
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(2.5)
                new_height = page.evaluate("document.body.scrollHeight")
                if new_height == last_height:
                    time.sleep(2)
                    new_height = page.evaluate("document.body.scrollHeight")
                    if new_height == last_height:
                        break
                last_height = new_height
            
            time.sleep(1)
            html = page.content()
            browser.close()
            return BeautifulSoup(html, 'html.parser')
    except Exception as e:
        print(f"Playwright error fetching {url}: {e}")
        return None


def get_precise_address(event_id):
    """Try to get venue + street address from the invitation page.
    
    The invitation HTML page contains a "City" section with:
    - Venue name (e.g., "Erhard-Wunderlich-Halle")
    - Street address (e.g., "Ulrich-Hofmaier-Str. 30")
    - ZIP + City (e.g., "86159 Augsburg")
    """
    url = f"{BASE_URL}/en/invitation/view/{event_id}/html"
    soup = fetch_page(url)
    if not soup:
        return None
    
    page_text = soup.get_text("\n", strip=True)
    
    # Try to find German ZIP+City pattern (5 digits + city name)
    # This is the most reliable indicator of a physical address
    zip_match = re.search(
        r'(\d{5})\s+([A-ZÄÖÜa-zßäöü][A-ZÄÖÜa-zßäöü\-\.]+(?:\s+[A-ZÄÖÜa-zßäöü\-\.]+)*)',
        page_text
    )
    
    if zip_match:
        zip_code = zip_match.group(1)
        city = zip_match.group(2).strip()
        # Clean city — remove noise words
        city = clean_city_name(city)
        if city:
            # Try to get the street address (line before the ZIP code)
            zip_pos = page_text.find(zip_match.group(0))
            if zip_pos > 0:
                before_zip = page_text[:zip_pos].strip()
                lines = before_zip.split('\n')
                
                # Skip words that are NOT venue/street names
                skip_words = ['cookie', 'accept', 'home', 'calendar', 'ranking',
                              'nation', 'city', 'date', 'referee', 'entries',
                              'learn more', 'terms', 'imprint', 'biographies',
                              'manual', 'athlete', 'provided', 'ophardt']
                
                # Look for street pattern in the few lines before ZIP
                street = None
                venue = None
                for line in reversed(lines[-8:]):
                    line = line.strip()
                    if not line or len(line) < 3:
                        continue
                    if any(skip in line.lower() for skip in skip_words):
                        continue
                    # Street pattern: word + number (house number)
                    if re.search(r'[A-Za-zäöüÄÖÜß\-\.]+\s*\.?\s*\d+', line) and not street:
                        street = line.rstrip(',')
                    elif not venue and len(line) > 3:
                        venue = line.rstrip(',')
                
                full_addr = f"{zip_code} {city}"
                if street:
                    full_addr = f"{street}, {full_addr}"
                if venue and venue != street:
                    full_addr = f"{venue}, {full_addr}"
                
                return {
                    "venue": venue,
                    "street": street,
                    "zip": zip_code,
                    "city": city,
                    "full": full_addr
                }
            
            return {"venue": None, "street": None, "zip": zip_code, "city": city, "full": f"{zip_code} {city}"}
    
    return None


def scrape_ophardt():
    print("🚀 FechtRadar Scraper v2.1 — Precise Address Edition")
    print(f"📅 Month filter: {INCLUDE_MONTHS if INCLUDE_MONTHS else 'All'}")
    print(f"🌍 Country filter: {COUNTRY_CODE if COUNTRY_CODE else 'All'}")
    
    # ── STEP 1: Load the calendar page ────────────────────────────────
    print("\n📡 Loading Ophardt calendar...")
    soup = fetch_page(CALENDAR_URL)
    
    if not soup:
        print("❌ Failed to load calendar page")
        return
    
    # Extract event links
    all_links = soup.find_all('a', href=True)
    event_entries = []
    seen_ids = set()
    
    for a in all_links:
        href = a['href']
        if '/widget/event/' not in href:
            continue
        
        id_match = re.search(r'/event/(\d+)', href)
        if not id_match:
            continue
        
        event_id = id_match.group(1)
        if event_id in seen_ids:
            continue
        
        name = a.get_text(strip=True)
        if not name or len(name) < 3 or "show more" in name.lower():
            continue
        
        name = " ".join(name.split())
        seen_ids.add(event_id)
        event_entries.append({"id": event_id, "name": name})
    
    print(f"   Found {len(event_entries)} unique event links on calendar")
    
    # ── STEP 2: Process each event ────────────────────────────────────
    print(f"\n🔍 Processing {len(event_entries)} events...\n")
    
    final_json = []
    failed_geocodes = []
    skipped_country = 0
    skipped_month = 0
    precise_count = 0
    
    for idx, entry in enumerate(event_entries, 1):
        short_name = entry['name'][:50]
        print(f"[{idx}/{len(event_entries)}] {short_name}...")
        
        # Visit the event detail page
        event_url = f"{BASE_URL}/en/widget/event/{entry['id']}"
        soup = fetch_page(event_url)
        
        if not soup:
            print(f"  ⚠️  Could not load page, skipping")
            continue
        
        page_text = soup.get_text(" ", strip=True)
        
        # Country check
        if COUNTRY_CODE and COUNTRY_CODE not in page_text:
            skipped_country += 1
            continue
        
        # Extract date
        date_match = re.search(
            r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{1,2}),\s*(\d{4})',
            page_text
        )
        
        month = None
        date_str = ""
        year = ""
        if date_match:
            month_map = {"Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
                         "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12}
            month = month_map.get(date_match.group(1))
            date_str = f"{date_match.group(1)} {date_match.group(2)}"
            year = date_match.group(3)
        
        if not date_match:
            dm = re.search(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', page_text)
            if dm:
                month = int(dm.group(2))
                month_names = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
                               "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
                if 1 <= month <= 12:
                    date_str = f"{month_names[month]} {dm.group(1)}"
                    year = dm.group(3)
        
        # Month filter
        if INCLUDE_MONTHS and (month is None or month not in INCLUDE_MONTHS):
            skipped_month += 1
            continue
        
        # ── Extract city from the header area ─────────────────────────
        # The city is in the gray header: after date, before "Invitation & Entries"
        # Pattern: "GER XX CityName"
        # We extract ONLY from the header area (first occurrence, before section headers)
        
        # First, try to isolate header text (before "Invitation" section)
        header_cutoff = page_text.find("Invitation")
        if header_cutoff == -1:
            header_cutoff = page_text.find("Results")
        if header_cutoff == -1:
            header_cutoff = min(500, len(page_text))
        
        header_text = page_text[:header_cutoff]
        
        city = None
        city_match = re.search(
            r'GER\s+(?:[A-Za-zÄÖÜäöüßé]{1,4}\s+)?([A-ZÄÖÜa-zßäöüé][\wßäöüÄÖÜé\-\s/\.]+)',
            header_text
        )
        if city_match:
            city = clean_city_name(city_match.group(1))
        
        if not city or len(city) < 2:
            print(f"  ⚠️  No city found, skipping")
            continue
        
        print(f"  📍 City: {city}")
        
        # ── Try invitation page for precise address ──────────────────
        precise_addr = get_precise_address(entry['id'])
        
        venue_name = None
        street_addr = None
        display_address = f"{city}, Germany"
        geocode_query = city
        
        if precise_addr:
            precise_count += 1
            venue_name = precise_addr.get("venue")
            street_addr = precise_addr.get("street")
            
            # Clean venue and street of any remaining noise
            noise_patterns = ['\nNation', '\nGermany', '\nDate', '\nCity',
                             'Nation\n', 'Germany\n', 'Date\n']
            if venue_name:
                for noise in noise_patterns:
                    venue_name = venue_name.replace(noise, '')
                venue_name = venue_name.strip(' ,\n')
            if street_addr:
                for noise in noise_patterns:
                    street_addr = street_addr.replace(noise, '')
                street_addr = street_addr.strip(' ,\n')
            
            if precise_addr.get("zip") and precise_addr.get("city"):
                p_city = precise_addr['city']
                p_zip = precise_addr['zip']
                display_address = f"{p_zip} {p_city}, Germany"
                geocode_query = f"{p_zip} {p_city}"
                if street_addr:
                    display_address = f"{street_addr}, {display_address}"
                    geocode_query = f"{street_addr}, {p_zip} {p_city}, Germany"
            
            if venue_name:
                print(f"  🏟️  Venue: {venue_name}")
            if street_addr:
                print(f"  📮 Address: {street_addr}, {precise_addr.get('zip', '')} {precise_addr.get('city', '')}")
        
        # Detect weapon and age group
        weapon = detect_weapon(entry['name'] + " " + header_text)
        age_group = detect_age_group(entry['name'] + " " + header_text)
        
        # ── Geocode ──────────────────────────────────────────────────
        lat, lng = geocode_city(geocode_query)
        
        if lat is None and geocode_query != city:
            lat, lng = geocode_city(city)
        
        if lat is None:
            # Fallback: try without special chars
            clean = re.sub(r'[/\(\)\.\-]', ' ', city).strip()
            clean = re.sub(r'\s+', ' ', clean)
            if clean != city:
                lat, lng = geocode_city(clean)
        
        if lat is None:
            print(f"  ⚠️  Geocoding failed for '{city}', using Germany center")
            lat, lng = 51.1657, 10.4515
            failed_geocodes.append(city)
        else:
            print(f"  ✅ Geocoded: {lat:.4f}, {lng:.4f}")
        
        # Final cleanup of display_address
        display_address = display_address.replace('\n', ', ').replace(',  ', ', ')
        display_address = re.sub(r',\s*,', ',', display_address)
        display_address = re.sub(r'\s+', ' ', display_address).strip(', ')
        
        final_json.append({
            "name": entry['name'],
            "city": city,
            "venue": venue_name,
            "lat": round(lat, 5),
            "lng": round(lng, 5),
            "date": date_str,
            "year": year,
            "weapon": weapon,
            "ageGroup": age_group,
            "exactAddress": display_address,
            "pdfLink": event_url
        })
    
    # ── STEP 3: Save results ──────────────────────────────────────────
    with open('tournaments.json', 'w', encoding='utf-8') as f:
        json.dump(final_json, f, indent=4, ensure_ascii=False)
    
    print(f"\n{'='*60}")
    print(f"✨ Done! Saved {len(final_json)} tournaments to tournaments.json")
    print(f"   Total events on calendar: {len(event_entries)}")
    print(f"   Skipped (non-{COUNTRY_CODE}): {skipped_country}")
    print(f"   Skipped (wrong month): {skipped_month}")
    print(f"   With precise address: {precise_count}")
    print(f"   Successfully geocoded: {len(final_json) - len(failed_geocodes)}")
    if failed_geocodes:
        print(f"   ⚠️  Failed geocoding ({len(failed_geocodes)}): {', '.join(set(failed_geocodes))}")
    print(f"   Unique cached geocodes: {len(_geocode_cache)}")
    print(f"{'='*60}")


if __name__ == "__main__":
    scrape_ophardt()