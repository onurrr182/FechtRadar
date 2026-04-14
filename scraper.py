"""
FechtRadar Scraper v2.3 — Infinite Scroll & Regional Disambiguation Edition
Uses Playwright for the main calendar (to trigger lazy loading),
and requests + BeautifulSoup for individual pages.
Intelligently maps Ophardt regional codes to German states for perfect geocoding.
"""

import requests
from bs4 import BeautifulSoup
import json
import re
import time
import urllib.parse
import urllib.request
import os

CALENDAR_URL = "https://fencing.ophardt.online/en/calendar?date-from=2025-01-01&date-to=2028-12-31&nation=GER"
BASE_URL = "https://fencing.ophardt.online"

# Months to include (set to None to include all)
INCLUDE_MONTHS = None  

# Country filter
COUNTRY_CODE = "GER"  
# ─── KNOWN GEOCODING ALIASES ──────────────────────────────────────────────
# Used to bypass Nominatim's aggressive population bias for specific cities.
GEOCODE_ALIASES = {
    "Munster": "29633 Munster",
    "Halle": "Halle (Saale)",
    "Freiburg": "Freiburg im Breisgau"
}

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,de;q=0.8"
})

CITY_STOP_WORDS = [
    "Invitation", "Entries", "Results", "Competitions", "Other dates",
    "View", "Live", "Pre-entries", "Official", "website", "Homepage",
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    "Equipment", "Referee", "Participants", "Created", "Modified",
    "Provided", "Ophardt", "fencingworldwide", "Published"
]

IOC_COUNTRY_MAP = {
    "GER": "Germany", "USA": "United States", "FRA": "France", "GBR": "United Kingdom",
    "ITA": "Italy", "ESP": "Spain", "AUT": "Austria", "SUI": "Switzerland",
    "NED": "Netherlands", "BEL": "Belgium", "CAN": "Canada", "POL": "Poland",
    "HUN": "Hungary", "SWE": "Sweden", "DEN": "Denmark", "NOR": "Norway"
}

# ─── OPHARDT REGION TO GERMAN STATE MAPPING ───────────────────────────────
REGION_MAP = {
    "NS": "Niedersachsen",
    "WE": "Nordrhein-Westfalen", # Westfalen
    "NR": "Nordrhein-Westfalen", # Nordrhein
    "BY": "Bayern",
    "HE": "Hessen",
    "BS": "Baden-Württemberg", # Südbaden
    "BN": "Baden-Württemberg", # Nordbaden
    "WÜ": "Baden-Württemberg", # Württemberg
    "SH": "Schleswig-Holstein",
    "HH": "Hamburg",
    "BE": "Berlin",
    "BR": "Bremen",
    "SA": "Sachsen",
    "ST": "Sachsen-Anhalt",
    "TH": "Thüringen",
    "MV": "Mecklenburg-Vorpommern",
    "SR": "Saarland",
    "SW": "Rheinland-Pfalz", # Südwest
    "MR": "Rheinland-Pfalz"  # Mittelrhein
}

# ─── GEOCODING CACHE ─────────────────────────────────────────────────────────
GEO_CACHE_FILE = "geocache.json"
_geocode_cache = {}

def load_geocache():
    """Load the geocoding cache from disk."""
    global _geocode_cache
    if os.path.exists(GEO_CACHE_FILE):
        try:
            with open(GEO_CACHE_FILE, 'r', encoding='utf-8') as f:
                _geocode_cache = json.load(f)
            print(f"✅ Loaded {len(_geocode_cache)} entries from {GEO_CACHE_FILE}")
        except Exception as e:
            print(f"⚠️  Could not load geocache: {e}")

def save_geocache():
    """Save the geocoding cache to disk."""
    try:
        with open(GEO_CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(_geocode_cache, f, indent=4, ensure_ascii=False)
        print(f"💾 Saved {len(_geocode_cache)} entries to {GEO_CACHE_FILE}")
    except Exception as e:
        print(f"⚠️  Could not save geocache: {e}")

# Call load at startup
load_geocache()

def geocode_city(city_name, country="Germany"):
    """Use Nominatim (OpenStreetMap) to geocode a city name to lat/lng.
    
    If country is a 3-letter IOC code, it is mapped to a full name first.
    Results are cached on disk to avoid re-querying daily.
    """
    if not city_name or city_name.strip() == "":
        return None, None
    
    city_name = city_name.strip()
    city_name = GEOCODE_ALIASES.get(city_name, city_name)
    
    # Map IOC codes (like GER, USA, FRA) to full names
    if len(country) == 3 and country.isupper():
        country = IOC_COUNTRY_MAP.get(country, country)
    
    cache_key = f"{city_name}, {country}"
    
    if cache_key in _geocode_cache:
        val = _geocode_cache[cache_key]
        if val and isinstance(val, list) and len(val) == 2:
            return val[0], val[1]
    
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
            clean_query = f"{city_name}, {country}"
            query = urllib.parse.quote(clean_query)
            url = f"https://nominatim.openstreetmap.org/search?q={query}&format=json&limit=1"
            
            if country.lower() in ["germany", "deutschland"]:
                url += "&countrycodes=de"
        
        req = urllib.request.Request(url, headers={
            "User-Agent": "FechtRadar/2.3 (fencing-tournament-map)"
        })
        
        with urllib.request.urlopen(req, timeout=10) as response:
            data = json.loads(response.read().decode())
        
        if data and len(data) > 0:
            lat = float(data[0]["lat"])
            lng = float(data[0]["lon"])
            _geocode_cache[cache_key] = [lat, lng]
            time.sleep(1.1)  # Nominatim rate limit
            return lat, lng
        
        # If structured query returned nothing, retry with free-text as fallback
        if zip_match:
            query = urllib.parse.quote(f"{city_name}, {country}")
            url = f"https://nominatim.openstreetmap.org/search?q={query}&format=json&limit=1"
            if country.lower() in ["germany", "deutschland"]:
                url += "&countrycodes=de"
            req = urllib.request.Request(url, headers={
                "User-Agent": "FechtRadar/2.3 (fencing-tournament-map)"
            })
            with urllib.request.urlopen(req, timeout=10) as response:
                data = json.loads(response.read().decode())
            if data and len(data) > 0:
                lat = float(data[0]["lat"])
                lng = float(data[0]["lon"])
                _geocode_cache[cache_key] = [lat, lng]
                time.sleep(1.1)
                return lat, lng
        
    except Exception as e:
        print(f"  ⚠️  Geocoding failed for '{city_name}': {e}")
    
    _geocode_cache[cache_key] = None
    time.sleep(1.1)
    return None, None


def detect_weapon(text):
    text_lower = text.lower()
    weapons = []
    if any(w in text_lower for w in ["degen", "epee", "épée"]): weapons.append("Epee")
    if any(w in text_lower for w in ["florett", "foil"]): weapons.append("Foil")
    if any(w in text_lower for w in ["säbel", "sabel", "sabre", "saber"]): weapons.append("Sabre")
    return weapons if weapons else ["Mixed"]


def detect_age_group(text):
    text_lower = text.lower()
    groups = []
    u_matches = re.findall(r'\bU\s?(\d+)\b', text, re.IGNORECASE)
    for m in u_matches:
        age = int(m)
        if age in [9, 11, 13, 15, 17, 20]:
            tag = f"U{age}"
            if tag not in groups: groups.append(tag)
    
    if any(w in text_lower for w in ["veteran", "ak40", "ak50", "ak60", "ak70"]) and "Veterans" not in groups: groups.append("Veterans")
    if any(w in text_lower for w in ["senior", "masters", "aktive"]) and "Seniors" not in groups: groups.append("Seniors")
    if any(w in text_lower for w in ["kids", "kinder", "youngster"]) and not any(g.startswith("U") for g in groups) and "U11" not in groups: groups.append("U11")
    if any(w in text_lower for w in ["jugend", "junior", "youth", "cadets"]) and not any(g.startswith("U") for g in groups) and "U17" not in groups: groups.append("U17")
    
    return groups if groups else ["Seniors"]


def clean_city_name(raw_city):
    if not raw_city: return None
    city = raw_city.strip()
    for stop in CITY_STOP_WORDS:
        idx = city.find(stop)
        if idx > 0: city = city[:idx].strip()
    city = re.split(r'\s{2,}|\s+\d{4}\b', city)[0].strip()
    city = re.sub(r'[\d\s,\.\-]+$', '', city).strip()
    return city if len(city) >= 2 else None


def fetch_page(url):
    for attempt in range(3):
        try:
            resp = SESSION.get(url, timeout=15)
            resp.raise_for_status()
            return BeautifulSoup(resp.text, 'html.parser')
        except Exception as e:
            if attempt == 2: print(f"  ⚠️  Failed to fetch {url}: {e}")
            time.sleep(2)
    return None

def fetch_page_playwright(url):
    from playwright.sync_api import sync_playwright
    import time
    print(f"  --> Launching Playwright to crawl {url} (this takes a couple minutes)")
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(viewport={'width': 1920, 'height': 1080})
            page = context.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=90000)
            
            try:
                cookie_btn = page.locator("button:has-text('Accept'), button:has-text('Zustimmen')").first
                if cookie_btn.is_visible(timeout=3000):
                    cookie_btn.click()
                    time.sleep(1)
            except: pass
            
            print("  --> Scrolling down to load all events...")
            retries = 0
            
            while retries < 3:
                elements = page.locator("a[href*='/widget/event/']").all()
                current_count = len(elements)
                
                if current_count > 0: elements[-1].scroll_into_view_if_needed()
                
                page.keyboard.press("End")
                time.sleep(3)
                
                new_count = len(page.locator("a[href*='/widget/event/']").all())
                
                if new_count == current_count:
                    retries += 1
                    time.sleep(2)
                else:
                    retries = 0
                    print(f"      ... loaded {new_count} events so far")
            
            time.sleep(1)
            html = page.content()
            browser.close()
            return BeautifulSoup(html, 'html.parser')
            
    except Exception as e:
        print(f"Playwright error fetching {url}: {e}")
        return None


def get_precise_address(event_id):
    url = f"{BASE_URL}/en/invitation/view/{event_id}/html"
    soup = fetch_page(url)
    if not soup: return None
    
    page_text = soup.get_text("\n", strip=True)
    zip_match = re.search(r'(\d{5})\s+([A-ZÄÖÜa-zßäöü][A-ZÄÖÜa-zßäöü\-\.]+(?:\s+[A-ZÄÖÜa-zßäöü\-\.]+)*)', page_text)
    
    if zip_match:
        zip_code = zip_match.group(1)
        city = clean_city_name(zip_match.group(2).strip())
        if city:
            zip_pos = page_text.find(zip_match.group(0))
            if zip_pos > 0:
                before_zip = page_text[:zip_pos].strip()
                lines = before_zip.split('\n')
                skip_words = ['cookie', 'accept', 'home', 'calendar', 'ranking', 'nation', 'city', 'date', 'referee', 'entries', 'learn more', 'terms', 'imprint', 'biographies', 'manual', 'athlete', 'provided', 'ophardt']
                
                street, venue = None, None
                for line in reversed(lines[-8:]):
                    line = line.strip()
                    if not line or len(line) < 3 or any(skip in line.lower() for skip in skip_words): continue
                    if re.search(r'[A-Za-zäöüÄÖÜß\-\.]+\s*\.?\s*\d+', line) and not street:
                        street = line.rstrip(',')
                    elif not venue and len(line) > 3:
                        venue = line.rstrip(',')
                
                full_addr = f"{zip_code} {city}"
                if street: full_addr = f"{street}, {full_addr}"
                if venue and venue != street: full_addr = f"{venue}, {full_addr}"
                
                return {"venue": venue, "street": street, "zip": zip_code, "city": city, "full": full_addr}
            return {"venue": None, "street": None, "zip": zip_code, "city": city, "full": f"{zip_code} {city}"}
    return None


def scrape_ophardt():
    print("🚀 FechtRadar Scraper v2.3 — Infinite Scroll & Regional Disambiguation Edition")
    
    print("\n📡 Loading Ophardt calendar (with infinite scroll)...")
    soup = fetch_page_playwright(CALENDAR_URL)
    
    if not soup:
        print("❌ Failed to load calendar page")
        return
    
    all_links = soup.find_all('a', href=True)
    event_entries = []
    seen_ids = set()
    
    for a in all_links:
        href = a['href']
        if '/widget/event/' not in href: continue
        id_match = re.search(r'/event/(\d+)', href)
