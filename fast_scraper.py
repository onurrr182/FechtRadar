import scraper
import json
import concurrent.futures
import threading
import re
import requests

scraper.CALENDAR_URL = "https://fencing.ophardt.online/en/calendar?date-from=2025-01-01&date-to=2028-12-31&nation=GER"

print("📡 Loading Ophardt calendar...")
soup = scraper.fetch_page(scraper.CALENDAR_URL)

all_links = soup.find_all('a', href=True)
event_entries = []
seen_ids = set()

for a in all_links:
    href = a.get('href', '')
    if '/widget/event/' not in href: continue
    id_match = re.search(r'/event/(\d+)', href)
    if not id_match: continue
    event_id = id_match.group(1)
    if event_id in seen_ids: continue
    name = a.get_text(strip=True)
    if not name or len(name) < 3 or "show more" in name.lower(): continue
    name = " ".join(name.split())
    seen_ids.add(event_id)
    event_entries.append({"id": event_id, "name": name})

print(f"   Found {len(event_entries)} unique event links on calendar")

geocode_lock = threading.Lock()
original_geocode = scraper.geocode_city

def thread_safe_geocode(city_name, country="Germany"):
    with geocode_lock:
        return original_geocode(city_name, country)

final_json = []
processed_count = 0
count_lock = threading.Lock()

def process_entry(entry):
    global processed_count
    try:
        session = requests.Session()
        session.headers.update({
            "User-Agent": "Mozilla/5.0",
        })
        def my_fetch(url):
            for attempt in range(2):
                try:
                    resp = session.get(url, timeout=8)
                    if resp.status_code == 200:
                        from bs4 import BeautifulSoup
                        return BeautifulSoup(resp.text, 'html.parser')
                except Exception as e:
                    pass
            return None

        event_url = f"{scraper.BASE_URL}/en/widget/event/{entry['id']}"
        soup = my_fetch(event_url)
        if not soup: return None
        
        page_text = soup.get_text(" ", strip=True)
        if scraper.COUNTRY_CODE and scraper.COUNTRY_CODE not in page_text: return None
            
        date_match = re.search(r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{1,2}),\s*(\d{4})', page_text)
        month = None; date_str = ""; year = ""
        if date_match:
            month_map = {"Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6, "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12}
            month = month_map.get(date_match.group(1))
            date_str = f"{date_match.group(1)} {date_match.group(2)}"
            year = date_match.group(3)
        if not date_match:
            dm = re.search(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', page_text)
            if dm:
                month = int(dm.group(2))
                month_names = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
                if 1 <= month <= 12: date_str = f"{month_names[month]} {dm.group(1)}"; year = dm.group(3)
                    
        header_cutoff = page_text.find("Invitation")
        if header_cutoff == -1: header_cutoff = page_text.find("Results")
        if header_cutoff == -1: header_cutoff = min(500, len(page_text))
        header_text = page_text[:header_cutoff]
        
        city = None
        city_match = re.search(r'GER\s+[A-ZÄÖÜa-zé]{2}\s+([A-ZÄÖÜa-zßäöüé][\wßäöüÄÖÜé\-\s/\.]+)', header_text)
        if city_match: city = scraper.clean_city_name(city_match.group(1))
        if not city or len(city) < 2: return None

        precise_addr = None
        inv_url = f"{scraper.BASE_URL}/en/invitation/view/{entry['id']}/html"
        inv_soup = my_fetch(inv_url)
        if inv_soup:
            inv_text = inv_soup.get_text("\n", strip=True)
            zip_match = re.search(r'(\d{5})\s+([A-ZÄÖÜa-zßäöü][A-ZÄÖÜa-zßäöü\-\.]+(?:\s+[A-ZÄÖÜa-zßäöü\-\.]+)*)', inv_text)
            if zip_match:
                zip_code = zip_match.group(1)
                p_city = scraper.clean_city_name(zip_match.group(2).strip())
                if p_city:
                    zip_pos = inv_text.find(zip_match.group(0))
                    if zip_pos > 0:
                        before_zip = inv_text[:zip_pos].strip()
                        lines = before_zip.split('\n')
                        skip_words = ['cookie', 'accept', 'home', 'calendar', 'ranking', 'nation', 'city', 'date', 'referee', 'entries', 'learn more', 'terms', 'imprint', 'biographies', 'manual', 'athlete', 'provided', 'ophardt']
                        street = None; venue = None
                        for line in reversed(lines[-8:]):
                            line = line.strip()
                            if not line or len(line) < 3: continue
                            if any(skip in line.lower() for skip in skip_words): continue
                            if re.search(r'[A-Za-zäöüÄÖÜß\-\.]+\s*\.?\s*\d+', line) and not street:
                                street = line.rstrip(',')
                            elif not venue and len(line) > 3:
                                venue = line.rstrip(',')
                        pos_city = p_city
                        full_addr = f"{zip_code} {pos_city}"
                        if street: full_addr = f"{street}, {full_addr}"
                        if venue and venue != street: full_addr = f"{venue}, {full_addr}"
                        precise_addr = {"venue": venue, "street": street, "zip": zip_code, "city": pos_city, "full": full_addr}
                    else:
                        precise_addr = {"venue": None, "street": None, "zip": zip_code, "city": p_city, "full": f"{zip_code} {p_city}"}
            
        venue_name = None; street_addr = None
        display_address = f"{city}, Germany"
        geocode_query = city
        
        if precise_addr:
            venue_name = precise_addr.get("venue")
            street_addr = precise_addr.get("street")
            noise_patterns = ['\nNation', '\nGermany', '\nDate', '\nCity', 'Nation\n', 'Germany\n', 'Date\n']
            if venue_name:
                for noise in noise_patterns: venue_name = venue_name.replace(noise, '')
                venue_name = venue_name.strip(' ,\n')
            if street_addr:
                for noise in noise_patterns: street_addr = street_addr.replace(noise, '')
                street_addr = street_addr.strip(' ,\n')
            if precise_addr.get("zip") and precise_addr.get("city"):
                p_city = precise_addr['city']
                p_zip = precise_addr['zip']
                display_address = f"{p_zip} {p_city}, Germany"
                geocode_query = f"{p_zip} {p_city}"
                if street_addr:
                    display_address = f"{street_addr}, {display_address}"
                    geocode_query = f"{street_addr}, {p_zip} {p_city}, Germany"
                    
        weapon = scraper.detect_weapon(entry['name'] + " " + header_text)
        age_group = scraper.detect_age_group(entry['name'] + " " + header_text)
        
        lat, lng = thread_safe_geocode(geocode_query)
        if lat is None and geocode_query != city: lat, lng = thread_safe_geocode(city)
        if lat is None:
            clean = re.sub(r'[/\(\)\.\-]', ' ', city).strip()
            clean = re.sub(r'\s+', ' ', clean)
            if clean != city: lat, lng = thread_safe_geocode(clean)
        if lat is None: lat, lng = (51.1657, 10.4515)
            
        display_address = display_address.replace('\n', ', ').replace(',  ', ', ')
        display_address = re.sub(r',\s*,', ',', display_address)
        display_address = re.sub(r'\s+', ' ', display_address).strip(', ')
        
        res = {
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
        }
        return res
    except Exception as e:
        print(f"Error processing {entry['id']}: {e}")
        return None
    finally:
        with count_lock:
            processed_count += 1
            if processed_count % 50 == 0:
                print(f"[{processed_count}/{len(event_entries)}] processed")

print(f"Starting threads (max 30 workers)...")
with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
    futures = [executor.submit(process_entry, entry) for entry in event_entries]
    for future in concurrent.futures.as_completed(futures):
        res = future.result()
        if res: final_json.append(res)

print("Filtering events without date & saving...")
final_json = [f for f in final_json if f.get('date')]
final_json.sort(key=lambda x: (x.get('year', ''), x.get('date', '')))

with open('tournaments.json', 'w', encoding='utf-8') as f:
    json.dump(final_json, f, indent=4, ensure_ascii=False)

print(f"Done! Saved {len(final_json)} valid events to tournaments.json")
