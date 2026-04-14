import scraper
import json
import concurrent.futures
import threading
import re
import requests
import sys

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
    
    
    raw_age = ""
    exact_weapon = []
    try:
        parent_td = a.find_parent('td')
        if parent_td:
            age_td = parent_td.find_next_sibling('td')
            if age_td: 
                raw_age = age_td.get_text(" ", strip=True)
                epee_td = age_td.find_next_sibling('td')
                foil_td = epee_td.find_next_sibling('td') if epee_td else None
                sabre_td = foil_td.find_next_sibling('td') if foil_td else None
                
                ws = []
                if epee_td and epee_td.find('i'): ws.append("Epee")
                if foil_td and foil_td.find('i'): ws.append("Foil")
                if sabre_td and sabre_td.find('i'): ws.append("Sabre")
                
                if len(ws) > 0:
                    exact_weapon = ws
    except: pass
    
    event_entries.append({"id": event_id, "name": name, "raw_age": raw_age, "exact_weapon": exact_weapon})

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
        session.headers.update({"User-Agent": "Mozilla/5.0"})
        
        def my_fetch(url):
            for _ in range(2):
                try:
                    resp = session.get(url, timeout=8)
                    if resp.status_code == 200:
                        from bs4 import BeautifulSoup
                        return BeautifulSoup(resp.text, 'html.parser')
                except: pass
            return None

        # 1. Fetch Event Widget
        event_url = f"{scraper.BASE_URL}/en/widget/event/{entry['id']}"
        soup = my_fetch(event_url)
        if not soup: return None
        
        page_text = soup.get_text(" ", strip=True)
        
        # 2. Extract Date
        date_str, year = "", ""
        date_match = re.search(r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{1,2}),\s*(\d{4})', page_text)
        if date_match:
            date_str = f"{date_match.group(1)} {date_match.group(2)}"
            year = date_match.group(3)
        else:
            dm = re.search(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', page_text)
            if dm:
                m_names = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
                m_idx = int(dm.group(2))
                if 1 <= m_idx <= 12:
                    date_str = f"{m_names[m_idx]} {dm.group(1)}"
                    year = dm.group(3)

        # 3. Extract City and Country
        header_cutoff = page_text.find("Invitation")
        if header_cutoff == -1: header_cutoff = page_text.find("Results")
        if header_cutoff == -1: header_cutoff = min(500, len(page_text))
        header_text = page_text[:header_cutoff]
        
        found_city = None
        found_country = "Germany" # Absolute default fallback
        
        # Regex for "IOC REGION City" or "IOC City"
        city_match = re.search(r'\b([A-Z]{3})\s+(?:[A-Za-zÄÖÜäöüßé]{1,4}\s+)?([A-ZÄÖÜa-zßäöüé][\wßäöüÄÖÜé\-\s/\.]+)', header_text)
        if city_match:
            found_country = city_match.group(1)
            found_city = scraper.clean_city_name(city_match.group(2))
        else:
            # Fallback to 2nd line of header
            h_lines = [L.strip() for L in header_text.split('\n') if L.strip()]
            if len(h_lines) > 1:
                found_city = scraper.clean_city_name(h_lines[1])
        
        if not found_city or len(found_city) < 2: return None

        # 4. Fetch Invitation for precise address
        precise_addr = None
        inv_url = f"{scraper.BASE_URL}/en/invitation/view/{entry['id']}/html"
        inv_soup = my_fetch(inv_url)
        if inv_soup:
            inv_text = inv_soup.get_text("\n", strip=True)
            zip_match = re.search(r'(\d{5})\s+([A-ZÄÖÜa-zßäöü][A-ZÄÖÜa-zßäöü\-\.]+(?:\s+[A-ZÄÖÜa-zßäöü\-\.]+)*)', inv_text)
            if zip_match:
                z_code = zip_match.group(1)
                p_city = scraper.clean_city_name(zip_match.group(2).strip())
                if p_city:
                    z_pos = inv_text.find(zip_match.group(0))
                    if z_pos > 0:
                        lines = inv_text[:z_pos].strip().split('\n')
                        skip_words = ['cookie', 'accept', 'home', 'calendar', 'ranking', 'nation', 'city', 'date', 'referee', 'entries', 'learn more', 'terms', 'imprint', 'biographies', 'manual', 'athlete', 'provided', 'ophardt']
                        street, venue = None, None
                        for line in reversed(lines[-8:]):
                            line = line.strip()
                            if not line or len(line) < 3 or any(s in line.lower() for s in skip_words): continue
                            if re.search(r'[A-Za-zäöüÄÖÜß\-\.]+\s*\.?\s*\d+', line) and not street:
                                street = line.rstrip(',')
                            elif not venue and len(line) > 3:
                                venue = line.rstrip(',')
                        addr_full = f"{z_code} {p_city}"
                        if street: addr_full = f"{street}, {addr_full}"
                        if venue and venue != street: addr_full = f"{venue}, {addr_full}"
                        precise_addr = {"venue": venue, "street": street, "zip": z_code, "city": p_city, "full": addr_full}

        # 5. Geocode and finalize
        v_name, s_addr = None, None
        disp_addr = f"{found_city}, {found_country}"
        g_query = found_city
        
        if precise_addr:
            v_name = precise_addr.get("venue")
            s_addr = precise_addr.get("street")
            noise = ['\nNation', '\nGermany', '\nDate', '\nCity', 'Nation\n', 'Germany\n', 'Date\n']
            if v_name:
                for n in noise: v_name = v_name.replace(n, '')
                v_name = v_name.strip(' ,\n')
            if s_addr:
                for n in noise: s_addr = s_addr.replace(n, '')
                s_addr = s_addr.strip(' ,\n')
            
            p_cit = precise_addr['city']
            p_z = precise_addr['zip']
            disp_addr = f"{p_z} {p_cit}, {found_country}"
            g_query = f"{p_z} {p_cit}"
            if s_addr:
                disp_addr = f"{s_addr}, {disp_addr}"
                g_query = f"{s_addr}, {p_z} {p_cit}, {found_country}"

        lat, lng = thread_safe_geocode(g_query, found_country)
        if lat is None: lat, lng = thread_safe_geocode(found_city, found_country)
        if lat is None: lat, lng = (51.1657, 10.4515) # Last fallback
            
        weapon = entry.get('exact_weapon', [])
        if not weapon: weapon = scraper.detect_weapon(entry['name'] + " " + header_text)
        age = scraper.detect_age_group(entry['name'] + " " + entry.get('raw_age', '') + " " + header_text)
        
        return {
            "name": entry['name'],
            "city": found_city,
            "country": found_country,
            "venue": v_name,
            "lat": round(lat, 5),
            "lng": round(lng, 5),
            "date": date_str,
            "year": year,
            "weapon": weapon,
            "ageGroup": age,
            "exactAddress": disp_addr.replace('\n', ', '),
            "pdfLink": event_url
        }
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

# Safety check: Never zero out tournaments.json if scraper fails
if len(final_json) < 100:
    print(f"🛑 ERROR: Only found {len(final_json)} events. This is too low (expected 1000+).")
    print("🛑 To protect tournaments.json from being zeroed out, saving has been aborted.")
    sys.exit(1)

with open('tournaments.json', 'w', encoding='utf-8') as f:
    json.dump(final_json, f, indent=4, ensure_ascii=False)

scraper.save_geocache()

print(f"Done! Saved {len(final_json)} valid events to tournaments.json")
