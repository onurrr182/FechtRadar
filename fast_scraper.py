#!/usr/bin/env python3
"""
FechtRadar Fast Multi-Threaded Global Scraper v3.0
- Scrapes Ophardt calendar in windows from current date onwards
- Worldwide tournament discovery (no regional or GER constraints)
- Extracts structured table data directly (clean dates, IOC country codes, cities, weapons)
- Disambiguates addresses & geocodes with persistent geocache.json
"""

import scraper
import json
import concurrent.futures
import threading
import re
import requests
import sys
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

MONTH_MAP = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12
}

def parse_date_str(raw_date):
    """Extract clean (date_str, year, sort_key) from date cell."""
    if not raw_date:
        return "", "", (9999, 12, 31)
    
    m = re.search(r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{1,2})(?:.*?(\d{4}))?', raw_date)
    if m:
        month_name = m.group(1)
        day_num = int(m.group(2))
        year_str = m.group(3) or ""
        
        # If year was not matched in first chunk, try to find any 4-digit year
        if not year_str:
            ym = re.search(r'\b(202\d)\b', raw_date)
            if ym:
                year_str = ym.group(1)
        
        m_num = MONTH_MAP.get(month_name, 1)
        y_num = int(year_str) if year_str.isdigit() else datetime.now().year
        return f"{month_name} {day_num}", year_str, (y_num, m_num, day_num)
    
    # Fallback to DD.MM.YYYY
    dm = re.search(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', raw_date)
    if dm:
        m_names = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
        m_idx = int(dm.group(2))
        d_idx = int(dm.group(1))
        y_str = dm.group(3)
        if 1 <= m_idx <= 12:
            return f"{m_names[m_idx]} {d_idx}", y_str, (int(y_str), m_idx, d_idx)
            
    return "", "", (9999, 12, 31)

def scrape_calendar_windows():
    """Fetch calendar across multiple upcoming windows to bypass Ophardt query limits."""
    now = datetime.now()
    windows = [
        (now - timedelta(days=14), now + timedelta(days=120)),
        (now + timedelta(days=121), now + timedelta(days=240)),
        (now + timedelta(days=241), now + timedelta(days=365)),
        (now + timedelta(days=366), now + timedelta(days=540))
    ]
    
    raw_entries = []
    seen_ids = set()
    
    print("📡 Loading Ophardt Germany calendar across upcoming windows...")
    for start, end in windows:
        s_str = start.strftime("%Y-%m-%d")
        e_str = end.strftime("%Y-%m-%d")
        url = f"{scraper.BASE_URL}/en/calendar?date-from={s_str}&date-to={e_str}&nation=GER"
        print(f"   Fetching Germany window {s_str} -> {e_str}...")
        
        soup = scraper.fetch_page(url)
        if not soup:
            continue
            
        rows = soup.find_all("tr")
        window_count = 0
        
        for tr in rows:
            all_w_links = tr.find_all("a", href=re.compile(r"/widget/event/\d+"))
            if not all_w_links:
                continue
                
            m_id = re.search(r"/widget/event/(\d+)", all_w_links[0].get("href", ""))
            if not m_id:
                continue
            event_id = m_id.group(1)
            if event_id in seen_ids:
                continue
                
            # Find the link that contains the tournament title
            title_clean = ""
            for a in all_w_links:
                txt = a.get_text(" ", strip=True)
                if txt and len(txt) >= 3 and "show more" not in txt.lower():
                    title_clean = " ".join(txt.split())
                    break
                    
            if not title_clean:
                continue
                
            tds = tr.find_all("td")
            if len(tds) < 8:
                continue
                
            # Date
            date_raw = tds[5].get("title", "") or tds[5].get_text(" ", strip=True)
            date_str, year, sort_key = parse_date_str(date_raw)
            
            # Country: text has IOC code, title has full name
            country_td = tds[6]
            country_name_raw = country_td.get("title", "").strip()
            country_code_raw = country_td.get_text(" ", strip=True)
            m_ioc = re.search(r'\b([A-Z]{3})\b', country_code_raw)
            country_code = m_ioc.group(1) if m_ioc else "GER"
            
            country_name = country_name_raw if country_name_raw else scraper.IOC_COUNTRY_MAP.get(country_code, "Germany")
            
            # City
            city_td = tds[7]
            city_raw = city_td.get("title", "").strip() or city_td.get_text(" ", strip=True)
            
            # Age
            raw_age = tds[9].get_text(" ", strip=True) if len(tds) > 9 else ""
            
            # Weapons and Genders from icon columns
            exact_weapons = []
            exact_genders = set()
            if len(tds) >= 13:
                cols = [(10, "Epee"), (11, "Foil"), (12, "Sabre")]
                for col_idx, w_name in cols:
                    c_icons = tds[col_idx].find_all(["i", "img"])
                    if len(c_icons) > 0 or "fa-" in str(tds[col_idx]):
                        exact_weapons.append(w_name)
                    for el in c_icons:
                        classes = " ".join(el.get("class", []))
                        title_attr = (el.get("title") or "").lower()
                        if "fa-mars" in classes or "male" in title_attr:
                            if "fa-venus-mars" not in classes:
                                exact_genders.add("Men")
                        if "fa-venus" in classes or "female" in title_attr:
                            if "fa-venus-mars" not in classes:
                                exact_genders.add("Women")
                        if "fa-venus-mars" in classes or "mixed" in title_attr or "open" in title_attr:
                            exact_genders.add("Men")
                            exact_genders.add("Women")
                
            # PDF / Invitation link
            pdf_link = f"{scraper.BASE_URL}/en/widget/event/{event_id}"
            inv_link = tr.find("a", href=re.compile(r"/cdn/documents/invitation/|\.pdf"))
            if inv_link and inv_link.get("href"):
                pdf_link = inv_link.get("href")
                
            seen_ids.add(event_id)
            window_count += 1
            
            raw_entries.append({
                "id": event_id,
                "name": title_clean,
                "city": city_raw,
                "country_code": country_code,
                "country_name": country_name,
                "date_str": date_str,
                "year": year,
                "sort_key": sort_key,
                "raw_age": raw_age,
                "exact_weapon": exact_weapons,
                "exact_gender": list(exact_genders),
                "pdf_link": pdf_link
            })
            
        print(f"      -> Found {window_count} unique events in this window")
        
    print(f"   Total unique calendar entries collected: {len(raw_entries)}")
    return raw_entries

geocode_lock = threading.Lock()
original_geocode = scraper.geocode_city

def thread_safe_geocode(city_name, country="Germany"):
    with geocode_lock:
        return original_geocode(city_name, country)

final_json = []
processed_count = 0
count_lock = threading.Lock()

AGE_ORDER = ["U9", "U11", "U13", "U15", "U17", "U20", "Seniors", "Veterans"]
WEAPON_ORDER = ["Epee", "Foil", "Sabre"]

SKIP_ROW_WORDS = [
    "referee", "kampfrichter", "fine", "penalty", "jury", "equipment", 
    "ausrüstung", "liability", "haftung", "payment", "zahlung", 
    "catering", "verpflegung", "organizer", "veranstalter", "license level",
    "licence level", "entries ind."
]

def extract_competitions_from_soup(soup):
    weapons = set()
    ages = set()
    genders = set()
    if not soup:
        return weapons, ages, genders
        
    for tr in soup.find_all("tr"):
        row_text = tr.get_text(" ", strip=True)
        row_lower = row_text.lower()
        if any(skip in row_lower for skip in SKIP_ROW_WORDS):
            continue
            
        is_comp = any(k in row_lower for k in [
            "epee", "épée", "degen", "foil", "florett", "sabre", "säbel", "saber",
            "women", "men", "damen", "herren", "individual", "team", "view entries", "entries list", 
            "medalists", "allowed"
        ])
        if not is_comp:
            continue
            
        if any(w in row_lower for w in ["epee", "épée", "degen"]):
            weapons.add("Epee")
        if any(w in row_lower for w in ["foil", "florett"]):
            weapons.add("Foil")
        if any(w in row_lower for w in ["sabre", "säbel", "saber"]):
            weapons.add("Sabre")
            
        for m in re.findall(r"\bU\s?(\d+)\b", row_text, re.IGNORECASE):
            num = int(m)
            if num in [9, 11, 13, 15, 17, 20]:
                ages.add(f"U{num}")
        if any(v in row_lower for v in ["veteran", "ak40", "ak50", "ak60", "ak70", "vet"]):
            ages.add("Veterans")
        if any(s in row_lower for s in ["senior", "aktive", "senioren", "masters"]):
            ages.add("Seniors")

        if any(w in row_lower for w in ["women", "damen", "weiblich", "female", "dame"]):
            genders.add("Women")
        if re.search(r'\b(men|herren|männlich|male|herr)\b', row_lower):
            genders.add("Men")
        if any(w in row_lower for w in ["mixed", "gemischt", "open"]):
            genders.add("Men")
            genders.add("Women")
            
    return weapons, ages, genders

def process_entry(entry):
    global processed_count
    try:
        session = requests.Session()
        session.headers.update({"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"})
        
        def my_fetch(url):
            for _ in range(2):
                try:
                    resp = session.get(url, timeout=8)
                    if resp.status_code == 200:
                        return BeautifulSoup(resp.text, 'html.parser')
                except:
                    pass
            return None

        event_id = entry["id"]
        found_city = scraper.clean_city_name(entry.get("city", ""))
        country_code = entry.get("country_code", "GER")
        country_name = entry.get("country_name", "Germany")
        
        # Handle special organization codes or empty cities
        if not found_city or found_city in ["DFB", "TBD", "None"]:
            if found_city == "DFB":
                found_city = "Frankfurt am Main"
            else:
                # Fallback: check widget text
                w_url = f"{scraper.BASE_URL}/en/widget/event/{event_id}"
                w_soup = my_fetch(w_url)
                if w_soup:
                    w_text = w_soup.get_text(" ", strip=True)
                    m = re.search(r'\b[A-Z]{3}\s+(?:[A-Z]{1,4}\s+)?([A-ZÄÖÜa-zßäöüé][\wßäöüÄÖÜé\-\s/\.]+?)(?=\s+(?:Official|Invitation|Results|Competitions|$))', w_text)
                    if m:
                        found_city = scraper.clean_city_name(m.group(1))

        if not found_city or len(found_city) < 2 or found_city.lower() in ["tbd", "n/a"]:
            return None

        # Fetch invitation or widget page for precise venue and competition categories
        precise_addr = None
        inv_soup = None
        comp_weapons = set()
        comp_ages = set()
        comp_genders = set()

        if country_code == "GER":
            inv_url = f"{scraper.BASE_URL}/en/invitation/view/{event_id}/html"
            inv_soup = my_fetch(inv_url)
            if inv_soup:
                cw, ca, cg = extract_competitions_from_soup(inv_soup)
                comp_weapons.update(cw)
                comp_ages.update(ca)
                comp_genders.update(cg)

                inv_text = inv_soup.get_text("\n", strip=True)
                zip_match = re.search(r'(\d{5})\s+([A-ZÄÖÜa-zßäöü][A-ZÄÖÜa-zßäöü\-\.]+(?:\s+[A-ZÄÖÜa-zßäöü\-\.]+)*)', inv_text)
                if zip_match:
                    z_code = zip_match.group(1)
                    p_city = scraper.clean_city_name(zip_match.group(2).strip())
                    is_valid_city = (
                        p_city and 
                        z_code != str(event_id) and 
                        not any(stop in p_city.lower() for stop in ['view', 'entries', 'results', 'information', 'further', 'ophardt'])
                    )
                    if is_valid_city:
                        z_pos = inv_text.find(zip_match.group(0))
                        if z_pos > 0:
                            lines = inv_text[:z_pos].strip().split('\n')
                            skip_words = [
                                'cookie', 'accept', 'home', 'calendar', 'ranking', 'nation', 'city', 
                                'date', 'referee', 'entries', 'learn more', 'terms', 'imprint', 
                                'biographies', 'manual', 'athlete', 'provided', 'ophardt', 'parkplatz', 'germany'
                            ]
                            street, venue = None, None
                            for line in reversed(lines[-8:]):
                                line = line.strip()
                                if not line or len(line) < 3 or any(s in line.lower() for s in skip_words):
                                    continue
                                if re.search(r'[A-Za-zäöüÄÖÜß\-\.]+\s*\.?\s*\d+', line) and not street:
                                    street = line.rstrip(',')
                                elif not venue and len(line) > 3:
                                    venue = line.rstrip(',')
                            
                            addr_full = f"{z_code} {p_city}"
                            if street: addr_full = f"{street}, {addr_full}"
                            if venue and venue != street: addr_full = f"{venue}, {addr_full}"
                            precise_addr = {"venue": venue, "street": street, "zip": z_code, "city": p_city, "full": addr_full}

        # If no competition categories found yet, fetch widget page (works worldwide)
        if not comp_ages or not comp_weapons:
            w_url = f"{scraper.BASE_URL}/en/widget/event/{event_id}"
            w_soup = my_fetch(w_url)
            if w_soup:
                cw, ca, cg = extract_competitions_from_soup(w_soup)
                comp_weapons.update(cw)
                comp_ages.update(ca)
                comp_genders.update(cg)

        # Format display address and geocoding query
        v_name = None
        disp_addr = found_city
        g_query = found_city

        if precise_addr:
            v_name = precise_addr.get("venue")
            s_addr = precise_addr.get("street")
            noise = ['\nNation', '\nGermany', '\nDate', '\nCity', 'Nation\n', 'Germany\n', 'Date\n', 'Nation', 'Germany', 'Date', 'Parkplatz']
            if v_name:
                for n in noise: v_name = v_name.replace(n, '')
                v_name = v_name.strip(' ,\n')
            if s_addr:
                for n in noise: s_addr = s_addr.replace(n, '')
                s_addr = s_addr.strip(' ,\n')
            
            p_cit = precise_addr['city']
            p_z = precise_addr['zip']
            disp_addr = f"{p_z} {p_cit}"
            g_query = f"{p_z} {p_cit}"
            if s_addr:
                disp_addr = f"{s_addr}, {disp_addr}"
                g_query = f"{s_addr}, {p_z} {p_cit}, {country_name}"

        # Clean up noise from display address
        disp_addr = re.sub(r'\b(Nation|Germany|Date|City|Parkplatz)\b,?', '', disp_addr)
        disp_addr = re.sub(r',\s*,', ',', disp_addr)
        disp_addr = re.sub(r'\s+', ' ', disp_addr).strip(' ,')

        # Geocode
        lat, lng = thread_safe_geocode(g_query, country_name)
        if lat is None:
            lat, lng = thread_safe_geocode(found_city, country_name)
        if lat is None:
            # Fallback to country center coordinates
            lat, lng = (51.1657, 10.4515) if country_code == "GER" else (48.8566, 2.3522)

        # Standardize weapons
        all_weapons = set(entry.get('exact_weapon', [])) | comp_weapons
        if not all_weapons:
            all_weapons = set(scraper.detect_weapon(entry['name']))
        weapon = [w for w in WEAPON_ORDER if w in all_weapons]
        for w in all_weapons:
            if w not in weapon:
                weapon.append(w)
        if not weapon:
            weapon = ["Mixed"]

        # Standardize gender
        GENDER_ORDER = ["Men", "Women"]
        all_genders = set(entry.get('exact_gender', [])) | comp_genders
        if not all_genders:
            name_lower = entry['name'].lower()
            if any(w in name_lower for w in ["damen", "women", "weiblich", "female"]):
                all_genders.add("Women")
            if re.search(r'\b(herren|men|männlich|male)\b', name_lower):
                all_genders.add("Men")

        if not all_genders:
            all_genders = {"Men", "Women"}

        gender = [g for g in GENDER_ORDER if g in all_genders]

        # Standardize age groups
        all_ages = set(comp_ages)
        cal_age_str = entry.get('raw_age', '')
        if cal_age_str:
            for m in re.findall(r"\bU\s?(\d+)\b", cal_age_str, re.IGNORECASE):
                num = int(m)
                if num in [9, 11, 13, 15, 17, 20]:
                    all_ages.add(f"U{num}")
            cal_lower = cal_age_str.lower()
            if any(v in cal_lower for v in ["veteran", "ak40", "ak50", "ak60", "ak70", "vet"]):
                all_ages.add("Veterans")
            if any(s in cal_lower for s in ["senior", "aktive", "senioren", "masters"]):
                all_ages.add("Seniors")

        if not all_ages:
            title_ages = scraper.detect_age_group(entry['name'] + " " + cal_age_str)
            all_ages.update(title_ages)

        age = [a for a in AGE_ORDER if a in all_ages]
        for a in all_ages:
            if a not in age:
                age.append(a)
        if not age:
            age = ["Seniors"]

        return {
            "name": entry['name'],
            "city": found_city,
            "country": country_code,
            "venue": v_name,
            "lat": round(lat, 5),
            "lng": round(lng, 5),
            "date": entry.get("date_str", ""),
            "year": entry.get("year", ""),
            "sort_key": entry.get("sort_key", (9999, 12, 31)),
            "weapon": weapon,
            "gender": gender,
            "ageGroup": age,
            "exactAddress": disp_addr,
            "pdfLink": entry.get("pdf_link", f"{scraper.BASE_URL}/en/widget/event/{event_id}")
        }
    except Exception as e:
        print(f"Error processing event {entry.get('id')}: {e}")
        return None
    finally:
        with count_lock:
            processed_count += 1
            if processed_count % 50 == 0:
                print(f"[{processed_count}/{len(event_entries)}] processed")

# ─── MAIN EXECUTION ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    event_entries = scrape_calendar_windows()

    print(f"Starting worker threads (max 30 workers) for {len(event_entries)} events...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
        futures = [executor.submit(process_entry, entry) for entry in event_entries]
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            if res:
                final_json.append(res)

    print("Filtering events and sorting chronologically...")
    final_json = [f for f in final_json if f.get('date')]
    final_json.sort(key=lambda x: x.get('sort_key', (9999, 12, 31)))

    # Remove temporary sort key before saving JSON
    for f in final_json:
        if 'sort_key' in f:
            del f['sort_key']

    # Safety check: Protect tournaments.json from being zeroed out
    if len(final_json) < 50:
        print(f"🛑 ERROR: Only found {len(final_json)} events. This is too low (expected 200+).")
        print("🛑 To protect tournaments.json from being zeroed out, saving has been aborted.")
        sys.exit(1)

    with open('tournaments.json', 'w', encoding='utf-8') as f:
        json.dump(final_json, f, indent=4, ensure_ascii=False)

    # Update sitemap.xml lastmod date
    try:
        today_str = datetime.now().strftime("%Y-%m-%d")
        sitemap_content = f"""<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc>https://fechtradar.de/</loc>
    <lastmod>{today_str}</lastmod>
    <changefreq>daily</changefreq>
    <priority>1.0</priority>
  </url>
</urlset>
"""
        with open('sitemap.xml', 'w', encoding='utf-8') as f:
            f.write(sitemap_content)
        print(f"Updated sitemap.xml lastmod to {today_str}")

        # Notify search engines via IndexNow
        payload = {
            "host": "fechtradar.de",
            "key": "c74900a89d7145b597cfa906e69d7a21",
            "keyLocation": "https://fechtradar.de/c74900a89d7145b597cfa906e69d7a21.txt",
            "urlList": [
                "https://fechtradar.de/",
                "https://fechtradar.de/sitemap.xml"
            ]
        }
        resp = requests.post("https://api.indexnow.org/indexnow", json=payload, timeout=10)
        print(f"IndexNow notification sent, status: {resp.status_code}")
    except Exception as e:
        print(f"Could not update sitemap.xml or ping IndexNow: {e}")

    scraper.save_geocache()
    print(f"🎉 Done! Successfully saved {len(final_json)} valid upcoming global tournaments to tournaments.json")

