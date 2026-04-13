import sys
sys.path.insert(0, '/Users/onurkeles/Desktop/FechtRadarGlobal')
import scraper
import fast_scraper
import xml.etree.ElementTree as ET
import re

soup = scraper.fetch_page("https://fencing.ophardt.online/en/calendar?date-from=2025-01-01&date-to=2028-12-31")
all_links = soup.find_all('a', href=True)
links = []
count = 0
for a in all_links:
    href = a.get('href', '')
    if '/widget/event/' in href:
        links.append({'id': re.search(r'/event/(\d+)', href).group(1), 'name': a.get_text()})
        count += 1
        if count >= 3: break

for l in links:
    print(f"\nProcessing {l['id']} - {l['name']}")
    res = fast_scraper.process_entry(l)
    print("Result:", res)
