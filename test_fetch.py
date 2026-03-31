import requests
import re

url = "https://fencing.ophardt.online/en/calendar"
headers = {
    "User-Agent": "Mozilla/5.0",
}
def count_events(params):
    r = requests.get(url, params=params, headers=headers)
    return len(list(set(re.findall(r'/event/(\d+)', r.text))))

print("GER only:", count_events({"nation": "GER"}))
print("GER empty dates:", count_events({"nation": "GER", "date-from": "", "date-to": ""}))
print("GER large range:", count_events({"nation": "GER", "date-from": "2024-01-01", "date-to": "2028-12-31"}))
print("GER 2026/2027:", count_events({"nation": "GER", "date-from": "2026-01-01", "date-to": "2027-12-31"}))
print("GER only future:", count_events({"nation": "GER", "date-from": "2026-03-31", "date-to": "2027-12-31"}))

