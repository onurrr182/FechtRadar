import scraper
import json

# Test cases
test_cases = [
    {"city": "29633 Munster", "country": "GER"}, # Expected: Munster (Lower Saxony)
    {"city": "Munster", "country": "GER"},       # Expected: Munster (Lower Saxony), not Ireland
    {"city": "5 Hamburger Hansepokal", "country": "GER"}, # Expected: Hamburg
]

for tc in test_cases:
    print(f"Testing geocoding for: {tc['city']} ({tc['country']})")
    lat, lng = scraper.geocode_city(tc['city'], tc['country'])
    if lat and lng:
        print(f"  --> Success! Lat: {lat}, Lng: {lng}")
    else:
        print(f"  --> FAILED!")

