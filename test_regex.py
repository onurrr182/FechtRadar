import re

texts = [
    """May 16, 2026 - May 16, 2026
GER NS Munster""",
    """# 5 Hamburger Hansepokal Wertungsturnier der norddeutschen Hansepokalserie U11,U13,U15
Sep 12, 2026 - Sep 13, 2026
GER HH Hamburg"""
]

for t in texts:
    match = re.search(r'\b([A-Z]{3})\s+(?:[A-Za-zÄÖÜäöüßé]{1,4}\s+)?([A-ZÄÖÜa-zßäöüé][\wßäöüÄÖÜé\-\s/\.]+)', t)
    if match:
        print(f"MATCH! Group 1: '{match.group(1)}', Group 2: '{match.group(2)}'")
    else:
        print("NO MATCH!")

