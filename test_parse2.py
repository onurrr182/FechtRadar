import bs4
soup = bs4.BeautifulSoup(open('calendar.html').read(), 'html.parser')
for tr in soup.find_all('tr'):
    a = tr.find('a', href=lambda h: h and '/widget/event/' in h)
    if a and '1 NRW-Cup' in a.get_text():
        # Let's print all weapon icons or text
        for td in tr.find_all('td'):
            print(f"TD TEXT: {td.get_text(strip=True)}")
            imgs = td.find_all('img')
            for img in imgs:
                print(f"  IMG: {img.get('src', '')} title={img.get('title', '')}")
            icons = td.find_all('i')
            for i in icons:
               print(f"  ICON: {i.get('class', [])} title={i.get('title', '')}")
        break
