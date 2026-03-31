import bs4
soup = bs4.BeautifulSoup(open('calendar.html').read(), 'html.parser')
for tr in soup.find_all('tr')[:50]:
    a = tr.find('a', href=lambda h: h and '/widget/event/' in h)
    if a:
        tds = [td.get_text(strip=True) for td in tr.find_all('td')]
        print(tds)
        break
