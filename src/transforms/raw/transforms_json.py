import requests
from bs4 import BeautifulSoup


def transform_ufcstats(**kwargs):
    url = kwargs["url"]
    
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    event_table = soup.find('table', class_='b-statistics__table-events')
    rows = event_table.findAll("tr")

    def makeData(row):
        segments = row.find_all("td", class_ = "b-statistics__table-col")
        anchor = segments[0].find('a')
        span = segments[0].find('span')
        payload = {
            "event_name": anchor.text.strip(),
            "event_details": anchor["href"],
            "event_date": span.text.strip(),
            "event_locations": segments[1].text.strip(),
        }
        return payload
    
    data = list(map(makeData, [row for row in rows if row.find('a')!=None]))

    return data