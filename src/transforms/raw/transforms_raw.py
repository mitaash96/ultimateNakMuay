import requests
from bs4 import BeautifulSoup
import pandas as pd
from .utils_raw import get_events_df, getData


def transform_fast_read(**kwargs):
    url = kwargs["url"]
    table_key = kwargs["table_key"]

    df = pd.read_html(url)[table_key]

    return df


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


def transform_wiki_events_ufc(**kwargs):
    url = kwargs["url"]
    response = requests.get(url)

    soup = BeautifulSoup(response.text, 'html.parser')
    past_events_table = soup.find('table', class_='wikitable', id="Past_events")
    scheduled_events_table = soup.find('table', class_='wikitable', id="Scheduled_events")

    df = pd.concat(list(map(get_events_df, [past_events_table, scheduled_events_table])), ignore_index=True)

    return df


def transform_wiki_events_onefc(**kwargs):
    url = kwargs["url"]
    response = requests.get(url)

    soup = BeautifulSoup(response.text, 'html.parser')
    events_table = soup.find('table', class_='wikitable')

    df = get_events_df(events_table)

    return df


def transform_wiki_events_bellator(**kwargs):
    url = kwargs["url"]
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    events_table = soup.find('table', class_='wikitable')

    headers = [th.text.strip() for th in events_table.find_all("th", scope="col")[1:]]
    headers = ["EventID", *headers]

    rows = []
    for tr in events_table.find_all('tr')[1:]:
        row = [tr.find('th').text.strip()]  if tr.find('th') != None else []
        for td in tr.find_all('td'):
            row.append(td.text.strip())
        rows.append(row)
    
    df = pd.DataFrame(rows, columns=headers)

    return df


def transform_wiki_events_glory(**kwargs):
    return transform_wiki_events_onefc(**kwargs)


def transform_wiki_results_onefc(**kwargs):
    url = kwargs["url"]
    soup = BeautifulSoup(requests.get(url).text,"html.parser")
    table = soup.find_all("table", class_ = "sortable")[0]
    table_rows = table.find_all("tr")[1:]
    anchors = [_.find_all("a") for _ in table_rows if len(_.find_all("a"))>0]
    cols = ["link", "name"]
    links_df = pd.DataFrame([(anchor[0].get("href"), anchor[0].text) for anchor in anchors], columns=cols)

    clean_link = lambda x: f"https://en.wikipedia.org/{x.split('#')[0]}"
    links_df["link_clean"] = links_df["link"].apply(clean_link)
    links = links_df["link_clean"].value_counts().reset_index().iloc[:,0]
    links = links.to_list()
    urls = list(set([link for link in links if "one_" in link.lower()]))

    df = pd.concat([getData(url) for url in urls], ignore_index=True)

    return df
