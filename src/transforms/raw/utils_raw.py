import os
import json
import pandas as pd
from bs4 import BeautifulSoup
import requests


def saveFile(fileFormat, filename, file):
    
    if fileFormat == "json":
        if os.path.exists(filename):
            os.remove(filename)
        with open(filename, 'w') as json_file:
            json.dump(file, json_file, indent=4)
    elif fileFormat == "csv":
        file.to_csv(filename, mode="w", index=False)
    
    print("-----------saved file-----------\n\n")

    return None


def get_events_df(table):
    headers = [_.text.strip() for _ in table.find_all('th')]
    
    rows = [
        [td.text.strip() for td in tr.find_all('td')]
        for tr in table.find_all('tr')[1:]
        ]
    
    df = pd.DataFrame(rows, columns=headers)

    return df


def cleanResults(result):
    event_name = result["event"]
    df = result["df"]
    df.columns = [_[1] for _ in df.columns]

    split_by_card = [(i, df.iloc[i,0]) for i in df[df.eq(df.iloc[:, 0], axis=0).all(axis=1)].index]
    if 0 not in [_[0] for _ in split_by_card]:
        split_by_card = [(0, "Main card"), *split_by_card]

    for i in range(len(split_by_card)-1):
        split_by_card[i] = (split_by_card[i][0], split_by_card[i+1][0], split_by_card[i][1])

    split_by_card[-1] = (split_by_card[-1][0], len(df), split_by_card[-1][1])

    sdfs = []
    for start, end, card in split_by_card:
        sdf = df.iloc[start:end, :]
        sdf = sdf.assign(fight_card = card)
        sdfs.append(sdf)
    
    df = pd.concat(sdfs).drop([i[0] for i in split_by_card[1:]]).reset_index(drop=True)

    cols2rename = {
        x: x.lower().replace(' ', '_') for x in df.columns
    }

    cols2rename = {
        **cols2rename,
        **{
            "Unnamed: 1_level_1": "winner",
            "Unnamed: 3_level_1": "loser",
        },
    }

    df.rename(columns=cols2rename, inplace=True)

    df.drop(columns=["unnamed:_2_level_1"], axis=1, inplace=True)

    df = df.assign(event_name = event_name)

    return df


# optimize
def getData_onefc(url):
    dfs = []
    soup = BeautifulSoup(requests.get(url).text, features="lxml")
    header_patterns = ["one championship:", "road to one", "one on", "one fighting championship", "hero series", "warrior series", "friday fight"]
    pattern_match = lambda x: len([_ for _ in header_patterns if _ in x.lower()]) > 0

    event_headers = [header for header in soup.find_all(lambda tag: tag.name == "h2" and "cancelled" not in tag.text.lower()) if pattern_match(header.text)]

    if not event_headers:
        event_headers = soup.find_all(lambda tag: tag.name == "h1" and "one" in tag.text.lower() and "cancelled" not in tag.text.lower())
    
    table_classes = [_.get("class") for _ in event_headers[0].find_all_next("table")]

    table_class = "wikitable" if "toccolours" not in [x for xs in list(filter(lambda x: x!=None, table_classes)) for x in xs] else "toccolours"
    
    for event in event_headers:

        event_name = event.text.replace("[edit]", "")
        tables = event.find_next("table", class_ = table_class)
        if tables != None:
            df = pd.read_html(str(tables))
        else:
            df = pd.read_html(str(event.find_next("table", class_ = "wikitable")))
        
        df = df[0]
        df = cleanResults(result={"event": event_name, "df": df})
        dfs.append(df)

    return pd.concat(dfs, ignore_index=True)


def get_links_df(url):
    soup = BeautifulSoup(requests.get(url).text,"html.parser")
    
    table = soup.find_all("table", class_ = "sortable")[0]
    table_rows = table.find_all("tr")[1:]
    anchors = [_.find_all("a") for _ in table_rows if len(_.find_all("a"))>0]
    
    cols = ["link", "name"]
    links_df = pd.DataFrame([(anchor[0].get("href"), anchor[0].text) for anchor in anchors], columns=cols)
    
    return links_df


# optimize
def getData_bellator(url):
    dfs = []
    soup = BeautifulSoup(requests.get(url).text, features="lxml")
    header_patterns = ["bellator"]
    pattern_match = lambda x: len([_ for _ in header_patterns if _ in x.lower()]) > 0

    # event_headers = soup.find_all(lambda tag: tag.name == "h2" and "one" in tag.text.lower() and "cancelled" not in tag.text.lower())
    event_headers = [
        header for header in soup.find_all(
        lambda tag: 
        tag.name == "h2" and "cancelled" not in tag.text.lower() and "tournament" not in tag.text.lower()
        ) if pattern_match(header.text)
        ]

    if not event_headers:
        event_headers = soup.find_all(lambda tag: tag.name == "h1" and "bellator" in tag.text.lower() and "cancelled" not in tag.text.lower())
    
    table_classes = [_.get("class") for _ in event_headers[0].find_all_next("table")]

    table_class = "wikitable" if "toccolours" not in [x for xs in list(filter(lambda x: x!=None, table_classes)) for x in xs] else "toccolours"
    
    for event in event_headers:

        event_name = event.text.replace("[edit]", "")
        tables = event.find_next("table", class_ = table_class)
        if tables != None:
            df = pd.read_html(str(tables))
        else:
            df = pd.read_html(str(event.find_next("table", class_ = "wikitable")))
        
        df = df[0]
        df = cleanResults(result={"event": event_name, "df": df})
        df = df.assign(link = url)
        dfs.append(df)

    return pd.concat(dfs, ignore_index=True)


def getData_glory(url):
    dfs = []
    soup = BeautifulSoup(requests.get(url).text, features="lxml")
    header_patterns = ["glory"]
    pattern_match = lambda x: len([_ for _ in header_patterns if _ in x.lower()]) > 0

    event_headers = [
        header for header in soup.find_all(
        lambda tag: 
        tag.name == "h2" and "cancelled" not in tag.text.lower() and "tournament" not in tag.text.lower()
        and "awards" not in tag.text.lower()
        ) if pattern_match(header.text)
        ]

    if not event_headers:
        event_headers = soup.find_all(lambda tag: tag.name == "h1" and "glory" in tag.text.lower() and "cancelled" not in tag.text.lower())
    
    table_classes = [_.get("class") for _ in event_headers[0].find_all_next("table")]

    table_class = "wikitable" if "toccolours" not in [x for xs in list(filter(lambda x: x!=None, table_classes)) for x in xs] else "toccolours"
    
    for event in event_headers:

        event_name = event.text.replace("[edit]", "")
        tables = event.find_next("table", class_ = table_class)
        if tables != None:
            df = pd.read_html(str(tables))
        else:
            df = pd.read_html(str(event.find_next("table", class_ = "wikitable")))
        
        df = df[0]
        df = cleanResults(result={"event": event_name, "df": df})
        df = df.assign(link = url)
        dfs.append(df)

    return pd.concat(dfs, ignore_index=True)
