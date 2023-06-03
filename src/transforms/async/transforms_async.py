import pandas as pd
import numpy as np
import asyncio
import pickle
import urllib


async def transform_wiki_ufc_fight_results(**kwargs):
    input_path = kwargs["input"]
    output_path = kwargs["output"]

    input_df = pd.read_csv(input_path)
    event_list = input_df[~input_df["#"].isna()]["Event"].to_list()
    dfs = []
    failed_events = []


    async def getResultsTable(event):
        getUrl = lambda x: "https://en.wikipedia.org/wiki/{}".format(x.replace(' ', '_'))
        event_url = getUrl(event)
        
        async def pack(url):
            try:
                encoded_url = urllib.parse.quote(url, safe=':/')
                df = await asyncio.to_thread(pd.read_html, encoded_url, attrs={"class": "toccolours"})
                dfs.append({"event": event, "url": url, "df": df[0]})
            except:
                url = url.rsplit(':', maxsplit=1)
                if len(url)>1:
                    return await pack(url[0])
                else:
                    failed_events.append({"event": event, "url": event_url, "final_url": url})
        
        return await pack(event_url)

    tasks = [asyncio.create_task(getResultsTable(_)) for _ in event_list]

    await asyncio.gather(*tasks)

    pickle_file = output_path
    failed_events_path = r"C:\Development\ultimateNakMuay\data\raw\fighter_ufc_payload_failed.pkl"

    print("Saving to Pickle")

    with open(pickle_file, "wb") as f:
        pickle.dump(dfs, f)
    
    with open(failed_events_path, "wb") as f:
        pickle.dump(failed_events, f)
