import pandas as pd
import numpy as np
import asyncio
import pickle


async def transform_wiki_ufc_fight_results(**kwargs):
    input_path = kwargs["input"]
    output_path = kwargs["output"]

    input_df = pd.read_csv(input_path)
    event_list = input_df[~input_df["#"].isna()]["Event"].to_list()
    dfs = []


    async def getResultsTable(event):
        getUrl = lambda x: "https://en.wikipedia.org/wiki/{}".format(x.replace(' ', '_'))
        event_url = getUrl(event)
        print(f"Fetching table for {event_url}")
        
        async def pack(url):
            try:
                df = await asyncio.to_thread(pd.read_html, url, attrs={"class": "toccolours"})
                dfs.append({event: df})
            except:
                url = url.rsplit(':', maxsplit=1)
                if len(url)>1:
                    return await pack(url[0])
        
        return await pack(event_url)

    tasks = [asyncio.create_task(getResultsTable(_)) for _ in event_list]

    await asyncio.gather(*tasks)

    pickle_file = output_path

    print("Saving to Pickle")

    with open(pickle_file, "wb") as f:
        pickle.dump(dfs, f)
