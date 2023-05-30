import os
import json
import pandas as pd


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