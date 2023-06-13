import pandas as pd
import numpy as np
import hashlib
import pickle


def transform_ufc(input_path):
    df = pd.read_json(input_path)

    df["event_date"] = df["event_date"].apply(lambda x: pd.to_datetime(x))
    df[["event_title", "fighter1", "fighter2"]] = df['event_name'].str.split(':| vs\. ', expand=True)
    df[["city", "state", "country"]] = df['event_locations'].str.split(", ", expand = True)
    df.drop(columns=["event_locations", "event_name"], axis=1, inplace= True)

    return df


def transform_wiki_ufc(input_path):
    df = pd.read_csv(input_path)

    for i in df[df["Ref."].isna()].index:
        for j in  range(5, 1, -1):
            if not pd.isna(df.iloc[i,j]) and len(set(['[', ']']).intersection(list(df.iloc[i,j])))>0:
                df.iloc[i, 6] = df.iloc[i,j]
                df.iloc[i, j] = np.nan
    
    for i in df[df["Attendance"].isna()].index:
        for j in  range(4, 1, -1):
            if not pd.isna(df.iloc[i,j]) and df.iloc[i,j].replace(',','').isnumeric():
                df.iloc[i, 5] = df.iloc[i,j]
                df.iloc[i, j] = np.nan
    
    for i in df[~df["Date"].apply(lambda x: x[-4:].isnumeric())].index:
        df.loc[i,"Location"] = df.loc[i, "Venue"]
        df.loc[i,"Venue"] = df.loc[i, "Date"]
        df.loc[i,"Date"] = np.nan
    
    cols2ffill = ["Date", "Venue", "Location", "Attendance"]
    for col in cols2ffill:
        df[col] = df[col].fillna(method="ffill")
    
    df['#'] = df['#'].apply(lambda x: "future" if pd.isna(x) else x if x.isnumeric() else "cancelled")
    df["Attendance"] = df.apply(lambda row: np.nan if row['#'] == 'future' or row["Attendance"] == '—' else row['Attendance'], axis=1)
    df["Date"] = df["Date"].apply(lambda x: pd.to_datetime(x))

    df.drop(columns=["Ref."], axis=1, inplace=True)

    cols2rename = {
    "#": "event_num",
    }

    cols2rename = {
        **{
            x: x.lower() for x in df.columns
        },
        **cols2rename
    }

    df.rename(columns=cols2rename, inplace= True)

    df["event_id"] = df["event"].apply(lambda x: hashlib.sha256(x.encode()).hexdigest())

    return df


def transform_wiki_fc_ufc(input_path):
    with open(r"C:\Development\ultimateNakMuay\data\raw\fighter_ufc_payload_async.pkl", "rb") as f:
        results = pickle.load(f)
    
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

        df.loc[:, "event_id"] = df["event_name"].apply(lambda x: hashlib.sha256(x.encode()).hexdigest())

        return df
    
    results = list(map(cleanResults, results))

    final_df = pd.concat(results, ignore_index=True)
    final_df.drop(columns=["unnamed:_8_level_1", "event"], inplace=True)

    return final_df
