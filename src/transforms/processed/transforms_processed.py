import pandas as pd


def transform_ufc(input_path):
    df = pd.read_json(input_path)

    df["event_date"] = df["event_date"].apply(lambda x: pd.to_datetime(x))
    df[["event_title", "fighter1", "fighter2"]] = df['event_name'].str.split(':| vs\. ', expand=True)
    df[["city", "state", "country"]] = df['event_locations'].str.split(", ", expand = True)
    df.drop(columns=["event_locations", "event_name"], axis=1, inplace= True)

    return df