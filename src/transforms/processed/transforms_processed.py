import pandas as pd
import numpy as np
import hashlib
import pickle
from pyspark.sql import functions as F
from pyspark.sql import types as T
from .utils_processed import(
    remove_poisoned_rows, add_location_cols, calculate_time_parts
    )


def transform_ufc(input_path):
    df = pd.read_json(input_path)

    df["event_date"] = df["event_date"].apply(lambda x: pd.to_datetime(x))
    df[["event_title", "fighter1", "fighter2"]] = df['event_name'].str.split(':| vs\. ', expand=True)
    df[["city", "state", "country"]] = df['event_locations'].str.split(", ", expand = True)
    df.drop(columns=["event_locations", "event_name"], axis=1, inplace= True)

    return df


def transform_wiki_ufc_prelim(input_path):
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


def transform_wiki_fc_ufc_prelim(input_path):
    with open(input_path, "rb") as f:
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


def transform_wiki_events_bellator(spark, input_path):
    events = spark.read.csv(input_path, header=True)
    
    for col in events.columns:
        events = events.withColumnRenamed(col, col.lower())
    
    events = events\
        .withColumn("date", F.to_date(F.col("date"), "MMMM d, yyyy"))\
        .withColumn("attendance", F.col("attendance").cast(T.IntegerType()))

    events = add_location_cols(events)

    return events


def transform_wiki_results_bellator(spark, input_path):
    results = spark.read.csv(input_path, header=True)
    
    results = remove_poisoned_rows(results)
    
    results = calculate_time_parts(results)
    
    results = results.withColumn("time", F.col("time").cast(T.DoubleType()))\
        .withColumn("round", F.col("round").cast(T.IntegerType()))
    
    results = results.drop("notes")

    return results


def transform_wiki_ufc(spark, input_path):
    events = spark.createDataFrame(transform_wiki_ufc_prelim(input_path))
    
    events = events.withColumn("date", F.to_date(F.col("date")))\
        .withColumn("attendance", F.regexp_replace(F.col("attendance"), ",", ""))\
        .withColumn("attendance", F.col("attendance").cast(T.IntegerType()))\
        .withColumn("location", F.when(F.col("location") == "—", F.lit(None)).otherwise(F.col("location")))
    
    events = events.withColumn(
        "location", F.when(
            F.col("location").endswith("U.S"), F.regexp_replace(F.col("location"), "U.S", "U.S.")
            ).otherwise(F.col("location"))
            )
    
    venue_map = events.filter(F.col("location").isNotNull())\
        .select("venue", "location").distinct()
    
    venue_map = venue_map.withColumn("venue", F.when(
        (F.col("location") == "Hidalgo, Texas, U.S.") & (F.col("venue") == "State Farm Arena"),
        F.lit("Payne Arena")).otherwise(F.col("venue")))\
        .withColumnRenamed("location", "location_filled")
    
    events = events.join(venue_map, on=["venue"], how="left")\
        .withColumn("location", F.col("location_filled"))\
        .drop("location_filled")

    events = add_location_cols(events)
    cols = ["event_num", "event", "date", "venue", "city", "state", "country", "attendance", "event_id"]

    events = events.select(*cols)

    return events


def transform_wiki_fc_ufc(spark, input_path):
    results = spark.createDataFrame(transform_wiki_fc_ufc_prelim(input_path))
    
    results = remove_poisoned_rows(results)

    results = calculate_time_parts(results)

    results = results.withColumn("time", F.col("time").cast(T.DoubleType()))\
        .withColumn("round", F.col("round").cast(T.IntegerType()))\
        .drop("notes")
    
    return results


def transform_wiki_events_onefc(spark, input_path):
    events = spark.read.csv(input_path, header=True)
    
    events = events.withColumn(
        "date",
        F.when(
            F.to_date(F.col("date"), "MMMM d, yyyy").isNotNull(), F.to_date(F.col("date"), "MMMM d, yyyy")
            ).otherwise(F.to_date(F.col("date"), "d MMMM yyyy"))
            )\
        .withColumn("attendance", F.regexp_replace(F.col("attendance"), ",", ""))\
        .withColumn("attendance", F.col("attendance").cast(T.IntegerType()))\
        .withColumn("location", F.when(F.col("location") == "—", F.lit(None)).otherwise(F.col("location")))\
        .withColumn("venue", F.when(F.col("venue") == "—", F.lit(None)).otherwise(F.col("venue")))
    
    events = events.withColumnRenamed("#", "event_num")
    for _ in events.columns:
        events = events.withColumnRenamed(_, _.lower())
    
    events = add_location_cols(events)

    cols = ["event_num", "event", "date", "venue", "city", "state", "country", "attendance"]

    events = events.select(*cols)

    return events


def transform_wiki_results_onefc(spark, input_path):
    results = spark.read.csv(input_path, header=True)
    
    if "event" in results.columns:
        results = results.withColumn("event_name", F.when(
            F.col("event").isNotNull(), F.col("event")
        ).otherwise(F.col("event_name"))
        ).drop("event")

    results = remove_poisoned_rows(results)

    results = calculate_time_parts(results)

    results = results.withColumn("time", F.col("time").cast(T.DoubleType()))\
        .withColumn("round", F.col("round").cast(T.IntegerType()))
    
    results = results.drop(*[col for col in results.columns if "unnamed" in col.lower()])

    return results


def transform_wiki_events_glory(spark, input_path):
    return transform_wiki_events_onefc(spark, input_path)


def transform_wiki_results_glory(spark, input_path):
    results = spark.read.csv(input_path, header=True)

    results = results.withColumn("weight_class", F.coalesce("weight_class", "weight")).drop("weight")

    results = remove_poisoned_rows(results)

    results = calculate_time_parts(results)

    results = results.withColumn("time", F.col("time").cast(T.DoubleType()))\
        .withColumn("round", F.col("round").cast(T.IntegerType()))
    
    return results


def transform_wiki_events_thai_fight(spark, input_path):
    events = spark.read.csv(input_path, header=True)

    events = events.withColumnRenamed("City", "location")

    events = events.withColumn("date", F.to_date(F.col("date"), "MMMM d, yyyy"))\
        .withColumn("location", F.when(F.col("location") == "—", F.lit(None)).otherwise(F.col("location")))
    
    events = add_location_cols(events)

    events = events.withColumnRenamed("#", "event_num")
    for _ in events.columns:
        events = events.withColumnRenamed(_, _.lower())

    cols = ["event_num", "event", "date", "venue", "city", "state", "country"]

    events = events.select(*cols)

    return events
