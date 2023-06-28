from pyspark.sql import functions as F
from pyspark.sql import types as T


def remove_poisoned_rows(df):
    hash_rows = lambda col_list: F.sha2(F.concat_ws("|", *col_list), 256)
    test_cols = [F.col(_) for _ in df.columns[:7]]

    incorrect_data = df.filter(
        ~(F.col("time").contains(":") | F.col("time").contains("."))
        ).select(*test_cols)\
        .distinct()
    
    incorrect_data = incorrect_data.withColumn("poison", hash_rows(incorrect_data.columns))

    df = df.withColumn("poison", hash_rows(test_cols))\
        .join(incorrect_data, ["poison"], how="left_anti")\
        .drop("poison")
    
    return df


def add_location_cols(df):
    
    df = df.withColumn("location", F.split(F.col("location"), ","))\
        .withColumn("city", F.when(F.size(F.col("location")) == 3, F.element_at(F.col("location"), 1)))\
        .withColumn("state", F.when(
            F.size(F.col("location")) == 3, F.element_at(F.col("location"), 2)
            ).otherwise(F.element_at(F.col("location"), 1))
            )\
        .withColumn("country", F.element_at(F.col("location"), -1))\
        .withColumn("country", F.regexp_replace(F.col("country"), "[^a-zA-Z0-9 ]", ""))\
        .drop("location")
    
    return df


def calculate_time_parts(df):
    df = df.withColumn("time", F.regexp_replace(F.col("time"), "\\.", ":"))\
        .withColumn("time_parts", F.split(F.col("time"), ":"))\
        .withColumn("time", F.element_at(F.col("time_parts"), 1)*60 + F.element_at(F.col("time_parts"), 2))\
        .drop("time_parts")

    return df