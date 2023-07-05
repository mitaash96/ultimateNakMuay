from pyspark.sql import functions as F
from pyspark.sql import types as T
from .utils_clean import roman_to_int, event_name_correction, drop_join_cols


def transform_ufc(spark, event, result):
    event = spark.read.csv(event, header=True)
    result = spark.read.csv(result, header=True)
    
    df = event.join(result, on="event_id", how="inner")\
        .drop("event_id", "event_name")\
        .orderBy(F.col("date").desc())
    
    return df


def transform_onefc(spark, event, result):
    event = spark.read.csv(event, header=True)
    result = spark.read.csv(result, header=True)

    manual_event_name_corrections = [
        ("2011 in ONE Championship", "ONE Fighting Championship 1: Champion vs. Champion"),
        ("Road to ONE Championship: Night of Warriors 17","Road to ONE 8: Night of Warriors"),    
    ]

    event = event.withColumn("ejc1", F.element_at(F.split(F.col("event"), ":"), 1))\
        .withColumn("event", F.regexp_replace(F.col("event"), "ONE:", "ONE Championship:"))\
        .withColumn("ejc4", F.udf(lambda x: roman_to_int(x), T.StringType())(F.col("event")))\
        .withColumn("ejc2", F.lower(F.trim(F.element_at(F.split(F.col("event"), ":"), -1))))\
        .withColumn(
            "ejc3", F.when(
                F.col("event").contains("Hero Series"),
                F.concat(
                    F.regexp_extract(F.col("event"), r"^(ONE Hero Series)", 1),
                    F.lit(" "),
                    F.date_format(F.col("date"), "MMMM")
                    )
                ).otherwise(F.lit(None))
            )
    
    result = result.withColumn("event_name", F.regexp_replace(F.col("event_name"), "Road to ONE:", "Road to ONE Championship:"))\
        .withColumn("rjc1", F.lower(F.trim(F.element_at(F.split(F.col("event_name"), ":"), -1))))
    
    result = event_name_correction(manual_event_name_corrections, result)

    join_conditions = (
        (event.event == result.event_name)
        |(event.ejc1 == result.event_name)
        |(event.ejc2 == result.rjc1)
        |(event.ejc4 == result.event_name)
        )
    
    df = event.join(result, on=join_conditions, how="left")

    hsdf = df.filter(F.col("ejc3").isNotNull() & F.col("event_name").isNull())
    df = df.subtract(hsdf)
    hsdf = hsdf.select(*event.columns)

    hsdf = hsdf.join(result, hsdf.ejc3 == result.event_name, "left")

    df = df.unionByName(hsdf).withColumn("event_num", F.regexp_replace(F.col("event_num"), "–", "cancelled"))

    df = drop_join_cols(df)

    df = df.orderBy(F.col("date").desc())

    return df
