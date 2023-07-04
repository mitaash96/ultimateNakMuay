from pyspark.sql import functions as F
from pyspark.sql import types as T


def transform_ufc(spark, event, result):
    event = spark.read.csv(event, header=True)
    result = spark.read.csv(result, header=True)
    
    df = event.join(result, on="event_id", how="inner")\
        .drop("event_id", "event_name")\
        .orderBy(F.col("date").desc())
    
    return df