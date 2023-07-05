from pyspark.sql import functions as F
from pyspark.sql import types as T
from .utils_ontology import union_datasets


def transform_combat_sports_events(spark, **kwargs):
    
    to_df = lambda x: spark.read.csv(kwargs[x], header=True).withColumn("organization", F.lit(x))
    
    ufc = to_df("ufc")
    onefc = to_df("onefc")
    glory = to_df("glory")
    bellator = to_df("bellator")

    df = union_datasets([ufc, onefc, glory, bellator]).orderBy(F.col("date").desc())

    return df
