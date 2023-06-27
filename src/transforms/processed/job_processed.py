from .config_processed import PROCESSED_LAYER_DEPENDENCIES, PROCESSED_LAYER_TRANSFORMS
from pyspark.sql import SparkSession
import os


spark = SparkSession.builder.master("local[*]").getOrCreate()


def generate_processed_datasets(dataset):
    input_path = PROCESSED_LAYER_DEPENDENCIES[dataset]["input"]
    output_path = PROCESSED_LAYER_DEPENDENCIES[dataset]["output"]

    if "type" not in list(PROCESSED_LAYER_DEPENDENCIES[dataset]):
        out_df = PROCESSED_LAYER_TRANSFORMS[dataset](input_path)
    else:
        out_df = PROCESSED_LAYER_TRANSFORMS[dataset](spark, input_path)
        out_df = out_df.toPandas()
    
    out_df.to_csv(output_path, mode="w", index=False)

    print(f"completed job for {dataset}")

    return None


TRANSFORMS = [generate_processed_datasets(dataset) for dataset in list(PROCESSED_LAYER_DEPENDENCIES)]