from .config_clean import CLEAN_LAYER_DEPENDENCIES, CLEAN_LAYER_TRANSFORMS
from pyspark.sql import SparkSession


spark = SparkSession.builder.master("local[*]").getOrCreate()


def generate_processed_datasets(dataset):
    event = CLEAN_LAYER_DEPENDENCIES[dataset]["event"]
    result = CLEAN_LAYER_DEPENDENCIES[dataset]["result"]
    output_path = CLEAN_LAYER_DEPENDENCIES[dataset]["output"]

    out_df = CLEAN_LAYER_TRANSFORMS[dataset](spark, event, result)
    
    out_df = out_df.toPandas()
    
    out_df.to_csv(output_path, mode="w", index=False)

    print(f"completed job for {dataset}")

    return None


TRANSFORMS = [generate_processed_datasets(dataset) for dataset in list(CLEAN_LAYER_DEPENDENCIES)]