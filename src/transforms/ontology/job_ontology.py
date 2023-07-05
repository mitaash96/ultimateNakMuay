from .config_ontology import ONTOLOGY_DEPENDENCIES, ONTOLOGY_TRANSFORMS
from pyspark.sql import SparkSession


spark = SparkSession.builder.master("local[*]").getOrCreate()


def generate_processed_datasets(dataset):
    inputs = ONTOLOGY_DEPENDENCIES[dataset]["inputs"]
    output_path = ONTOLOGY_DEPENDENCIES[dataset]["output"]

    out_df = ONTOLOGY_TRANSFORMS[dataset](spark, **inputs)
    
    out_df = out_df.toPandas()
    
    out_df.to_csv(output_path, mode="w", index=False)

    print(f"completed job for {dataset}")

    return None


TRANSFORMS = [generate_processed_datasets(dataset) for dataset in list(ONTOLOGY_DEPENDENCIES)]