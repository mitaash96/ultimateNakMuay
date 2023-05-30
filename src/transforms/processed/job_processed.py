from .config_processed import CLEAN_LAYER_DEPENDENCIES, CLEAN_LAYER_TRANSFORMS
import pandas as pd


def generate_clean_datasets(dataset):
    input_path = CLEAN_LAYER_DEPENDENCIES[dataset]["input"]
    output_path = CLEAN_LAYER_DEPENDENCIES[dataset]["output"]

    out_df = CLEAN_LAYER_TRANSFORMS[dataset](input_path)

    print(out_df.head(5))

    out_df.to_csv(output_path, mode="w", index=False)

    print(f"completed job for {dataset}")


TRANSFORMS = [generate_clean_datasets(dataset) for dataset in list(CLEAN_LAYER_DEPENDENCIES)]