from .config_processed import PROCESSED_LAYER_DEPENDENCIES, PROCESSED_LAYER_TRANSFORMS


def generate_processed_datasets(dataset):
    input_path = PROCESSED_LAYER_DEPENDENCIES[dataset]["input"]
    output_path = PROCESSED_LAYER_DEPENDENCIES[dataset]["output"]

    out_df = PROCESSED_LAYER_TRANSFORMS[dataset](input_path)

    print(out_df.head(5))

    out_df.to_csv(output_path, mode="w", index=False)

    print(f"completed job for {dataset}")

    return None


TRANSFORMS = [generate_processed_datasets(dataset) for dataset in list(PROCESSED_LAYER_DEPENDENCIES)]