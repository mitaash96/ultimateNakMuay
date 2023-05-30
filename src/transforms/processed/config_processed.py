from .transforms_processed import transform_ufc


CLEAN_LAYER_DEPENDENCIES = {
    "ufcstats": {
        "input": r"C:\Development\ultimateNakMuay\data\raw\ufcstats.json",
        "output": r"C:\Development\ultimateNakMuay\data\processed\ufcstats_events.csv",
    }
}

CLEAN_LAYER_TRANSFORMS = {
    "ufcstats": transform_ufc,
}