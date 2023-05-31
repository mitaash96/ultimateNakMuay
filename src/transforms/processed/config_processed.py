from .transforms_processed import (
    transform_ufc, transform_wiki_ufc
)


PROCESSED_LAYER_DEPENDENCIES = {
    "ufcstats": {
        "input": r"C:\Development\ultimateNakMuay\data\raw\ufcstats.json",
        "output": r"C:\Development\ultimateNakMuay\data\processed\ufcstats_events.csv",
    },
    "wikievents_ufc":
    {
        "input": r"C:\Development\ultimateNakMuay\data\raw\wiki_events_ufc.csv",
        "output": r"C:\Development\ultimateNakMuay\data\processed\wiki_events_ufc.csv",
    },
}

PROCESSED_LAYER_TRANSFORMS = {
    "ufcstats": transform_ufc,
    "wikievents_ufc": transform_wiki_ufc,
}