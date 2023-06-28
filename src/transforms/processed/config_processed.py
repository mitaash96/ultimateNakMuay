from .transforms_processed import (
    transform_ufc, transform_wiki_ufc, transform_wiki_fc_ufc, transform_wiki_events_bellator,
    transform_wiki_results_bellator,
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
    "wikifightcards_ufc":
    {
        "input": r"C:\Development\ultimateNakMuay\data\raw\fighter_ufc_payload_async.pkl",
        "output": r"C:\Development\ultimateNakMuay\data\processed\wiki_fc_ufc.csv",
    },
    "wikievents_bellator":
    {
        "input": r"C:\Development\ultimateNakMuay\data\raw\wiki_events_bellator.csv",
        "output": r"C:\Development\ultimateNakMuay\data\processed\wiki_events_bellator.csv",
    },
    "wikifightcards_bellator":
    {
        "input": r"C:\Development\ultimateNakMuay\data\raw\wiki_results_bellator.csv",
        "output": r"C:\Development\ultimateNakMuay\data\processed\wiki_results_bellator.csv",
    },
}

PROCESSED_LAYER_TRANSFORMS = {
    "ufcstats": transform_ufc,
    "wikievents_ufc": transform_wiki_ufc,
    "wikifightcards_ufc": transform_wiki_fc_ufc,
    "wikievents_bellator": transform_wiki_events_bellator,
    "wikifightcards_bellator": transform_wiki_results_bellator,
}

pyspark_jobs = [
    "wikievents_bellator", "wikifightcards_bellator", "wikievents_ufc"
]

for job in pyspark_jobs:
    PROCESSED_LAYER_DEPENDENCIES[job].update({"type": "pyspark"})