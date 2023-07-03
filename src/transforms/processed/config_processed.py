from .transforms_processed import (
    transform_ufc, transform_wiki_ufc, transform_wiki_fc_ufc, transform_wiki_events_bellator,
    transform_wiki_results_bellator, transform_wiki_events_onefc, transform_wiki_results_onefc,
    transform_wiki_events_glory, transform_wiki_results_glory, transform_wiki_events_thai_fight,
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
        "output": r"C:\Development\ultimateNakMuay\data\processed\wiki_results_ufc.csv",
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
    "wikievents_onefc":
    {
        "input": r"C:\Development\ultimateNakMuay\data\raw\wiki_events_onefc.csv",
        "output": r"C:\Development\ultimateNakMuay\data\processed\wiki_events_onefc.csv",
    },
    "wikifightcards_onefc":
    {
        "input": r"C:\Development\ultimateNakMuay\data\raw\wiki_results_onefc.csv",
        "output": r"C:\Development\ultimateNakMuay\data\processed\wiki_results_onefc.csv",
    },
    "wikievents_glory":
    {
        "input": r"C:\Development\ultimateNakMuay\data\raw\wiki_events_glory.csv",
        "output": r"C:\Development\ultimateNakMuay\data\processed\wiki_events_glory.csv",
    },
    "wikifightcards_glory":
    {
        "input": r"C:\Development\ultimateNakMuay\data\raw\wiki_results_glory.csv",
        "output": r"C:\Development\ultimateNakMuay\data\processed\wiki_results_glory.csv",
    },
    "wikievents_thai_fight":
    {
        "input": r"C:\Development\ultimateNakMuay\data\raw\wiki_events_thai_fight.csv",
        "output": r"C:\Development\ultimateNakMuay\data\processed\wiki_events_thai_fight.csv",
    },
}

PROCESSED_LAYER_TRANSFORMS = {
    "ufcstats": transform_ufc,
    "wikievents_ufc": transform_wiki_ufc,
    "wikifightcards_ufc": transform_wiki_fc_ufc,
    "wikievents_bellator": transform_wiki_events_bellator,
    "wikifightcards_bellator": transform_wiki_results_bellator,
    "wikievents_onefc": transform_wiki_events_onefc,
    "wikifightcards_onefc": transform_wiki_results_onefc,
    "wikievents_glory": transform_wiki_events_glory,
    "wikifightcards_glory": transform_wiki_results_glory,
    "wikievents_thai_fight": transform_wiki_events_thai_fight,
}

pyspark_jobs = [
    "wikievents_bellator", "wikifightcards_bellator", "wikievents_ufc", "wikifightcards_ufc",
    "wikievents_onefc", "wikifightcards_onefc", "wikievents_glory", "wikifightcards_glory",
    "wikievents_thai_fight",
]

for job in pyspark_jobs:
    PROCESSED_LAYER_DEPENDENCIES[job].update({"type": "pyspark"})