from .transforms_raw import(
    transform_ufcstats, transform_wiki_events_ufc,
    transform_wiki_events_onefc, transform_wiki_events_bellator,
    transform_wiki_events_glory, transform_fast_read, transform_wiki_results_onefc,
    transform_wiki_results_bellator,
    )


SCRAPED_DATA_DEPENDENCIES = {
    "ufcstats":
    {
        "url": "http://ufcstats.com/statistics/events/completed?page=all",
        "output": r"C:\Development\ultimateNakMuay\data\raw\ufcstats.json",
    },
    "wikievents_ufc":
    {
        "url": "https://en.wikipedia.org/wiki/List_of_UFC_events",
        "output": r"C:\Development\ultimateNakMuay\data\raw\wiki_events_ufc.csv",
    },
    "wikievents_onefc":
    {
        "url": "https://en.wikipedia.org/wiki/List_of_ONE_Championship_events#Events",
        "output": r"C:\Development\ultimateNakMuay\data\raw\wiki_events_onefc.csv",
        "table_key": 1,
    },
    "wikievents_bellator":
    {
        "url": "https://en.wikipedia.org/wiki/List_of_Bellator_MMA_events",
        "output": r"C:\Development\ultimateNakMuay\data\raw\wiki_events_bellator.csv",
        "table_key": 0,
    },
    "wikievents_glory":
    {
        "url": "https://en.wikipedia.org/wiki/Glory_(kickboxing)",
        "output": r"C:\Development\ultimateNakMuay\data\raw\wiki_events_glory.csv",
        "table_key": 1,
    },
    "wikievents_thai_fight":
    {
        "url": "https://en.wikipedia.org/wiki/Thai_Fight#Events",
        "output": r"C:\Development\ultimateNakMuay\data\raw\wiki_events_thai_fight.csv",
        "table_key": 2
    },
    "wikiresults_onefc":
    {
        "url": "https://en.wikipedia.org/wiki/List_of_ONE_Championship_events#Events",
        "output": r"C:\Development\ultimateNakMuay\data\raw\wiki_results_onefc.csv",
    },
    "wikiresults_bellator":
    {
        "url": "https://en.wikipedia.org/wiki/List_of_Bellator_MMA_events",
        "output": r"C:\Development\ultimateNakMuay\data\raw\wiki_results_bellator.csv",
    },
}

SCRAPED_DATA_TRANSFORMS = {
    "ufcstats": transform_ufcstats,
    "wikievents_ufc": transform_wiki_events_ufc,
    "wikievents_onefc": transform_wiki_events_onefc,
    "wikievents_bellator": transform_wiki_events_bellator,
    "wikievents_glory": transform_wiki_events_glory,
    "wikievents_thai_fight": transform_fast_read,
    "wikiresults_onefc": transform_wiki_results_onefc,
    "wikiresults_bellator": transform_wiki_results_bellator,
}

SCRAPED_DATA_TRANSFORMS = {
    **SCRAPED_DATA_TRANSFORMS, **{
        k: transform_fast_read for k in SCRAPED_DATA_TRANSFORMS.keys()
        if "table_key" in SCRAPED_DATA_DEPENDENCIES[k].keys()
        }
    }
