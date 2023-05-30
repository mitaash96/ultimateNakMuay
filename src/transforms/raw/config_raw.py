from .transforms_raw import(
    transform_ufcstats, transform_wiki_events_ufc,
    transform_wiki_events_onefc, transform_wiki_events_bellator,
    transform_wiki_events_glory,
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
    },
    "wikievents_bellator":
    {
        "url": "https://en.wikipedia.org/wiki/List_of_Bellator_MMA_events",
        "output": r"C:\Development\ultimateNakMuay\data\raw\wiki_events_bellator.csv",
    },
    "wikievents_glory":
    {
        "url": "https://en.wikipedia.org/wiki/Glory_(kickboxing)",
        "output": r"C:\Development\ultimateNakMuay\data\raw\wiki_events_glory.csv",
    },
}

SCRAPED_DATA_TRANSFORMS = {
    "ufcstats": transform_ufcstats,
    "wikievents_ufc": transform_wiki_events_ufc,
    "wikievents_onefc": transform_wiki_events_onefc,
    "wikievents_bellator": transform_wiki_events_bellator,
    "wikievents_glory": transform_wiki_events_glory,
}
