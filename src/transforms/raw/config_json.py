from .transforms_json import transform_ufcstats


SCRAPED_DATA_DEPENDENCIES = {
    "ufcstats":
    {
        "url": "http://ufcstats.com/statistics/events/completed?page=all",
        "output": r"C:\Development\ultimateNakMuay\data\raw\ufcstats.json",
    },
}

SCRAPED_DATA_TRANSFORMS = {
    "ufcstats": transform_ufcstats
}