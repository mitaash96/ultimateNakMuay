from .transforms_clean import transform_ufc, transform_onefc



CLEAN_LAYER_DEPENDENCIES = {
    "ufc": {
        "event": r"C:\Development\ultimateNakMuay\data\processed\wiki_events_ufc.csv",
        "result": r"C:\Development\ultimateNakMuay\data\processed\wiki_results_ufc.csv",
        "output": r"C:\Development\ultimateNakMuay\data\clean\ufc.csv",
    },
    "onefc": {
        "event": r"C:\Development\ultimateNakMuay\data\processed\wiki_events_onefc.csv",
        "result": r"C:\Development\ultimateNakMuay\data\processed\wiki_results_onefc.csv",
        "output": r"C:\Development\ultimateNakMuay\data\clean\onefc.csv",
    },
}

CLEAN_LAYER_TRANSFORMS = {
    "ufc": transform_ufc,
    "onefc": transform_onefc,
}