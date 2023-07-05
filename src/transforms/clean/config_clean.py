from .transforms_clean import transform_ufc, transform_onefc, transform_bellator, transform_glory


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
    "bellator": {
        "event": r"C:\Development\ultimateNakMuay\data\processed\wiki_events_bellator.csv",
        "result": r"C:\Development\ultimateNakMuay\data\processed\wiki_results_bellator.csv",
        "output": r"C:\Development\ultimateNakMuay\data\clean\bellator.csv",
    },
    "glory": {
        "event": r"C:\Development\ultimateNakMuay\data\processed\wiki_events_glory.csv",
        "result": r"C:\Development\ultimateNakMuay\data\processed\wiki_results_glory.csv",
        "output": r"C:\Development\ultimateNakMuay\data\clean\glory.csv",
    },
}

CLEAN_LAYER_TRANSFORMS = {
    "ufc": transform_ufc,
    "onefc": transform_onefc,
    "bellator": transform_bellator,
    "glory": transform_glory,
}