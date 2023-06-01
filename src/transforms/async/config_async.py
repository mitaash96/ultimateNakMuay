from .transforms_async import transform_wiki_ufc_fight_results


ASYNC_PAYLOAD_DEPENDENCIES = {
    "wiki_ufc_fight_results": {
       "input": r"C:\Development\ultimateNakMuay\data\raw\wiki_events_ufc.csv",
       "output": r"C:\Development\ultimateNakMuay\notebooks\pickle_objects\fighter_ufc_payload_async.pkl",
    }
}

ASYNC_TRANSFORMS = {
    "wiki_ufc_fight_results": transform_wiki_ufc_fight_results,
}