from .transforms_ontology import transform_combat_sports_events


organizations = ["ufc", "onefc", "glory", "bellator"]

ONTOLOGY_DEPENDENCIES = {
    "combat_sport_events": {
        "inputs": {org: r"C:\Development\ultimateNakMuay\data\clean\{}.csv".format(org) for org in organizations},
        "output": r"C:\Development\ultimateNakMuay\data\ontology\combat_sport_events.csv"
        }
}

ONTOLOGY_TRANSFORMS = {
    "combat_sport_events": transform_combat_sports_events,
}