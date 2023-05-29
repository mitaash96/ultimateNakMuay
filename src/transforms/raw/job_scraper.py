import os
import json
from .config_json import SCRAPED_DATA_DEPENDENCIES, SCRAPED_DATA_TRANSFORMS


def generate_json_files(data):
    input_dict = SCRAPED_DATA_DEPENDENCIES[data]
    filename = SCRAPED_DATA_DEPENDENCIES[data]["output"]
    payload = SCRAPED_DATA_TRANSFORMS[data](**input_dict)
    
    if os.path.exists(filename):
        os.remove(filename)

    with open(filename, 'w') as json_file:
        json.dump(payload, json_file, indent=4)

    print(f"Saved the list of events for {data}")


TRANSFORMS = [generate_json_files(data) for data in list(SCRAPED_DATA_DEPENDENCIES)]