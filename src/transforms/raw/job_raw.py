import os
import json
from .config_raw import SCRAPED_DATA_DEPENDENCIES, SCRAPED_DATA_TRANSFORMS
from .utils_raw import saveFile


def generate_json_files(data):
    input_dict = SCRAPED_DATA_DEPENDENCIES[data]
    filename = SCRAPED_DATA_DEPENDENCIES[data]["output"]
    fileformat = filename.split('.')[-1]

    print(f"Computing transformation for {data}\n")
    payload = SCRAPED_DATA_TRANSFORMS[data](**input_dict)

    print(f"Saving data for {data}\n")
    return saveFile(fileFormat=fileformat, filename=filename, file=payload)


TRANSFORMS = [generate_json_files(data) for data in list(SCRAPED_DATA_DEPENDENCIES)]