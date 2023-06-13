import os
import json
import time
from .config_raw import SCRAPED_DATA_DEPENDENCIES, SCRAPED_DATA_TRANSFORMS
from .utils_raw import saveFile
init_time = time.time()


def generate_raw_files(data):
    input_dict = SCRAPED_DATA_DEPENDENCIES[data]
    filename = SCRAPED_DATA_DEPENDENCIES[data]["output"]
    fileformat = filename.split('.')[-1]

    print(f"Computing transformation for {data}\n")
    payload = SCRAPED_DATA_TRANSFORMS[data](**input_dict)

    print(f"Saving data for {data}\n")
    return saveFile(fileFormat=fileformat, filename=filename, file=payload)


TRANSFORMS = [generate_raw_files(data) for data in list(SCRAPED_DATA_DEPENDENCIES)]
print(f"BUILD FINISHED IN: {time.time() - init_time: .4f} seconds")