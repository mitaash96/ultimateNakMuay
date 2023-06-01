import os
import json
import asyncio
import time
from .config_raw import SCRAPED_DATA_DEPENDENCIES, SCRAPED_DATA_TRANSFORMS
from .utils_raw import saveFile
init_time = time.time()


async def generate_json_files(data):
    input_dict = SCRAPED_DATA_DEPENDENCIES[data]
    filename = SCRAPED_DATA_DEPENDENCIES[data]["output"]
    fileformat = filename.split('.')[-1]

    print(f"Computing transformation for {data}\n")
    payload = SCRAPED_DATA_TRANSFORMS[data](**input_dict)

    print(f"Saving data for {data}\n")
    return saveFile(fileFormat=fileformat, filename=filename, file=payload)


async def main():
    print("running async main")
    TRANSFORMS = [
        asyncio.create_task(generate_json_files(data)) for data in list(SCRAPED_DATA_DEPENDENCIES)
        ]
    await asyncio.gather(*TRANSFORMS)


asyncio.run(main())
print(f"BUILD FINISHED IN: {time.time() - init_time: .4f} seconds")