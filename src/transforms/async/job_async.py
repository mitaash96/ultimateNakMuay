import asyncio
import time
from .config_async import ASYNC_PAYLOAD_DEPENDENCIES, ASYNC_TRANSFORMS
init_time = time.time()


def generatePayload(data):
    input_dict = ASYNC_PAYLOAD_DEPENDENCIES[data]

    print(f"Computing transformation for {data}\n")
    asyncio.run(ASYNC_TRANSFORMS[data](**input_dict))
    
    print(f"Saved data for {data}\n")
    return None


TRANSFORMS = [generatePayload(data) for data in list(ASYNC_PAYLOAD_DEPENDENCIES)]
print(f"BUILD FINISHED IN: {time.time() - init_time: .4f} seconds")