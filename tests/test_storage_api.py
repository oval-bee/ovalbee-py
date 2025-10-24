import asyncio
import os
import time

from dotenv import load_dotenv

from ovalbee.api.api import Api

api = Api(
    server_address="http://0.0.0.0:30080",
    token="bzzz_admin_api_token_$MTpnbzkyUllVZmFBWTVLUmJ4cUo5clVIRVh1a1ppMlJ4Rg",
)


BUCKET = "test-bucket"
PREFIX = "tests/1760496115"
LOCAL_DIR = "./downloaded_dir"


def test():
    t = time.perf_counter()
    for i in range(3):
        for f in api.storage.list_objects(BUCKET, PREFIX):
            bytes = api.storage.objects.get_size(BUCKET, f.key)
            print(bytes)
    print(f"Test duration: {time.perf_counter() - t:.2f} seconds")


async def async_test():
    t = time.perf_counter()
    for i in range(3):
        for f in await api.storage.list_objects(BUCKET, PREFIX):
            bytes = await api.storage.objects.get_size(BUCKET, f.key)
            print(bytes)
    print(f"Test duration: {time.perf_counter() - t:.2f} seconds")


if __name__ == "__main__":
    test()
    asyncio.run(async_test())
