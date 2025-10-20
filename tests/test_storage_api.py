import asyncio
import os

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
    with api.storage as client:
        buckets = client.buckets.list()
        print(buckets)


async def async_test():
    async with api.storage as client:
        buckets = await client.buckets.list()
        print(buckets)


test()
asyncio.run(async_test())
