"""
Tests for asset-related types and functionalities.
"""

import pytest

from ovalbee.api.api import Api
from ovalbee.domain.types.asset import AssetInfo, AssetType
from ovalbee.domain.types.file import FileInfo, FileType


@pytest.fixture
def asset_info():
    file1 = FileInfo(key="image1.png", url="http://example.com/image1.png", type=FileType.INTERNAL)
    file2 = FileInfo(key="image2.png", url="http://example.com/image2.png", type=FileType.INTERNAL)
    return AssetInfo(space_id=1, name="new.jpg", type=AssetType.IMAGES, resources=[file1, file2])


api = Api(
    server_address="http://0.0.0.0:30080",
    token="bzzz_admin_api_token_$MTpnbzkyUllVZmFBWTVLUmJ4cUo5clVIRVh1a1ppMlJ4Rg",
)


def test_list_assets():
    assets = api.asset.get_list(space_id=1)
    assert isinstance(assets, list)
    assert all(isinstance(asset, AssetInfo) for asset in assets)


def test_create_asset(asset_info):
    created_asset = api.asset.create(asset_info)
    assert isinstance(created_asset, AssetInfo)
    assert isinstance(created_asset.id, str)
    assert created_asset.type == AssetType.IMAGES
    assert len(created_asset.resources) == 2
    assert created_asset.space_id == 1


if __name__ == "__main__":
    pytest.main()
    # test_list_assets()
    # test_create_asset(asset_info=asset_info)
