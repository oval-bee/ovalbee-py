"""
Tests for asset-related types and functionalities.
"""

import pytest

from ovalbee.api.api import Api
from ovalbee.domain.types.asset import AssetInfo, AssetType
from ovalbee.domain.types.file import FileInfo


@pytest.fixture
def asset_info():
    file1 = FileInfo(name="image1.png", url="http://example.com/image1.png", uploaded=True)
    file2 = FileInfo(name="image2.png", url="http://example.com/image2.png", uploaded=False)
    return AssetInfo(type=AssetType.IMAGES, resources=[file1, file2])


api = Api(
    server_address="http://0.0.0.0:30080",
    token="bzzz_admin_api_token_$MTpnbzkyUllVZmFBWTVLUmJ4cUo5clVIRVh1a1ppMlJ4Rg",
)


def test_list_assets():
    assets = api.asset.get_list(space_id=1)
    assert isinstance(assets, list)
    assert all(isinstance(asset, AssetInfo) for asset in assets)


def test_create_asset():
    asset_info = AssetInfo(workspace_id=1, type=AssetType.IMAGES)
    created_asset = api.asset.create(asset_info)
    assert isinstance(created_asset, AssetInfo)
    assert isinstance(created_asset.id, str)
    assert created_asset.type == AssetType.IMAGES
    assert created_asset.resources == []
    assert created_asset.workspace_id == 1


if __name__ == "__main__":
    pytest.main()
    # test_list_assets()
    # test_create_asset()
