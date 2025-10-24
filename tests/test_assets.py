"""
Tests for asset-related types and functionalities.
"""

import pytest

from ovalbee.domain.types.asset import AssetInfo, AssetType
from ovalbee.domain.types.file import FileInfo, FileType


@pytest.fixture
def asset_info():
    file1 = FileInfo(key="image1.png")
    file2 = FileInfo(key="image2.png")
    return AssetInfo(space_id=1, type=AssetType.IMAGES, resources=[file1, file2])


def test_asset_info_creation(asset_info):
    assert asset_info.type == AssetType.IMAGES
    assert len(asset_info.resources) == 2
    assert asset_info.resources[0].key == "image1.png"


def test_asset_info_empty_resources():
    asset = AssetInfo(space_id=1, type=AssetType.VIDEOS)
    assert asset.type == AssetType.VIDEOS
    assert len(asset.resources) == 0


def test_file_info_optional_fields():
    file_info = FileInfo(key="video1.mp4")
    assert file_info.key == "video1.mp4"
    assert file_info.url is None
    assert file_info.type == FileType.INTERNAL


def test_asset_info_with_no_resources():
    asset = AssetInfo(space_id=1, type=AssetType.IMAGES)
    assert asset.type == AssetType.IMAGES
    assert asset.resources == []


if __name__ == "__main__":
    pytest.main()
