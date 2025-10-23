"""
Tests for collection-related types and functionalities.
"""

import pytest

from ovalbee.domain.types.asset import AssetInfo, AssetType
from ovalbee.domain.types.collection import CollectionInfo
from ovalbee.domain.types.file import FileInfo, FileType


@pytest.fixture
def asset_1():
    file1 = FileInfo(key="image1.png", url="http://example.com/image1.png", type=FileType.INTERNAL)
    return AssetInfo(workspace_id=1, name="new1.jpg", type=AssetType.IMAGES, resources=[file1])


@pytest.fixture
def asset_2():
    file2 = FileInfo(key="image2.png", url="http://example.com/image2.png", type=FileType.INTERNAL)
    return AssetInfo(workspace_id=1, name="new2.jpg", type=AssetType.IMAGES, resources=[file2])


@pytest.fixture
def collection_1(asset_1):
    return CollectionInfo(name="My Collection", workspace_id=1, assets=[asset_1])


@pytest.fixture
def collection_2(asset_2):
    return CollectionInfo(name="My Collection", workspace_id=1, assets=[asset_2])


def test_collection_name(collection_1, collection_2):
    assert collection_1.name == collection_2.name
    assert collection_1.workspace_id == collection_2.workspace_id
    assert len(collection_1.assets) == 1
    assert len(collection_2.assets) == 1


if __name__ == "__main__":
    pytest.main()
