"""
Tests for collection-related types and functionalities.
"""

import pytest

from ovalbee.domain.types.asset import AssetInfo, AssetType
from ovalbee.domain.types.collection import CollectionInfo
from ovalbee.domain.types.file import FileInfo, FileType


@pytest.fixture
def collection_info():
    file1 = FileInfo(name="doc1.pdf")
    file2 = FileInfo(name="doc2.pdf")
    asset1 = AssetInfo(workspace_id=1, type=AssetType.IMAGES, resources=[file1])
    asset2 = AssetInfo(workspace_id=1, type=AssetType.IMAGES, resources=[file2])
    return CollectionInfo(workspace_id=1, name="My Collection", assets=[asset1, asset2])


def test_collection_name(collection_info):
    assert collection_info.name == "My Collection"


if __name__ == "__main__":
    pytest.main()
