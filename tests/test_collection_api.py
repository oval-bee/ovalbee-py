"""
Tests for CollectionApi-related functionalities.
"""

import pytest

from ovalbee.api.api import Api
from ovalbee.dto.asset import AssetInfo, AssetType
from ovalbee.dto.collection import CollectionInfo
from ovalbee.dto.file import FileInfo, FileType

SPACE_ID = 1
NEW_COLLECTION_NAME = "temp_test_collection"


@pytest.fixture
def asset_info_1():
    file1 = FileInfo(key="doc1.jpg", type=FileType.INTERNAL)
    file2 = FileInfo(key="doc2.jpg", type=FileType.INTERNAL)
    return AssetInfo(space_id=SPACE_ID, type=AssetType.IMAGES, resources=[file1, file2])


@pytest.fixture
def asset_info_2():
    file1 = FileInfo(key="doc3.jpg", type=FileType.INTERNAL)
    file2 = FileInfo(key="doc4.jpg", type=FileType.INTERNAL)
    return AssetInfo(space_id=SPACE_ID, type=AssetType.IMAGES, resources=[file1, file2])


@pytest.fixture
def collection_info(asset_info_1, asset_info_2):
    return CollectionInfo(
        space_id=SPACE_ID, name=NEW_COLLECTION_NAME, assets=[asset_info_1, asset_info_2]
    )


api = Api.from_env()


def test_list_collections():
    collections = api.collection.get_list(space_id=SPACE_ID)
    assert isinstance(collections, list)
    assert all(isinstance(collection, CollectionInfo) for collection in collections)


def test_create_collection():
    collection_info = CollectionInfo(space_id=SPACE_ID, name=NEW_COLLECTION_NAME)
    created_collection = api.collection.create(collection_info)
    assert isinstance(created_collection, CollectionInfo)
    assert created_collection.name == NEW_COLLECTION_NAME
    assert created_collection.space_id == SPACE_ID


def test_add_remove_asset_in_collection(asset_info_2):
    collections = api.collection.get_list(space_id=SPACE_ID)
    for collection in collections:
        if collection.name == NEW_COLLECTION_NAME:
            collection_id = collection.id
            break
    else:
        pytest.skip(f"Collection '{NEW_COLLECTION_NAME}' not found")
    new_asset = api.asset.get_info_by_id(
        space_id=SPACE_ID, id="0199fd07-41f8-71e8-a235-b871f67ca3d6"
    )
    res = api.collection.add_assets(collection_id, [new_asset.id])
    assert res is not None
    assert len(res) == 1, "Expected one asset to be added"

    # res = api.collection.remove_assets(collection_id, [new_asset.id])
    # assert res is not None
    # assert len(res) == 1, "Expected one asset to be removed"


# def test_delete_collection():
#     collections = api.collection.get_list(space_id=SPACE_ID)
#     for collection in collections:
#         if collection.name == NEW_COLLECTION_NAME:
#             collection_id = collection.id
#             break
#     else:
#         pytest.skip(f"Collection '{NEW_COLLECTION_NAME}' not found")
#     api.collection.delete(collection_id)
#     collections_after_deletion = api.collection.get_list(space_id=SPACE_ID)
#     assert all(
#         collection.id != collection_id for collection in collections_after_deletion
#     ), "Collection was not deleted"


def test_get_collection_assets():
    collections = api.collection.get_list(space_id=SPACE_ID)
    for collection in collections:
        if collection.name == NEW_COLLECTION_NAME:
            collection_id = collection.id
            break
    else:
        pytest.skip(f"Collection '{NEW_COLLECTION_NAME}' not found")
    assets = api.collection.get_assets(collection_id)
    assert isinstance(assets, list)
    assert all(isinstance(asset, AssetInfo) for asset in assets)
    assert len(assets) >= 0  # Can be empty


if __name__ == "__main__":
    pytest.main()
    # test_list_collections()
    # test_delete_collection()
    # test_create_collection()
    # test_add_remove_asset_in_collection(asset_info_2)
    # test_delete_collection()
