"""
Tests for asset-related types and functionalities.
"""

import pytest

from ovalbee.api.api import Api
from ovalbee.domain.types.annotation import (
    Annotation,
    AnnotationFormat,
    AnnotationResource,
    AnnotationTask,
)
from ovalbee.domain.types.asset import AssetInfo, AssetType
from ovalbee.domain.types.file import FileInfo, FileType

SPACE_ID = 1


@pytest.fixture
def ann_info():
    file1 = AnnotationResource(
        key="image1.json",
        url="http://example.com/image1.json",
        type=FileType.INTERNAL,
        format=AnnotationFormat.SLY,
    )
    file2 = AnnotationResource(
        key="image2.json",
        url="http://example.com/image2.json",
        type=FileType.INTERNAL,
        format=AnnotationFormat.SLY,
    )
    return Annotation(
        space_id=SPACE_ID,
        name="new.jpg",
        type=AssetType.ANNOTATIONS,
        resources=[file1, file2],
        source_id="0199fd07-41f8-71e8-a235-b871f67ca3d6",
    )


api = Api.from_env()


def test_list_annotations():
    assets = api.annotation.get_list(space_id=SPACE_ID)
    assert isinstance(assets, list)
    assert all(isinstance(asset, Annotation) for asset in assets)


def test_create_annotation(ann_info):
    created_asset = api.annotation.create(ann_info)
    assert isinstance(created_asset, Annotation)
    assert isinstance(created_asset.id, str)
    assert created_asset.type == AssetType.ANNOTATIONS
    assert len(created_asset.resources) == 2
    assert created_asset.space_id == SPACE_ID


if __name__ == "__main__":
    pytest.main()
    # test_list_assets()
    # test_create_asset(asset_info=asset_info)

if __name__ == "__main__":
    pytest.main()
    # test_list_assets()
    # test_create_asset(asset_info=asset_info)
