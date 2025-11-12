import numpy as np
import pytest
from PIL import Image

from ovalbee.api.api import Api
from ovalbee.domain.types.annotation import (
    Annotation,
    AnnotationFormat,
    AnnotationResource,
)
from ovalbee.domain.types.asset import AssetInfo, AssetType
from ovalbee.domain.types.collection import CollectionInfo
from ovalbee.domain.types.file import FileInfo, FileType
from ovalbee.io.decorators import run_sync


class TestRenderAnnotations:

    collection: CollectionInfo = None
    asset: AssetInfo = None
    annotation: Annotation = None

    @pytest.fixture(scope="class")
    def test_constants(self):
        """Test constants fixture."""
        return {
            "bucket": "workspace",
            "space_id": 1,
        }

    @pytest.fixture(scope="class")
    def api(self):
        """API client fixture."""
        return Api.from_env()

    @pytest.fixture(scope="class")
    def test_file_paths(self):
        """Test file paths fixture."""
        return {
            "image_path": "test_data/000000795h.jpg",
            "annotation_path": "test_data/000000795h.jpg.json",
            "result_image_path": "test_data/000000795h_rendered.jpg",
        }

    def test_01_create_asset(self, api: Api, test_file_paths, test_constants):
        """Test creating an asset for rendering tests."""
        key = test_file_paths["image_path"].split("/")[-1]

        api.storage.upload(
            bucket=test_constants["bucket"],
            key=key,
            file_path=test_file_paths["image_path"],
        )
        file = FileInfo(
            key=key,
            url=f"s3://{test_constants['bucket']}/{key}",
            type=FileType.INTERNAL,
        )
        asset = AssetInfo(
            space_id=test_constants["space_id"],
            type=AssetType.IMAGES,
            resources=[file],
        )
        asset = api.asset.create(asset)

        assert isinstance(asset, str)

        asset = api.asset.get_info_by_id(space_id=test_constants["space_id"], id=asset)
        assert asset is not None
        assert isinstance(asset.id, str)

        # Store asset for later tests
        TestRenderAnnotations.asset = asset

    def test_02_create_annotation(self, api, test_file_paths, test_constants):
        """Test creating an annotation for rendering tests."""
        key = test_file_paths["annotation_path"].split("/")[-1]

        api.storage.upload(
            bucket=test_constants["bucket"],
            key=key,
            file_path=test_file_paths["annotation_path"],
        )
        annotation = api.annotation.create(
            Annotation(
                space_id=test_constants["space_id"],
                source_id=TestRenderAnnotations.asset.id,
                resources=[
                    AnnotationResource(
                        key=key,
                        url=f"s3://{test_constants['bucket']}/{key}",
                        type=FileType.INTERNAL,
                        format=AnnotationFormat.SLY,
                    )
                ],
            )
        )

        assert isinstance(annotation, str)

        annotation = api.annotation.get_info_by_id(
            space_id=test_constants["space_id"], id=annotation
        )
        assert annotation is not None
        assert isinstance(annotation.id, str)

        # Store annotation for later tests
        TestRenderAnnotations.annotation = annotation

    def test_03_render_annotation(self, api: Api, test_file_paths):
        """Test rendering the created annotation."""
        annotation = TestRenderAnnotations.annotation
        assert annotation is not None

        rendered_img = run_sync(annotation.render, api=api, img=test_file_paths["image_path"])
        Image.fromarray(rendered_img).save(test_file_paths["result_image_path"])

        assert isinstance(rendered_img, np.ndarray)
        assert rendered_img.ndim == 3  # HWC
        assert rendered_img.shape[2] == 3  # RGB channels


if __name__ == "__main__":
    pytest.main([__file__])
