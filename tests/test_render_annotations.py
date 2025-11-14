import numpy as np
import pytest
from PIL import Image

from ovalbee.api.api import Api
from ovalbee.dto.annotation import Annotation, AnnotationFormat, AnnotationResource
from ovalbee.dto.asset import AssetInfo, AssetType
from ovalbee.dto.collection import CollectionInfo
from ovalbee.dto.file import FileInfo, FileType


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
            "yolo_ann": "test_data/100016h.txt",
            "yolo_image": "test_data/100016h.jpg",
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

        assert isinstance(asset, AssetInfo)
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
        annotation_info = Annotation(
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
        annotation = api.annotation.create(annotation_info)

        assert isinstance(annotation, Annotation)
        assert isinstance(annotation.id, str)

        # Store annotation for later tests
        TestRenderAnnotations.annotation = annotation

    def test_03_render_annotation(self, api: Api, test_file_paths):
        """Test rendering the created annotation."""
        annotation = TestRenderAnnotations.annotation
        assert annotation is not None

        rendered_img = api.annotation_ops.render(annotation, image=test_file_paths["image_path"])
        Image.fromarray(rendered_img).save(test_file_paths["result_image_path"])

        assert isinstance(rendered_img, np.ndarray)
        assert rendered_img.ndim == 3  # HWC
        assert rendered_img.shape[2] == 3  # RGB channels

    def test_04_render_yolo_annotation(self, api: Api, test_file_paths):
        """Test rendering a YOLO annotation."""
        key = test_file_paths["yolo_ann"].split("/")[-1]

        api.storage.upload(
            bucket="workspace",
            key=key,
            file_path=test_file_paths["yolo_ann"],
        )
        annotation_info = Annotation(
            space_id=1,
            resources=[
                AnnotationResource(
                    key=key,
                    url=f"s3://workspace/{key}",
                    type=FileType.INTERNAL,
                    format=AnnotationFormat.YOLO,
                )
            ],
        )
        annotation = api.annotation.create(annotation_info)

        rendered_img = api.annotation_ops.render(
            annotation, image=test_file_paths["yolo_image"]
        )

        assert isinstance(rendered_img, np.ndarray)
        assert rendered_img.ndim == 3  # HWC
        assert rendered_img.shape[2] == 3  # RGB channels


if __name__ == "__main__":
    pytest.main([__file__])
