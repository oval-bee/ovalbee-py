from pathlib import Path

import numpy as np
import pytest
from PIL import Image

from ovalbee.api.api import Api
from ovalbee.dto.annotation import Annotation, AnnotationFormat, AnnotationResource
from ovalbee.dto.asset import AssetInfo, AssetType
from ovalbee.dto.collection import CollectionInfo
from ovalbee.dto.file import FileInfo, FileType
from ovalbee.ops.convert.yolo import SlyToYoloConverter, YoloToSlyConverter


class TestConverters:

    annotation: Annotation = None
    result = None
    img_height = None
    img_width = None

    @pytest.fixture(scope="class")
    def api(self):
        """API client fixture."""
        return Api.from_env()

    @pytest.fixture(scope="class")
    def test_constants(self):
        """Test constants fixture."""
        return {
            "bucket": "workspace",
            "space_id": 1,
        }

    @pytest.fixture(scope="class")
    def test_file_paths(self):
        """Test file paths fixture."""
        return {
            "img": "test_data/000000795h.jpg",
            "src": "test_data/000000795h.jpg.json",
            "dst": "test_data",
        }

    def test_01_convert_sly_to_yolo(self, test_file_paths):
        """Test creating an asset for rendering tests."""
        converter = SlyToYoloConverter()
        converter.convert_file(
            src=test_file_paths["src"],
            dst_dir=test_file_paths["dst"],
        )

    def test_02_full_cycle_sly_to_yolo(self, test_file_paths):
        """Test full conversion cycle from SLY to YOLO."""
        converter = SlyToYoloConverter()
        yolo_lines = []
        sly_ann, meta = converter.read_source(test_file_paths["src"])
        TestConverters.img_height = sly_ann.img_size[0]
        TestConverters.img_width = sly_ann.img_size[1]
        yolo_lines.extend(converter.convert(sly_ann, meta))
        converter.write_target(
            ann=yolo_lines,
            path=Path(test_file_paths["dst"]) / "000000795h___.txt",
        )

    def test_03_create_annotation(self, api, test_file_paths, test_constants):
        """Test creating an annotation for rendering tests."""
        key = test_file_paths["src"].split("/")[-1]

        api.storage.upload(
            bucket=test_constants["bucket"],
            key=key,
            file_path=test_file_paths["src"],
        )
        annotation_info = Annotation(
            space_id=test_constants["space_id"],
            # source_id=TestConverters.asset.id,
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
        TestConverters.annotation = annotation

    def test_04_convert_sly_to_yolo_via_service(self, test_file_paths, api: Api):
        """Test conversion via AnnotationService."""
        path = api.annotation_ops.convert(
            annotation=TestConverters.annotation,
            to_format=AnnotationFormat.YOLO,
            res_path=str(Path(test_file_paths["dst"]) / "converted_via_service.txt"),
        )

        assert Path(path).exists()

        TestConverters.converted = path

    def test_05_upload_converted_yolo(self, api: Api, test_file_paths, test_constants):
        """Test uploading the converted YOLO annotation."""
        key = "converted/000000795h___.txt"
        api.storage.upload(
            bucket=test_constants["bucket"],
            key=key,
            file_path=TestConverters.converted,
        )
        file_info = api.storage.objects.get_info(
            bucket=test_constants["bucket"],
            key=key,
        )
        assert isinstance(file_info, FileInfo)
        assert file_info.key == key

        ann = Annotation(
            space_id=test_constants["space_id"],
            resources=[
                AnnotationResource(
                    **file_info.model_dump(),
                    format=AnnotationFormat.YOLO,
                )
            ],
            type=AssetType.ANNOTATIONS,
        )
        uploaded_annotation = api.annotation.create(ann)
        assert isinstance(uploaded_annotation, Annotation)
        assert len(uploaded_annotation.resources) == 1

        TestConverters.result = uploaded_annotation

    def test_06_convert_back_yolo_to_sly(self, api: Api, test_file_paths):
        """Test converting back from YOLO to SLY."""
        img_size = Image.open(test_file_paths["img"]).size
        path = api.annotation_ops.convert(
            annotation=TestConverters.result,
            to_format=AnnotationFormat.SLY,
            res_path=str(Path(test_file_paths["dst"]) / "converted_back_sly.json"),
            img_height=img_size[1],
            img_width=img_size[0],
        )

        assert Path(path).exists()

        # converter = YoloToSlyConverter()
        # yolo_data = converter.read_source(TestConverters.result)
        # sly_ann = converter.convert(yolo_data, TestConverters.img_height, TestConverters.img_width)
        # sly_path = Path(test_file_paths["dst"]) / f"converted_back_sly{converter.output_suffix}"
        # converter.write_target(sly_ann, sly_path)
        # assert sly_path.exists()
        # assert len(yolo_data) > 0


if __name__ == "__main__":
    pytest.main([__file__])
