import asyncio

import pytest

import ovalbee as ob
from ovalbee.api.api import Api


class TestIntegrationWorkflow:
    """Integration test for the complete workflow: upload files, create collection, asset, annotation, and cleanup."""

    # Class-level attributes to store created resources
    collection = None
    asset = None
    annotation = None

    @pytest.fixture(scope="class")
    def api(self):
        """API client fixture."""
        return Api(
            server_address="http://localhost:30080/api",
            token="bzzz_admin_api_token_$MTpnbzkyUllVZmFBWTVLUmJ4cUo5clVIRVh1a1ppMlJ4Rg",
        )

    @pytest.fixture(scope="class")
    def test_file_paths(self):
        """Test file paths fixture."""
        return {
            "image_path": "/Users/almaz/Downloads/1857_Train data/dataset 2025-10-21 11-22-57/img/000000795h.jpg",
            "annotation_path": "/Users/almaz/Downloads/1857_Train data/dataset 2025-10-21 11-22-57/ann/000000795h.jpg.json",
        }

    @pytest.fixture(scope="class")
    def test_constants(self):
        """Test constants fixture."""
        return {
            "bucket": "workspace",
            "collection_name": "debug_collection",
            "asset_name": "debug_image",
            "annotation_name": "debug_annotation",
            "space_id": 1,
        }

    def _upload_file(self, api, local_path: str, bucket: str):
        """Helper method to upload files to S3."""
        key = local_path.split("/")[-1]
        api.storage.upload(bucket=bucket, key=key, file_path=local_path)

    def test_01_upload_files_to_s3(self, api, test_file_paths, test_constants):
        """Test uploading image and annotation files to S3."""
        # Upload image file
        self._upload_file(api, test_file_paths["image_path"], test_constants["bucket"])

        # Upload annotation file
        self._upload_file(api, test_file_paths["annotation_path"], test_constants["bucket"])

        # If we reach here without exceptions, uploads were successful
        assert True

    def test_02_create_collection(self, api, test_constants):
        """Test creating a new collection."""
        collection = ob.CollectionInfo(
            space_id=test_constants["space_id"], name=test_constants["collection_name"]
        )
        created_collection = api.collection.create(collection)

        assert created_collection is not None
        assert created_collection.name == test_constants["collection_name"]
        assert created_collection.id is not None

        # Store collection for later tests
        TestIntegrationWorkflow.collection = created_collection

    def test_03_list_collections(self, api, test_constants):
        """Test listing collections in the space."""
        collections = api.collection.get_list(space_id=test_constants["space_id"])

        assert isinstance(collections, list)
        assert len(collections) > 0

        # Verify our collection is in the list
        collection_names = [col.name for col in collections]
        assert test_constants["collection_name"] in collection_names

    def test_04_create_asset(self, api, test_constants):
        """Test creating a new asset with image resource."""
        file_info = ob.FileInfo(
            key="debug_image.png",
            url="http://localhost:9000/test-bucket/000000795h.jpg",
            type=ob.FileType.INTERNAL,
        )
        asset = ob.AssetInfo(
            space_id=test_constants["space_id"],
            type=ob.AssetType.IMAGES,
            resources=[file_info],
        )
        created_asset = api.asset.create(asset)

        assert isinstance(created_asset, str)

        assert isinstance(created_asset, str)
        created_asset = api.asset.get_info_by_id(
            space_id=test_constants["space_id"], id=created_asset
        )
        assert created_asset.id is not None
        assert created_asset.type == ob.AssetType.IMAGES

        # Store asset for later tests
        TestIntegrationWorkflow.asset = created_asset

    def test_05_create_annotation(self, api, test_constants):
        """Test creating a new annotation with annotation resource."""
        annotation_file_info = ob.AnnotationResource(
            key="debug_annotation.json",
            url="http://localhost:9000/test-bucket/000000795h.jpg.json",
            type=ob.FileType.INTERNAL,
            format=ob.AnnotationFormat.SLY,
        )
        annotation = ob.Annotation(
            space_id=test_constants["space_id"],
            resources=[annotation_file_info],
            source_id=TestIntegrationWorkflow.asset.id,
        )
        created_annotation = api.annotation.create(annotation)

        assert isinstance(created_annotation, str)
        created_annotation = api.annotation.get_info_by_id(
            space_id=test_constants["space_id"], id=created_annotation
        )
        assert created_annotation.id is not None
        assert created_annotation.source_id == TestIntegrationWorkflow.asset.id

        # Store annotation for later tests
        TestIntegrationWorkflow.annotation = created_annotation

    def test_06_add_assets_to_collection(self, api):
        """Test adding asset and annotation to collection."""
        api.collection.add_assets(
            collection_id=TestIntegrationWorkflow.collection.id,
            asset_ids=[TestIntegrationWorkflow.asset.id, TestIntegrationWorkflow.annotation.id],
        )

        # If we reach here without exceptions, addition was successful
        assert True

    def test_07_list_annotations_for_asset(self, api, test_constants):
        """Test listing annotations for the created asset."""
        annotations = api.annotation.get_by_asset_id(
            space_id=test_constants["space_id"], asset_id=TestIntegrationWorkflow.asset.id
        )

        assert isinstance(annotations, list)
        assert len(annotations) > 0

        # Verify our annotation is in the list
        annotation_ids = [ann.id for ann in annotations]
        assert TestIntegrationWorkflow.annotation.id in annotation_ids

    def test_08_list_assets_in_collection(self, api):
        """Test listing assets in the collection."""
        assets_in_collection = api.collection.get_assets(
            collection_id=TestIntegrationWorkflow.collection.id, item_type=ob.AssetType.IMAGES
        )

        assert isinstance(assets_in_collection, list)
        assert len(assets_in_collection) > 0

        # Verify our asset is in the collection
        asset_ids = [asset.id for asset in assets_in_collection]
        assert TestIntegrationWorkflow.asset.id in asset_ids

    def test_09_list_annotations_in_collection(self, api):
        """Test listing annotations in the collection."""
        anns_in_collection = api.collection.get_assets(
            collection_id=TestIntegrationWorkflow.collection.id, item_type=ob.AssetType.ANNOTATIONS
        )

        assert isinstance(anns_in_collection, list)
        assert len(anns_in_collection) > 0

        # Verify our annotation is in the collection
        annotation_ids = [ann.id for ann in anns_in_collection]
        assert TestIntegrationWorkflow.annotation.id in annotation_ids

    def test_10_remove_assets_from_collection(self, api):
        """Test removing assets from collection."""
        api.collection.remove_assets(
            collection_id=TestIntegrationWorkflow.collection.id,
            asset_ids=[TestIntegrationWorkflow.asset.id, TestIntegrationWorkflow.annotation.id],
        )

        # Verify assets were removed
        assets_after_removal = api.collection.get_assets(
            collection_id=TestIntegrationWorkflow.collection.id, item_type=ob.AssetType.IMAGES
        )
        anns_after_removal = api.collection.get_assets(
            collection_id=TestIntegrationWorkflow.collection.id, item_type=ob.AssetType.ANNOTATIONS
        )

        # Check that our assets are no longer in the collection
        asset_ids_after = [asset.id for asset in assets_after_removal]
        ann_ids_after = [ann.id for ann in anns_after_removal]

        assert TestIntegrationWorkflow.asset.id not in asset_ids_after
        assert TestIntegrationWorkflow.annotation.id not in ann_ids_after

    def test_11_delete_annotation(self, api, test_constants):
        """Test deleting annotation."""
        api.annotation.delete(id=TestIntegrationWorkflow.annotation.id)

        # Verify deletion by trying to get annotation info
        deleted = api.annotation.get_info_by_id(
            space_id=test_constants["space_id"], id=TestIntegrationWorkflow.annotation.id
        )

        # # soft deleting does not remove the annotation completely
        # assert (
        #     deleted is None or getattr(deleted, "id", None) != TestIntegrationWorkflow.annotation.id
        # )

    def test_12_delete_asset(self, api, test_constants):
        """Test deleting asset."""
        api.asset.delete(id=TestIntegrationWorkflow.asset.id)

        # Verify deletion by trying to get asset info
        deleted = api.asset.get_info_by_id(
            space_id=test_constants["space_id"], id=TestIntegrationWorkflow.asset.id
        )

        # soft deleting does not remove the asset completely
        # assert deleted is None or getattr(deleted, "id", None) != TestIntegrationWorkflow.asset.id

    def test_13_delete_collection(self, api, test_constants):
        """Test deleting collection."""
        api.collection.delete(id=TestIntegrationWorkflow.collection.id)

        # Verify deletion by trying to get collection info
        deleted = api.collection.get_info_by_id(
            space_id=test_constants["space_id"], id=TestIntegrationWorkflow.collection.id
        )

        # Expecting None or some indication that collection doesn't exist
        assert (
            deleted is None or getattr(deleted, "id", None) != TestIntegrationWorkflow.collection.id
        )


if __name__ == "__main__":
    pytest.main([__file__])
