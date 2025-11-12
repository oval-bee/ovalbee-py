import asyncio

import ovalbee as ob
from ovalbee.api.api import Api
from ovalbee.api.storage_api import S3StorageClient

api = Api.from_env()


async def _upload_file(local_path: str):
    bucket = "test-bucket"
    key = local_path.split("/")[-1]
    semaphore = asyncio.Semaphore(5)
    # Set up S3 storage client
    s3client = S3StorageClient(
        access_key="minioadmin",
        secret_key="minioadmin",
        storage_url=f"http://localhost:9000",
        semaphore=semaphore,
    )
    await s3client.upload(
        bucket=bucket,
        key=key,
        file_path=local_path,
    )


# upload files to S3
image_path = "/Users/almaz/Downloads/1857_Train data/dataset 2025-10-21 11-22-57/img/000000795h.jpg"
annotation_path = (
    "/Users/almaz/Downloads/1857_Train data/dataset 2025-10-21 11-22-57/ann/000000795h.jpg.json"
)
asyncio.run(_upload_file(image_path))
asyncio.run(_upload_file(annotation_path))


# create new collection
COLLECTION_NAME = "debug_collection"
collection = ob.CollectionInfo(space_id=1, name=COLLECTION_NAME)
collection = api.collection.create(collection)
print(f"🟢 Created collection: {collection.name} with ID: {collection.id}")

# list collections
collections = api.collection.get_list(space_id=1)
print(f"🟢 Collections in space 1: {[col.name for col in collections]}")

# create new asset with 1 image
ASSET_NAME = "debug_image"
file_info = ob.FileInfo(
    key="debug_image.png",
    url="http://localhost:9000/test-bucket/000000795h.jpg",
    type=ob.FileType.INTERNAL,
)
asset = ob.AssetInfo(space_id=1, name=ASSET_NAME, type=ob.AssetType.IMAGES, resources=[file_info])
asset = api.asset.create(asset)
print(f"🟢 Created asset with ID: {asset.id}")

# create new annotation with 1 resource
ANNOTATION_NAME = "debug_annotation"
annotation_file_info = ob.AnnotationResource(
    key="debug_annotation.json",
    url="http://localhost:9000/test-bucket/000000795h.jpg.json",
    type=ob.FileType.INTERNAL,
    format=ob.AnnotationFormat.SLY,
)
annotation = ob.Annotation(
    space_id=1,
    name=ANNOTATION_NAME,
    resources=[annotation_file_info],
    source_id=asset.id,
)
annotation = api.annotation.create(annotation)
print(f"🟢 Created annotation with ID: {annotation.id}")

# add asset and annotation to collection
api.collection.add_assets(collection_id=collection.id, asset_ids=[asset.id, annotation.id])
print(
    f"🟢 Added asset ID {asset.id} and annotation ID {annotation.id} to collection ID {collection.id}"
)


# list annotations for the asset
annotations = api.annotation.get_by_asset_id(space_id=1, asset_id=asset.id)
print(f"🟢 Annotations for asset ID {asset.id}: {[ann.id for ann in annotations]}")

# list assets in the collection
assets_in_collection = api.collection.get_assets(
    collection_id=collection.id, item_type=ob.AssetType.IMAGES
)
print(f"🟢 Assets in collection ID {collection.id}: {[asset.id for asset in assets_in_collection]}")

# list annotations in the collection
anns_in_collection = api.collection.get_assets(
    collection_id=collection.id, item_type=ob.AssetType.ANNOTATIONS
)
print(
    f"🟢 Annotations in collection ID {collection.id}: {[asset.id for asset in anns_in_collection]}"
)

# cleanup: delete annotation, asset, collection
api.collection.remove_assets(collection_id=collection.id, asset_ids=[asset.id, annotation.id])

# check deletion
assets_after_removal = api.collection.get_assets(
    collection_id=collection.id, item_type=ob.AssetType.IMAGES
)
anns_after_removal = api.collection.get_assets(
    collection_id=collection.id, item_type=ob.AssetType.ANNOTATIONS
)
print(
    f"🟠 Assets after removal in collection ID {collection.id}: {[asset.id for asset in assets_after_removal]}"
)
print(
    f"🟠 Annotations after removal in collection ID {collection.id}: {[asset.id for asset in anns_after_removal]}"
)

# delete annotation
api.annotation.delete(id=annotation.id)
print(f"🟠 Deleted annotation ID: {annotation.id}")
# check deletion
deleted = annotations_after_deletion = api.annotation.get_info_by_id(space_id=1, id=annotation.id)
print(f"🟠 Annotations after deletion: {deleted}")

# delete asset
api.asset.delete(id=asset.id)
print(f"🟠 Deleted asset ID: {asset.id}")
# check deletion
deleted = api.asset.get_info_by_id(space_id=1, id=asset.id)
print(f"🟠 Asset after deletion: {deleted}")

# delete collection
api.collection.delete(id=collection.id)
print(f"🟠 Deleted collection ID: {collection.id}")

# check deletion
deleted = api.collection.get_info_by_id(space_id=1, id=collection.id)
print(f"🟠 Collection after deletion: {deleted}")
