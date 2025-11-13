from typing import List, Optional, cast

from ovalbee.api.module_api import CRUDModuleApi
from ovalbee.dto.asset import AssetInfo, AssetType
from ovalbee.dto.annotation import Annotation
from ovalbee.dto.collection import CollectionInfo


class CollectionApi(CRUDModuleApi):
    support_bulk_creation: bool = False

    @staticmethod
    def _info_class() -> type[CollectionInfo]:
        return CollectionInfo

    def _endpoint_prefix(self) -> str:
        return "collections"

    # --- Creation -------------------------------------------------
    def create(self, collection_info: CollectionInfo) -> CollectionInfo:
        return cast(CollectionInfo, super().create(collection_info))

    def create_bulk(self, *args, **kwargs):
        raise NotImplementedError("Only single collection creation is supported")

    # --- Retrieval ------------------------------------------------
    def get_info_by_id(self, space_id: int, id: int) -> Optional[CollectionInfo]:
        return cast(Optional[CollectionInfo], super().get_info_by_id(space_id, id))

    def get_list(self, space_id: int) -> List[CollectionInfo]:
        items = super().get_list(space_id=space_id)
        return [cast(CollectionInfo, item) for item in items]

    # --- Update ---------------------------------------------------
    def update(self, collection_info: CollectionInfo) -> CollectionInfo:
        return cast(CollectionInfo, super().update(collection_info))

    # --- Deletion -------------------------------------------------
    def delete(self, id: int) -> None:
        super().delete(id)

    # --- Add assets to collection ----------------------------------
    def add_assets(self, collection_id: int, asset_ids: List[int]) -> Optional[List[int]]:
        method = f"{self.endpoint}/{collection_id}/add-assets"
        data = {"assetIds": asset_ids}
        resp = self._api.post(method, data=data)
        resp_json = resp.json()
        if resp_json.get("message") == "Assets added to collection":
            return resp_json["assetIds"]
        else:
            raise Exception("Failed to add assets to collection")

    # --- Remove assets from collection -------------------------------
    def remove_assets(self, collection_id: int, asset_ids: List[int]) -> Optional[List[int]]:
        method = f"{self.endpoint}/{collection_id}/remove-assets"
        data = {"assetIds": asset_ids}
        resp = self._api.post(method, data=data)
        resp_json = resp.json()
        if resp_json.get("message") == "Assets removed from collection":
            return resp_json["assetIds"]
        else:
            raise Exception("Failed to remove assets from collection")

    # --- Get assets in collection ------------------------------------
    def get_assets(self, collection_id: int, item_type: Optional[AssetType] = None) -> List[AssetInfo | Annotation]:
        method = f"{self.endpoint}/{collection_id}/assets"
        params = {}
        if item_type:
            params["type"] = item_type.value if isinstance(item_type, AssetType) else item_type
        resp = self._api.get(method, params=params)
        resp_json = resp.json()
        res = []
        for item in resp_json["items"]:
            if item.get("type") == AssetType.ANNOTATIONS.value:
                res.append(Annotation(**item))
            else:
                res.append(AssetInfo(**item))
        return res
