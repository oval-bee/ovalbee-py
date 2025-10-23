from typing import List, Optional

from ovalbee.api.module_api import CRUDModuleApi
from ovalbee.domain.types.asset import AssetInfo
from ovalbee.domain.types.base import BaseInfo
from ovalbee.domain.types.collection import CollectionInfo


class CollectionApi(CRUDModuleApi):

    @staticmethod
    def _info_class() -> BaseInfo:
        return CollectionInfo

    def _endpoint_prefix(self) -> str:
        return "collections"

    # --- Creation -------------------------------------------------
    def create(self, collection_info: CollectionInfo) -> CollectionInfo:
        method = self._endpoint_prefix()
        data = collection_info.model_dump(exclude_unset=True)
        resp = self._api.post(method, data=data)
        resp_json = resp.json()
        collection_id = resp_json.get("id")
        created = self.get_info_by_id(space_id=collection_info.workspace_id, id=collection_id)
        return created

    def create_bulk(self, *args, **kwargs):
        raise NotImplementedError("Only single collection creation is supported")

    # --- Retrieval ------------------------------------------------
    def get_info_by_id(self, space_id: int, id: int) -> CollectionInfo:
        return self._get_info_by_id(space_id, id)

    def get_list(self, space_id: int) -> List[CollectionInfo]:
        return self._get_list_all_pages(space_id=space_id)

    # --- Update ---------------------------------------------------
    def update(self, collection_info: CollectionInfo) -> CollectionInfo:
        return self._update(collection_info)

    # --- Deletion -------------------------------------------------
    def delete(self, collection_id: int) -> None:
        self._delete(collection_id)

    # --- Add assets to collection ----------------------------------
    def add_assets(self, collection_id: int, asset_ids: List[int]) -> Optional[List[int]]:
        method = f"{self._endpoint_prefix()}/{collection_id}/add-assets"
        data = {"assetIds": asset_ids}
        resp = self._api.post(method, data=data)
        resp_json = resp.json()
        if resp_json.get("message") == "Assets added to collection":
            return resp_json["assetIds"]
        else:
            raise Exception("Failed to add assets to collection")

    # --- Remove assets from collection -------------------------------
    def remove_assets(self, collection_id: int, asset_ids: List[int]) -> Optional[List[int]]:
        method = f"{self._endpoint_prefix()}/{collection_id}/remove-assets"
        data = {"assetIds": asset_ids}
        resp = self._api.post(method, data=data)
        resp_json = resp.json()
        if resp_json.get("message") == "Assets removed from collection":
            return resp_json["assetIds"]
        else:
            raise Exception("Failed to remove assets from collection")

    # --- Get assets in collection ------------------------------------
    def get_assets(self, collection_id: int) -> List[AssetInfo]:
        method = f"{self._endpoint_prefix()}/{collection_id}/assets"
        resp = self._api.get(method, params={})
        resp_json = resp.json()
        return [AssetInfo(**asset) for asset in resp_json["items"]]
