from typing import List

from ovalbee.api.base_api import CRUDModuleApi
from ovalbee.domain.types.asset import AssetInfo
from ovalbee.domain.types.base import BaseInfo


class AssetApi(CRUDModuleApi):

    @staticmethod
    def _info_class() -> BaseInfo:
        return AssetInfo

    def _endpoint_prefix(self) -> str:
        return "assets"

    # --- Creation -------------------------------------------------
    def _creation_endpoint_name(self) -> str:
        return "bulk"

    def _create_field_name(self) -> str:
        return "assets"

    def create(self, asset_info: AssetInfo) -> AssetInfo:
        return self._create_bulk([asset_info])[0]

    def create_bulk(self, asset_infos: List[AssetInfo]) -> List[AssetInfo]:
        return self._create_bulk(asset_infos)

    # --- Retrieval ------------------------------------------------
    def get_info_by_id(self, space_id: int, id: int) -> AssetInfo:
        return self._get_info_by_id(space_id, id)

    def get_list(self, space_id: int) -> List[AssetInfo]:
        return self._get_list_all_pages(space_id=space_id)

    # --- Update ---------------------------------------------------
    def update(self, asset_info: AssetInfo) -> AssetInfo:
        return self._update(asset_info)
