
from typing import Any, Generator, List

from ovalbee.api.module_api import CRUDModuleApi
from ovalbee.domain.types.asset import AssetInfo, AssetType
from ovalbee.domain.types.base import BaseInfo


class AssetApi(CRUDModuleApi):

    @staticmethod
    def _info_class() -> BaseInfo:
        return AssetInfo

    def _endpoint_prefix(self) -> str:
        return "assets"

    # --- Creation -------------------------------------------------
    def _creation_endpoint_name(self) -> str:
        return "bulkCreate"

    def _create_field_name(self) -> str:
        return "assets"

    def create(self, asset_info: AssetInfo) -> int:
        return self._create_bulk([asset_info])[0]

    def create_bulk(self, asset_infos: List[AssetInfo]) -> List[int]:
        return self._create_bulk(asset_infos)

    # --- Retrieval ------------------------------------------------
    def get_info_by_id(self, space_id: int, id: int) -> AssetInfo:
        return self._get_info_by_id(space_id, id)

    def get_list(
        self,
        space_id: int,
        item_type: AssetType = AssetType.IMAGES,
    ) -> List[AssetInfo]:
        item_type = item_type.value if isinstance(item_type, AssetType) else item_type
        return self._get_list_all_pages(space_id=space_id, item_type=item_type)

    # --- Update ---------------------------------------------------
    def update(self, asset_info: AssetInfo) -> AssetInfo:
        return self._update(asset_info)

    # --- Download Resources --------------------------------------
    def download_resources_generator(
        self, space_id: int, id: str, save_dir: str = None
    ) -> Generator[Any, None, None]:
        asset = self.get_info_by_id(space_id=space_id, id=id)

        for resource in asset.resources:
            resource.download(self._api, save_dir)

    def download_resources(self, space_id: int, id: str, save_dir: str = None) -> List[Any]:
        return list(self.download_resources_generator(space_id=space_id, id=id, save_dir=save_dir))
