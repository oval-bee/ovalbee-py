import asyncio
from typing import Any, List, Optional, cast

from ovalbee.api.module_api import CRUDModuleApi
from ovalbee.dto.asset import AssetInfo, AssetType
from ovalbee.io.decorators import run_sync


class AssetApi(CRUDModuleApi):

    @staticmethod
    def _info_class() -> type[AssetInfo]:
        return AssetInfo

    def _endpoint_prefix(self) -> str:
        return "assets"

    # --- Creation -------------------------------------------------
    def _creation_endpoint_name(self) -> str:
        return "bulkCreate"

    def _create_field_name(self) -> str:
        return "assets"

    def create(self, asset_info: AssetInfo) -> AssetInfo:
        return cast(AssetInfo, super().create(asset_info))

    def create_bulk(self, asset_infos: List[AssetInfo]) -> List[AssetInfo]:
        created = super().create_bulk(asset_infos)
        return [cast(AssetInfo, item) for item in created]

    # --- Retrieval ------------------------------------------------
    def get_info_by_id(self, space_id: int, id: int) -> Optional[AssetInfo]:
        result = super().get_info_by_id(space_id, id)
        return cast(Optional[AssetInfo], result)

    def get_list(
        self,
        space_id: int,
        item_type: AssetType = AssetType.IMAGES,
    ) -> List[AssetInfo]:
        item_type = item_type.value if isinstance(item_type, AssetType) else item_type
        items = super().get_list(space_id=space_id, item_type=item_type)
        return [cast(AssetInfo, item) for item in items]

    # --- Update ---------------------------------------------------
    def update(self, asset_info: AssetInfo) -> AssetInfo:
        updated = super().update(asset_info)
        return cast(AssetInfo, updated)

    # --- Download Resources --------------------------------------
    async def download_async(self, space_id: int, id: str, save_dir: str = None) -> List[Any]:
        """Download all resource files of the asset and return list of paths to them"""
        asset = self.get_info_by_id(space_id=space_id, id=id)
        if asset is None:
            raise ValueError(f"Asset with id={id!r} not found in space {space_id!r}.")

        tasks = []
        for resource in asset.resources:
            tasks.append(resource.download_async(self._api, save_dir))
        return await asyncio.gather(*tasks)

    def download(self, space_id: int, id: str, save_dir: str = None) -> List[Any]:
        """Download all resource files of the asset and return list of paths to them"""
        return run_sync(self.download_async, space_id=space_id, id=id, save_dir=save_dir)
