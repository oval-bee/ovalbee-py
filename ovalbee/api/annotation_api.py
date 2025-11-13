import asyncio
from typing import Any, List, Optional, Union, cast

from ovalbee.api.module_api import CRUDModuleApi
from ovalbee.domain.types.annotation import Annotation
from ovalbee.domain.types.asset import AssetType
from ovalbee.io.decorators import run_sync


class AnnotationApi(CRUDModuleApi):

    @staticmethod
    def _info_class() -> type[Annotation]:
        return Annotation

    def _endpoint_prefix(self) -> str:
        return "assets"

    # --- Creation -------------------------------------------------
    def _creation_endpoint_name(self) -> str:
        return "bulkCreate"

    def _create_field_name(self) -> str:
        return "assets"

    def create(self, asset_info: Annotation) -> Annotation:
        return cast(Annotation, super().create(asset_info))

    def create_bulk(self, asset_infos: List[Annotation]) -> List[Annotation]:
        created = super().create_bulk(asset_infos)
        return [cast(Annotation, item) for item in created]

    # --- Retrieval ------------------------------------------------
    def get_info_by_id(self, space_id: int, id: int) -> Optional[Annotation]:
        info = super().get_info_by_id(space_id, id)
        return cast(Optional[Annotation], info)

    def get_list(
        self,
        space_id: int,
        source_id: Union[str, List[str], None] = None,
    ) -> List[Annotation]:
        items = super().get_list(
            space_id=space_id, item_type=AssetType.ANNOTATIONS.value, source_id=source_id
        )
        return [cast(Annotation, item) for item in items]

    def get_by_asset_id(
        self,
        space_id: int,
        asset_id: str,
    ) -> List[Annotation]:
        items = super().get_list(
            space_id=space_id, item_type=AssetType.ANNOTATIONS.value, source_id=asset_id
        )
        return [cast(Annotation, item) for item in items]

    # --- Update ---------------------------------------------------
    def update(self, asset_info: Annotation) -> Annotation:
        updated = super().update(asset_info)
        return cast(Annotation, updated)

    # --- Download -------------------------------------------------
    async def download_async(self, space_id: int, id: str, save_dir: str = None) -> List[Any]:
        """Download all resource files of the annotation and return list of paths to them"""
        ann = self.get_info_by_id(space_id=space_id, id=id)
        if ann is None:
            raise ValueError(f"Annotation with id={id!r} not found in space {space_id!r}.")

        tasks = []
        for resource in ann.resources:
            tasks.append(resource.download_async(self._api, save_dir))
        return await asyncio.gather(*tasks)

    def download(self, space_id: int, id: str, save_dir: str = None) -> List[Any]:
        """Download all resource files of the annotation and return list of paths to them"""
        return run_sync(self.download_async, space_id=space_id, id=id, save_dir=save_dir)
