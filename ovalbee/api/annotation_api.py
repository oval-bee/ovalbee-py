from typing import List, Optional, Union

from ovalbee.api.module_api import CRUDModuleApi
from ovalbee.domain.types.annotation import Annotation
from ovalbee.domain.types.asset import AssetType
from ovalbee.domain.types.base import BaseInfo


class AnnotationApi(CRUDModuleApi):

    @staticmethod
    def _info_class() -> BaseInfo:
        return Annotation

    def _endpoint_prefix(self) -> str:
        return "assets"

    # --- Creation -------------------------------------------------
    def _creation_endpoint_name(self) -> str:
        return "bulkCreate"

    def _create_field_name(self) -> str:
        return "assets"

    def create(self, asset_info: Annotation) -> Annotation:
        return self._create_bulk([asset_info])[0]

    def create_bulk(self, asset_infos: List[Annotation]) -> List[Annotation]:
        return self._create_bulk(asset_infos)

    # --- Retrieval ------------------------------------------------
    def get_info_by_id(self, space_id: int, id: int) -> Annotation:
        return self._get_info_by_id(space_id, id)

    def get_list(
        self,
        space_id: int,
        source_id: Union[str, List[str], None] = None,
    ) -> List[Annotation]:
        return self._get_list_all_pages(
            space_id=space_id, item_type=AssetType.ANNOTATIONS.value, source_id=source_id
        )
    
    def get_by_asset_id(
        self,
        space_id: int,
        asset_id: str,
    ) -> List[Annotation]:
        return self._get_list_all_pages(
            space_id=space_id, item_type=AssetType.ANNOTATIONS.value, source_id=asset_id
        )

    # --- Update ---------------------------------------------------
    def update(self, asset_info: Annotation) -> Annotation:
        return self._update(asset_info)
