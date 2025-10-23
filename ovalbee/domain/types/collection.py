from typing import List, Optional

from pydantic import Field

from ovalbee.domain.types.asset import AssetInfo
from ovalbee.domain.types.base import BaseInfo


class CollectionInfo(BaseInfo):
    name: str = Field(..., description="Name of the collection")
    workspace_id: int = Field(
        ...,
        alias="workspaceId",
        description="ID of the workspace the collection belongs to",
    )
    created_at: Optional[str] = Field(
        default=None,
        alias="createdAt",
        description="Creation timestamp of the collection",
    )
    assets: List[AssetInfo] = Field(
        default_factory=list,
        description="List of assets in the collection",
    )
    # updated_at: Optional[str] = Field(default=None, alias="updatedAt", description="Last update timestamp of the collection")
    # assets: List[AssetInfo] = Field(default_factory=list, description="List of assets in the collection")
    # assets: List[int] = Field(default_factory=list, description="List of asset IDs in the collection")
