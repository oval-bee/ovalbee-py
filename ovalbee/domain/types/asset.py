import enum
from typing import List, Optional

from pydantic import Field
from pydantic.dataclasses import dataclass

# from ovalbee.domain.types.annotation import AnnotationInfo
from ovalbee.domain.types.base import BaseInfo
from ovalbee.domain.types.file import FileInfo


class AssetType(str, enum.Enum):
    IMAGES = "images"
    VIDEOS = "videos"


class AssetInfo(BaseInfo):
    name: Optional[str] = None
    workspace_id: int = Field(alias="workspaceId")
    type: AssetType = AssetType.IMAGES
    resources: List[FileInfo] = Field(default_factory=list)
    source_id: Optional[int] = Field(default=None, alias="sourceId")
    label: Optional[str] = None
    uploaded_at: Optional[str] = Field(default=None, alias="uploadedAt")
