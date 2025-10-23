import enum
from typing import List, Optional

from pydantic import Field, field_validator
from pydantic.dataclasses import dataclass

# from ovalbee.domain.types.annotation import AnnotationInfo
from ovalbee.domain.types.base import BaseInfo
from ovalbee.domain.types.file import FileInfo


class AssetType(str, enum.Enum):
    IMAGES = "images"
    VIDEOS = "videos"
    POINT_CLOUDS = "point_clouds"
    VOLUMES = "volumes"
    ANNOTATIONS = "annotations"
    MODELS = "models"
    BLOBS = "blobs"


class AssetInfo(BaseInfo):
    # name: Optional[str] = None
    space_id: Optional[int] = Field(alias="workspaceId")
    type: AssetType = AssetType.IMAGES
    resources: List[FileInfo] = Field(default_factory=list)
    source_id: Optional[str] = Field(default=None, alias="sourceId")
    label: Optional[str] = None
    uploaded_at: Optional[str] = Field(default=None, alias="uploadedAt")

    @field_validator("resources")
    def validate_resources(cls, v):
        if v is None or len(v) == 0:
            raise ValueError("At least one resource must be provided for an asset.")
        return v
