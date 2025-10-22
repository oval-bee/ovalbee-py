import enum
from typing import Any, Dict, List, Optional

from pydantic import Field

from ovalbee.domain.types.asset import AssetInfo, AssetType
from ovalbee.domain.types.base import BaseInfo
from ovalbee.domain.types.file import FileInfo


class AnnotationFormat(str, enum.Enum):
    """Supported annotation formats."""

    SLY = "sly"
    YOLO = "yolo"
    # COCO = "coco"
    # CUSTOM = "custom"


class AnnotationResource(FileInfo):
    """File resource for annotation data."""

    @property
    def format(self) -> Optional[AnnotationFormat]:
        if self.metadata is not None:
            return AnnotationFormat(self.metadata.get("format"))
        return None


class AnnotationTask(str, enum.Enum):
    """Types of annotation tasks."""

    OBJECT_DETECTION = "object_detection"
    INSTANCE_SEGMENTATION = "instance_segmentation"
    SEMANTIC_SEGMENTATION = "semantic_segmentation"


class Annotation(BaseInfo):
    """Single annotation data in specific format."""

    # name: Optional[str] = None  # ??
    workspace_id: Optional[int] = Field(alias="workspaceId")  # ??
    type: AssetType = AssetType.ANNOTATIONS
    resources: List[AnnotationResource] = Field(default_factory=list)
    metadata: Optional[Dict[str, Any]] = None
    source_id: Optional[int] = Field(
        default=None,
        alias="sourceId",
        description="ID of the asset annotations belongs to",
    )
    # label: Optional[str] = None

    @property
    def asset_id(self) -> Optional[str]:
        return self.source_id
