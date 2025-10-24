import enum
from typing import Any, Dict, List, Optional

from pydantic import Field, field_validator

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

    format: Optional[AnnotationFormat] = None

    # auto-set format in metadata when dumping
    def model_dump(self, *args: Any, **kwargs: Any):
        data = super().model_dump(*args, **kwargs)
        if self.format is not None:
            if "metadata" not in data or data["metadata"] is None:
                data["metadata"] = {}
            data["metadata"]["format"] = self.format.value
        return data

    # auto-get format from metadata when loading
    @classmethod
    def model_validate(cls, data: Dict[str, Any]) -> "AnnotationResource":
        if "metadata" in data and data["metadata"] is not None:
            format_value = data["metadata"].get("format")
            if format_value is not None:
                data["format"] = AnnotationFormat(format_value)
        return super().model_validate(data)


class AnnotationTask(str, enum.Enum):
    """Types of annotation tasks."""

    OBJECT_DETECTION = "object_detection"
    INSTANCE_SEGMENTATION = "instance_segmentation"
    SEMANTIC_SEGMENTATION = "semantic_segmentation"


class Annotation(BaseInfo):
    """Single annotation data in specific format."""

    # name: Optional[str] = None  # ??
    space_id: Optional[int] = Field(alias="workspaceId")  # ??
    type: AssetType = AssetType.ANNOTATIONS
    resources: List[AnnotationResource] = Field(default_factory=list)
    source_id: Optional[str] = Field(
        default=None,
        alias="sourceId",
        description="ID of the asset annotations belongs to",
    )

    @property
    def asset_id(self) -> Optional[str]:
        return self.source_id

    @field_validator("resources")
    def validate_resources(cls, v):
        if v is None or len(v) == 0:
            raise ValueError("At least one resource must be provided for an asset.")
        return v
