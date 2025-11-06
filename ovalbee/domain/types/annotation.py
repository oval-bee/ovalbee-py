import enum
import tempfile
from typing import Annotated, Any, Dict, List, Optional

from pydantic import Field, field_serializer, field_validator, model_validator

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

    @field_serializer("metadata")
    def serialize_metadata(self, v: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Auto-set format in metadata when serializing"""
        if v is None:
            v = {}
        if self.format is not None:
            value = self.format.value if isinstance(self.format, AnnotationFormat) else self.format
            v["format"] = value
        return v

    @model_validator(mode="before")
    def extract_format_from_metadata(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """Get format from metadata when loading the model"""
        metadata = values.get("metadata", {})
        format_value = metadata.get("format")
        if format_value is not None:
            values["format"] = format_value
        return values

    def download(self, api):
        pass


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

    def clone(
        self,
        *,
        id: Optional[str] = None,
        space_id: Optional[int] = None,
        type: Optional[AssetType] = None,
        resources: Optional[List[AnnotationResource]] = None,
        source_id: Optional[str] = None,
    ) -> "Annotation":
        return Annotation(
            id=None,  # ? reset id on clone
            space_id=space_id if space_id is not None else self.space_id,
            type=type if type is not None else self.type,
            resources=resources if resources is not None else self.resources.copy(),
            source_id=source_id if source_id is not None else self.source_id,
        )

    def convert(self, to_format: AnnotationFormat, save_dir: str = None) -> List[str]:
        """Convert annotation to specific format and return paths to files"""
        # TODO: Maybe a separate class?
        from ovalbee.annotation.convert import converters, find_convert_chain

        if save_dir is None:
            save_dir = tempfile.TemporaryDirectory(
                prefix=f"Annotation_{self.asset_id}_format_{to_format}_"
            )
        resources_by_format: Dict[AnnotationFormat, List[AnnotationResource]] = {}
        for resource in self.resources:
            resources_by_format.setdefault(resource.format, []).append(resource)

        # iterate over converters first
        # the first converters have higher priority
        for (con_from_format, con_to_format), converter in converters.items():
            if con_to_format == to_format:
                for annotation_format, resources in resources_by_format.items():
                    if annotation_format != con_from_format:
                        continue
                    files = [resource.download(api) for resource in resources]
                    files = converter(resources, save_dir)
                    return files

        # if no direct converter is found, try to find the conversion chain
        for annotation_format, resources in resources_by_format.items():
            try:
                converters_chain = find_convert_chain(annotation_format, to_format)
            except NotImplementedError:
                continue
            files = [resource.download(api) for resource in resources]
            for converter in converters_chain:
                files = converter(files, save_dir)
            return files

        raise NotImplementedError(f"No converter found for format: {to_format}")

    def visualize(self, save_dir: str):
        from ovalbee.annotation.visualize import visualize

        if save_dir is None:
            save_dir = tempfile.TemporaryDirectory(
                prefix=f"Annotation_{self.asset_id}_visualization_"
            )
        resources_by_format: Dict[AnnotationFormat, List[AnnotationResource]] = {}
        for resource in self.resources:
            resources_by_format.setdefault(resource.format, []).append(resource)

        for annotation_format, resources in resources_by_format.items():
            try:
                items_files = download_asset_resources(self.source_id)
                annotation_files = [resource.download for resource in resources]
                return visualize(
                    items_files=items_files,
                    annotation_files=annotation_files,
                    annotation_format=annotation_format,
                    save_dir=save_dir,
                    convert_for_visualization=True,
                )
            except NotImplementedError:
                continue
        raise NotImplementedError(f"No visualizers found")
