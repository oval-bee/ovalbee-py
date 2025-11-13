from __future__ import annotations

import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Optional, Sequence, Union, cast

import numpy as np
from PIL import Image

from ovalbee.domain.types.annotation import (
    Annotation,
    AnnotationFormat,
    AnnotationResource,
)
from ovalbee.domain.types.asset import AssetInfo
from ovalbee.domain.types.file import FileInfo
from ovalbee.io.url import parse_s3_url
from ovalbee.ops.convert.convert import converters, find_convert_chain
from ovalbee.ops.render.visualize import (
    create_blank_mask,
    get_image_size,
    render_annotation,
    visualize,
)

if TYPE_CHECKING:
    from ovalbee.api.api import Api


class AnnotationService:
    """High level operations for annotation resources."""

    def __init__(self, api: "Api"):
        self._api = api

    # --------------------------------------------------------------------- Download
    def download_resource(
        self,
        resource: AnnotationResource,
        *,
        save_dir: Optional[str] = None,
        prefix: Optional[str] = None,
    ) -> str:
        """Download a single annotation resource and return the local path."""
        file_prefix = prefix or self._default_resource_prefix(resource)
        return resource.download(self._api, save_dir, file_prefix)

    def download_resources(
        self,
        annotation: Annotation,
        *,
        save_dir: Optional[str] = None,
    ) -> List[str]:
        """Download all resources linked to the annotation."""
        return [self.download_resource(res, save_dir=save_dir) for res in annotation.resources]

    # --------------------------------------------------------------------- Convert
    def convert(
        self,
        annotation: Annotation,
        to_format: AnnotationFormat,
        *,
        save_dir: Optional[str] = None,
    ) -> List[str]:
        target_dir = save_dir or self._make_workspace(
            f"Annotation_{annotation.asset_id}_format_{to_format.value}_"
        )
        resources_by_format = self._group_by_format(annotation.resources)

        for (from_format, target_format), converter in converters.items():
            if target_format != to_format:
                continue
            source_resources = resources_by_format.get(from_format, [])
            if not source_resources:
                continue
            files = [
                self.download_resource(res, save_dir=target_dir) for res in source_resources
            ]
            return converter(files, target_dir)

        for from_format, source_resources in resources_by_format.items():
            try:
                chain = find_convert_chain(from_format, to_format)
            except NotImplementedError:
                continue

            files = [
                self.download_resource(res, save_dir=target_dir) for res in source_resources
            ]
            for converter in chain:
                files = converter(files, target_dir)
            return files

        raise NotImplementedError(f"No converter found for format: {to_format}")

    # --------------------------------------------------------------------- Render
    def render(
        self,
        annotation: Annotation,
        *,
        image: Union[np.ndarray, Path, str, FileInfo, None] = None,
        img_height: Optional[int] = None,
        img_width: Optional[int] = None,
    ) -> np.ndarray:
        """Render annotation overlays on top of the given image."""
        base_image = self._resolve_image_source(
            annotation,
            source=image,
            img_height=img_height,
            img_width=img_width,
        )
        return render_annotation(annotation, self._api, base_image)

    # --------------------------------------------------------------------- Visualize
    def visualize(
        self,
        annotation: Annotation,
        *,
        save_dir: Optional[str] = None,
        convert_for_visualization: bool = True,
    ) -> List[str]:
        """Produce rendered visualization files for the annotation."""
        target_dir = save_dir or self._make_workspace(
            f"Annotation_{annotation.asset_id}_visualization_"
        )

        asset = self._fetch_parent_asset(annotation)
        item_files = [
            resource.download(self._api, save_dir=target_dir) for resource in asset.resources
        ]

        for ann_format, resources in self._group_by_format(annotation.resources).items():
            try:
                annotation_files = [
                    self.download_resource(res, save_dir=target_dir) for res in resources
                ]
                return visualize(
                    items_files=item_files,
                    annotation_files=annotation_files,
                    annotation_format=ann_format,
                    save_dir=target_dir,
                    convert_for_visualization=convert_for_visualization,
                )
            except NotImplementedError:
                continue

        raise NotImplementedError("No visualizers available for provided annotation resources.")

    # --------------------------------------------------------------------- helpers
    def _fetch_parent_asset(self, annotation: Annotation) -> AssetInfo:
        if annotation.space_id is None or annotation.asset_id is None:
            raise ValueError("Annotation must have space_id and source asset reference.")

        asset = self._api.asset.get_info_by_id(annotation.space_id, annotation.asset_id)
        if asset is None:
            raise ValueError(
                f"Parent asset {annotation.asset_id!r} not found in space {annotation.space_id!r}."
            )
        return cast(AssetInfo, asset)

    def _group_by_format(
        self, resources: Sequence[AnnotationResource]
    ) -> Dict[AnnotationFormat | None, List[AnnotationResource]]:
        grouped: Dict[AnnotationFormat | None, List[AnnotationResource]] = {}
        for resource in resources:
            grouped.setdefault(resource.format, []).append(resource)
        return grouped

    def _resolve_image_source(
        self,
        annotation: Annotation,
        *,
        source: Union[np.ndarray, Path, str, FileInfo, None],
        img_height: Optional[int],
        img_width: Optional[int],
    ) -> np.ndarray:
        if isinstance(source, np.ndarray):
            return source

        if isinstance(source, FileInfo):
            file_path = source.download(self._api)
            return self._read_image(file_path)

        if isinstance(source, (str, Path)):
            path = Path(source)
            if path.is_file():
                return self._read_image(path)
            bucket, key = parse_s3_url(str(source))
            if bucket is None or key is None:
                raise ValueError(f"Unsupported image source: {source}")
            tmp_path = Path(
                tempfile.NamedTemporaryFile(
                    prefix=f"AnnotationRender_{Path(key).name}_", delete=False
                ).name
            )
            self._api.storage.download(key=key, bucket=bucket, file_path=str(tmp_path))
            return self._read_image(tmp_path)

        if img_height is not None and img_width is not None:
            return create_blank_mask(img_width, img_height)

        asset_images = self._download_parent_images(annotation)
        if len(asset_images) != 1:
            raise NotImplementedError("Rendering multiple asset resources is not supported yet.")
        img_path = asset_images[0]
        img_size = get_image_size(img_path)
        return create_blank_mask(*img_size)

    def _download_parent_images(self, annotation: Annotation) -> List[str]:
        if annotation.space_id is None or annotation.asset_id is None:
            raise ValueError("Annotation must have space_id and source asset reference.")
        return self._api.asset.download(annotation.space_id, annotation.asset_id)

    @staticmethod
    def _read_image(path: Union[str, Path]) -> np.ndarray:
        with Image.open(path) as img:
            return np.array(img.convert("RGB"))

    @staticmethod
    def _make_workspace(prefix: str) -> str:
        return tempfile.mkdtemp(prefix=prefix)

    @staticmethod
    def _default_resource_prefix(resource: AnnotationResource) -> str:
        name = resource.key or Path(resource.url or "resource").name
        return f"AnnotationResource_{name}_"
