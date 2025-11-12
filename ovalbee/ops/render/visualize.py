import tempfile
from typing import TYPE_CHECKING, List, Optional, Tuple

import numpy as np
from PIL import Image, ImageDraw

if TYPE_CHECKING:
    from ovalbee.api.api import Api
from ovalbee.domain.types.annotation import AnnotationFormat
from ovalbee.ops.convert.convert import can_convert, convert
from ovalbee.ops.convert.sly import get_sly_meta_from_annotation


def get_image_size(path: str) -> Tuple[int, int]:
    """Get image size (width, height) without loading the full image into memory"""
    with Image.open(path) as img:
        return img.size  # (width, height)


def create_blank_mask(img_width: int, img_height: int) -> np.ndarray:
    """Create a blank mask image"""
    return np.array(Image.new("L", (img_width, img_height), 0))


def visualize_sly(
    img: np.ndarray,
    annotation_file: str,
    metadata: Optional[dict] = None,
) -> np.ndarray:
    """Visualize SLY annotation on the given image"""
    import supervisely as sly

    sly_meta = metadata.get("sly_meta") if metadata else None
    if sly_meta is not None:
        sly_meta = sly.ProjectMeta.from_json(sly_meta)
    else:
        sly_meta = get_sly_meta_from_annotation(sly.json.load_json_file(annotation_file))

    ann = sly.Annotation.load_json_file(annotation_file, sly_meta)
    ann.draw_pretty(img, thickness=3)

    return img


visualizers = {AnnotationFormat.SLY: visualize_sly}


def render_resource(
    ann_resource,
    api: "Api",
    img: np.ndarray,
):
    img = img.copy()
    if ann_resource.format in visualizers:
        visualizer = visualizers[ann_resource.format]
        ann_file = ann_resource.download(api)
        return visualizer(img, ann_file, ann_resource.metadata)
    raise NotImplementedError(f"No visualizer for format: {ann_resource.format.value}")


def render_annotation(
    annotation,
    api: "Api",
    img: np.ndarray,
) -> np.ndarray:
    res = img.copy()
    for ann_resource in annotation.resources:
        res = render_resource(ann_resource, api, res)
    return res


def visualize(
    items_files: List[str],
    annotation_files: List[str],
    annotation_format,
    save_dir: str,
    convert_for_visualization: bool = True,
) -> List[str]:
    if annotation_format in visualizers:
        visualizer = visualizers[annotation_format]
        return visualizer(items_files, annotation_files, save_dir)
    if convert_for_visualization:
        for vis_format, visualizer in visualizer:
            if not can_convert(annotation_format, vis_format):
                continue
            annotation_files = convert(
                annotation_files,
                from_format=annotation_format,
                to_format=vis_format,
                save_dir=save_dir,
            )
            return visualizer(items_files, annotation_files, save_dir)

    raise NotImplementedError("No visualizer")
