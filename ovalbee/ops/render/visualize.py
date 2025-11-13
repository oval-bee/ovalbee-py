from pathlib import Path
from typing import TYPE_CHECKING, List, Optional, Tuple

import numpy as np
from PIL import Image
from supervisely.annotation.annotation import Annotation

if TYPE_CHECKING:
    from ovalbee.api.api import Api

from ovalbee.domain.types.annotation import AnnotationFormat, AnnotationResource
from ovalbee.ops.convert.convert import can_convert, convert
from ovalbee.ops.render.sly import visualize_sly

visualizers = {AnnotationFormat.SLY: visualize_sly}


def get_image_size(path: str) -> Tuple[int, int]:
    """Get image size (width, height) without loading the full image into memory"""
    with Image.open(path) as img:
        return img.size  # (width, height)


def create_blank_mask(img_width: int, img_height: int):  # -> np.ndarray:
    """Create a blank mask image"""
    import numpy as np
    from PIL import Image

    return np.array(Image.new("RGBA", (img_width, img_height), (0, 0, 0, 0)))


def render_resource(
    ann_resource: AnnotationResource,
    api: "Api",
    img,  # : np.ndarray,
):
    import numpy as np

    img: np.ndarray

    img = img.copy()
    if ann_resource.format in visualizers:
        visualizer = visualizers[ann_resource.format]
        ann_file = ann_resource.download(api)
        return visualizer(img, ann_file, ann_resource.metadata)
    raise NotImplementedError(f"No visualizer for format: {ann_resource.format.value}")


def render_annotation(
    annotation: Annotation,
    api: "Api",
    img,  # : np.ndarray,
):  # -> np.ndarray:
    import numpy as np

    img: np.ndarray
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
        rendered_paths: List[str] = []
        Path(save_dir).mkdir(parents=True, exist_ok=True)
        if len(annotation_files) not in (1, len(items_files)):
            raise ValueError("Annotation files count must be 1 or match items_files length.")

        if len(annotation_files) == 1 and len(items_files) > 1:
            annotation_files = annotation_files * len(items_files)

        for item_path, ann_path in zip(items_files, annotation_files):
            img = np.array(Image.open(item_path).convert("RGB"))
            rendered = visualizer(img, ann_path, metadata=None)
            out_path = Path(save_dir) / f"{Path(item_path).stem}_annotated.png"
            Image.fromarray(rendered).save(out_path)
            rendered_paths.append(str(out_path))
        return rendered_paths

    if convert_for_visualization:
        for vis_format, visualizer in visualizers.items():
            if not can_convert(annotation_format, vis_format):
                continue
            converted_files = convert(
                annotation_files,
                from_format=annotation_format,
                to_format=vis_format,
                save_dir=save_dir,
            )
            return visualize(
                items_files=items_files,
                annotation_files=converted_files,
                annotation_format=vis_format,
                save_dir=save_dir,
                convert_for_visualization=False,
            )

    raise NotImplementedError("No visualizer available for format.")
