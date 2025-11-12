from typing import List

from ovalbee.annotation.convert import can_convert, convert
from ovalbee.domain.types.annotation import AnnotationFormat


def visualize_sly(items_files: List[str], annotation_files: List[str], save_dir: str) -> List[str]:
    return []


visualizers = {AnnotationFormat.SLY: visualize_sly}


def visualize(
    items_files: List[str],
    annotation_files: List[str],
    annotation_format: AnnotationFormat,
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
