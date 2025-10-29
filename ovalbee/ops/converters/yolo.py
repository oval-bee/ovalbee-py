from typing import Any, Dict, List, Literal, Optional, Tuple, Union

try:
    import numpy as np
    import supervisely as sly
except ImportError:
    pass


class YOLOTaskType:
    DETECT = "detect"
    SEGMENT = "segment"
    POSE = "pose"


def rectangle_to_yolo_line(
    class_idx: int,
    geometry: sly.Rectangle,
    img_height: int,
    img_width: int,
):
    x = geometry.center.col / img_width
    y = geometry.center.row / img_height
    w = geometry.width / img_width
    h = geometry.height / img_height
    return f"{class_idx} {x:.6f} {y:.6f} {w:.6f} {h:.6f}"


def polygon_to_yolo_line(
    class_idx: int,
    geometry: sly.Polygon,
    img_height: int,
    img_width: int,
) -> str:
    coords = []
    for point in geometry.exterior:
        x = point.col / img_width
        y = point.row / img_height
        coords.extend([x, y])
    return f"{class_idx} {' '.join(map(lambda coord: f'{coord:.6f}', coords))}"


def keypoints_to_yolo_line(
    class_idx: int,
    geometry: sly.GraphNodes,
    img_height: int,
    img_width: int,
    max_kpts_count: int,
):
    bbox = geometry.to_bbox()
    x, y, w, h = bbox.center.col, bbox.center.row, bbox.width, bbox.height
    x, y, w, h = x / img_width, y / img_height, w / img_width, h / img_height

    line = f"{class_idx} {x:.6f} {y:.6f} {w:.6f} {h:.6f}"

    for node in geometry.nodes.values():
        node: sly.Node
        visible = 2 if not node.disabled else 1
        line += (
            f" {node.location.col / img_width:.6f} {node.location.row / img_height:.6f} {visible}"
        )
    if len(geometry.nodes) < max_kpts_count:
        for _ in range(max_kpts_count - len(geometry.nodes)):
            line += " 0 0 0"

    return line


def convert_label_geometry_if_needed(
    label: sly.Label,
    task_type: Literal["detect", "segment", "pose"],
    verbose: bool = False,
) -> List[sly.Label]:
    if task_type == YOLOTaskType.DETECT:
        available_geometry_type = sly.Rectangle
        convertable_geometry_types = [
            sly.Polygon,
            sly.GraphNodes,
            sly.Bitmap,
            sly.Polyline,
            sly.AlphaMask,
            sly.AnyGeometry,
        ]
    elif task_type == YOLOTaskType.SEGMENT:
        available_geometry_type = sly.Polygon
        convertable_geometry_types = [sly.Bitmap, sly.AlphaMask, sly.AnyGeometry]
    elif task_type == YOLOTaskType.POSE:
        available_geometry_type = sly.GraphNodes
        convertable_geometry_types = []
    else:
        raise ValueError(
            f"Unsupported task type: {task_type}. "
            f"Supported types: '{YOLOTaskType.DETECT}', '{YOLOTaskType.SEGMENT}', '{YOLOTaskType.POSE}'"
        )

    if label.obj_class.geometry_type == available_geometry_type:
        return [label]

    need_convert = label.obj_class.geometry_type in convertable_geometry_types

    if need_convert:
        new_obj_cls = label.obj_class.clone(geometry_type=available_geometry_type)
        return label.convert(new_obj_cls)

    if verbose:
        sly.logger.warning(
            f"Label '{label.obj_class.name}' has unsupported geometry type: "
            f"{type(label.obj_class.geometry_type)}. Skipping."
        )
    return []


def label_to_yolo_lines(
    label: sly.Label,
    img_height: int,
    img_width: int,
    class_names: List[str],
    task_type: Literal["detect", "segment", "pose"],
) -> List[str]:
    """
    Convert the Supervisely Label to a line in the YOLO format.
    """

    labels = convert_label_geometry_if_needed(label, task_type)
    class_idx = class_names.index(label.obj_class.name)

    lines = []
    for label in labels:
        if task_type == YOLOTaskType.DETECT:
            yolo_line = rectangle_to_yolo_line(
                class_idx=class_idx,
                geometry=label.geometry,
                img_height=img_height,
                img_width=img_width,
            )
        elif task_type == YOLOTaskType.SEGMENT:
            yolo_line = polygon_to_yolo_line(
                class_idx=class_idx,
                geometry=label.geometry,
                img_height=img_height,
                img_width=img_width,
            )
        elif task_type == YOLOTaskType.POSE:
            nodes_field = label.obj_class.geometry_type.items_json_field
            max_kpts_count = len(label.obj_class.geometry_config[nodes_field])
            yolo_line = keypoints_to_yolo_line(
                class_idx=class_idx,
                geometry=label.geometry,
                img_height=img_height,
                img_width=img_width,
                max_kpts_count=max_kpts_count,
            )
        else:
            raise ValueError(
                f"Unsupported task type: {task_type}. "
                f"Supported types: '{YOLOTaskType.DETECT}', '{YOLOTaskType.SEGMENT}', '{YOLOTaskType.POSE}'"
            )

        if yolo_line is not None:
            lines.append(yolo_line)

    return lines


def sly_ann_to_yolo(
    ann: sly.Annotation,
    class_names: List[str],
    task_type: Literal["detect", "segment", "pose"] = "detect",
) -> List[str]:
    """
    Convert the Supervisely annotation to the YOLO format.
    """
    h, w = ann.img_size
    yolo_lines = []
    for label in ann.labels:
        lines = label_to_yolo_lines(
            label=label,
            img_height=h,
            img_width=w,
            class_names=class_names,
            task_type=task_type,
        )
        yolo_lines.extend(lines)
    return yolo_lines


def read_sly_file(file_path: str, meta: Union[Dict, sly.ProjectMeta]) -> sly.Annotation:
    """Read Supervisely annotation from a JSON file."""
    if isinstance(meta, dict):
        meta = sly.ProjectMeta.from_json(meta)
    ann_json = sly.json.load_json_file(file_path)
    ann = sly.Annotation.from_json(ann_json, project_meta=meta)
    return ann


def write_yolo_file(yolo_lines: List[str], save_path: str) -> None:
    """Write YOLO annotation lines to a text file."""
    with open(save_path, "w") as f:
        for line in yolo_lines:
            f.write(line + "\n")
