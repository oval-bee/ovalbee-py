from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Sequence, Tuple, Union

from supervisely.geometry.graph import GraphNodes

try:
    import numpy as np

    import supervisely as sly
except ImportError:
    pass

from ovalbee.dto.annotation import AnnotationFormat
from ovalbee.ops.convert.base import AnnotationConverter
from ovalbee.ops.convert.sly import get_sly_meta_from_annotation


class YOLOTaskType:
    DETECT = "detect"
    SEGMENT = "segment"
    POSE = "pose"


def sly_rectangle_to_yolo_line(
    class_idx: int,
    geometry: "sly.Rectangle",
    img_height: int,
    img_width: int,
):
    x = geometry.center.col / img_width
    y = geometry.center.row / img_height
    w = geometry.width / img_width
    h = geometry.height / img_height
    return f"{class_idx} {x:.6f} {y:.6f} {w:.6f} {h:.6f}"


def sly_polygon_to_yolo_line(
    class_idx: int,
    geometry: "sly.Polygon",
    img_height: int,
    img_width: int,
) -> str:
    coords = []
    for point in geometry.exterior:
        x = point.col / img_width
        y = point.row / img_height
        coords.extend([x, y])
    return f"{class_idx} {' '.join(map(lambda coord: f'{coord:.6f}', coords))}"


def sly_keypoints_to_yolo_line(
    class_idx: int,
    geometry: "sly.GraphNodes",
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


def convert_sly_geometry_if_needed(
    label: "sly.Label",
    task_type: Literal["detect", "segment", "pose"],
    verbose: bool = False,
) -> List["sly.Label"]:
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


def sly_label_to_yolo_lines(
    label: sly.Label,
    img_height: int,
    img_width: int,
    class_names: List[str],
    task_type: Literal["detect", "segment", "pose"],
) -> List[str]:
    """
    Convert the Supervisely Label to a line in the YOLO format.
    """

    labels = convert_sly_geometry_if_needed(label, task_type)
    class_idx = class_names.index(label.obj_class.name)

    lines = []
    for label in labels:
        if task_type == YOLOTaskType.DETECT:
            yolo_line = sly_rectangle_to_yolo_line(
                class_idx=class_idx,
                geometry=label.geometry,
                img_height=img_height,
                img_width=img_width,
            )
        elif task_type == YOLOTaskType.SEGMENT:
            yolo_line = sly_polygon_to_yolo_line(
                class_idx=class_idx,
                geometry=label.geometry,
                img_height=img_height,
                img_width=img_width,
            )
        elif task_type == YOLOTaskType.POSE:
            nodes_field = label.obj_class.geometry_type.items_json_field
            max_kpts_count = len(label.obj_class.geometry_config[nodes_field])
            yolo_line = sly_keypoints_to_yolo_line(
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
        lines = sly_label_to_yolo_lines(
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
    ann_json = sly.json.load_json_file(str(file_path))
    ann = sly.Annotation.from_json(ann_json, project_meta=meta)
    return ann


def write_yolo_file(yolo_lines: List[str], save_path: str) -> None:
    """Write YOLO annotation lines to a text file."""
    with open(save_path, "w") as f:
        for line in yolo_lines:
            f.write(line + "\n")


SLY_YOLO_TASK_TYPE_MAP = {
    "object detection": YOLOTaskType.DETECT,
    "instance segmentation": YOLOTaskType.SEGMENT,
    "pose estimation": YOLOTaskType.POSE,
}

YOLO_DETECTION_COORDS_NUM = 4
YOLO_SEGM_MIN_COORDS_NUM = 6
YOLO_KEYPOINTS_MIN_COORDS_NUM = 6

coco_classes = [
    "person",
    "bicycle",
    "car",
    "motorcycle",
    "airplane",
    "bus",
    "train",
    "truck",
    "boat",
    "traffic light",
    "fire hydrant",
    "stop sign",
    "parking meter",
    "bench",
    "bird",
    "cat",
    "dog",
    "horse",
    "sheep",
    "cow",
    "elephant",
    "bear",
    "zebra",
    "giraffe",
    "backpack",
    "umbrella",
    "handbag",
    "tie",
    "suitcase",
    "frisbee",
    "skis",
    "snowboard",
    "sports ball",
    "kite",
    "baseball bat",
    "baseball glove",
    "skateboard",
    "surfboard",
    "tennis racket",
    "bottle",
    "wine glass",
    "cup",
    "fork",
    "knife",
    "spoon",
    "bowl",
    "banana",
    "apple",
    "sandwich",
    "orange",
    "broccoli",
    "carrot",
    "hot dog",
    "pizza",
    "donut",
    "cake",
    "chair",
    "couch",
    "potted plant",
    "bed",
    "dining table",
    "toilet",
    "tv",
    "laptop",
    "mouse",
    "remote",
    "keyboard",
    "cell phone",
    "microwave",
    "oven",
    "toaster",
    "sink",
    "refrigerator",
    "book",
    "clock",
    "vase",
    "scissors",
    "teddy bear",
    "hair drier",
    "toothbrush",
]


def get_coordinates(line: str) -> Tuple[int, List[float]]:
    """
    Parse coordinates from a line in the YOLO format.
    """
    class_index = int(line[0])
    coords = list(map(float, line[1:]))
    return class_index, coords


def convert_rectangle(
    img_height: int,
    img_width: int,
    coords: List[float],
    **kwargs,
) -> "sly.Rectangle":
    """
    Convert rectangle coordinates from relative (0-1) to absolute (px) values.
    """
    import supervisely as sly

    x_center, y_center, ann_width, ann_height = coords
    x_center = float(x_center)
    y_center = float(y_center)
    ann_width = float(ann_width)
    ann_height = float(ann_height)

    px_x_center = x_center * img_width
    px_y_center = y_center * img_height

    px_ann_width = ann_width * img_width
    px_ann_height = ann_height * img_height

    left = int(px_x_center - (px_ann_width / 2))
    right = int(px_x_center + (px_ann_width / 2))

    top = int(px_y_center - (px_ann_height / 2))
    bottom = int(px_y_center + (px_ann_height / 2))

    # check if the coordinates are within the image
    left, top = max(0, left), max(0, top)
    right, bottom = min(img_width, right), min(img_height, bottom)

    return sly.Rectangle(top, left, bottom, right)


def validate_polygon_coords(coords: List[float]) -> List[float]:
    """
    Check and correct polygon coordinates:
    - remove the last point if it is the same as the first one
    """
    if coords[0] == coords[-2] and coords[1] == coords[-1]:
        return coords[:-2]
    return coords


def convert_polygon(
    img_height: int,
    img_width: int,
    coords: List[float],
    **kwargs,
) -> Union["sly.Polygon", None]:
    """
    Convert polygon coordinates from relative (0-1) to absolute (px) values.
    """
    import supervisely as sly

    coords = validate_polygon_coords(coords)
    if len(coords) < 6:
        return None

    exterior = []
    for i in range(0, len(coords), 2):
        x = coords[i]
        y = coords[i + 1]
        px_x = min(img_width, max(0, int(x * img_width)))
        px_y = min(img_height, max(0, int(y * img_height)))
        exterior.append([px_y, px_x])
    return sly.Polygon(exterior=exterior)


def convert_keypoints(
    img_height: int,
    img_width: int,
    num_keypoints: int,
    num_dims: int,
    coords: List[float],
    **kwargs,
) -> Union["sly.GraphNodes", None]:
    """
    Convert keypoints coordinates from relative (0-1) to absolute (px) values.
    """
    import supervisely as sly

    nodes = []
    step = 3 if num_dims == 3 else 2
    shift = 4
    for i in range(shift, num_keypoints * step + shift, step):
        x = coords[i]
        y = coords[i + 1]
        visibility = int(coords[i + 2]) if num_dims == 3 else 2
        if visibility in [0, 1]:
            continue  # skip invisible keypoints
        px_x = min(img_width, max(0, int(x * img_width)))
        px_y = min(img_height, max(0, int(y * img_height)))
        node = sly.Node(row=px_y, col=px_x)  # , disabled=v)
        nodes.append(node)
    if len(nodes) > 0:
        return sly.GraphNodes(nodes)


def create_geometry_config(num_keypoints: int = None):
    """
    Create a template for keypoints with the specified number of keypoints.
    """
    from supervisely.geometry.graph import KeypointsTemplate

    i, j = 0, 0
    template = KeypointsTemplate()
    for p in list(range(num_keypoints)):
        template.add_point(label=str(p), row=i, col=j)
        j += 1
        i += 1

    return template


def is_applicable_for_rectangles(coords: List[float], **kwargs) -> bool:
    """
    Check if the coordinates are applicable for rectangles.
    """
    return len(coords) == YOLO_DETECTION_COORDS_NUM


def is_applicable_for_polygons(
    with_keypoint: bool,
    coords: List[float],
    **kwargs,
) -> bool:
    """
    Check if the coordinates are applicable for polygons.

    :param with_keypoint: Whether the YAML config file contains keypoints.
    :type with_keypoint: bool
    """
    if with_keypoint:
        return False
    return len(coords) >= YOLO_SEGM_MIN_COORDS_NUM and len(coords) % 2 == 0


def is_applicable_for_keypoints(
    with_keypoint: bool,
    num_keypoints: int,
    num_dims: int,
    coords: List[float],
    **kwargs,
) -> bool:
    """
    Check if the coordinates are applicable for keypoints.
    """
    if not with_keypoint or not num_keypoints or not num_dims:
        return False
    if len(coords) < YOLO_KEYPOINTS_MIN_COORDS_NUM:
        return False
    return len(coords) == num_keypoints * num_dims + 4


class SlyToYoloConverter(AnnotationConverter):
    """Convert Supervisely annotation files into YOLO format."""

    source_format = AnnotationFormat.SLY
    target_format = AnnotationFormat.YOLO
    output_suffix = ".txt"
    task_type = YOLOTaskType.DETECT

    def read_source(self, path: Path, meta=None) -> "sly.Annotation":
        import supervisely as sly

        ann_json = sly.json.load_json_file(str(path))
        if isinstance(meta, dict):
            meta = sly.ProjectMeta.from_json(meta)
        elif meta is None:
            meta = get_sly_meta_from_annotation(ann_json)
        ann = sly.Annotation.from_json(ann_json, meta)
        return ann, meta

    def write_target(self, ann, path):
        with open(path, "w") as f:
            for line in ann:
                f.write(line + "\n")

    def convert_file(self, src: Path | str, dst_dir: Path | str) -> str:
        annotation, meta = self.read_source(src)
        yolo_lines = self.convert(annotation, meta)
        destination_path = Path(dst_dir) / f"{Path(src).stem}{self.output_suffix}"
        self.write_target(yolo_lines, destination_path)
        return str(destination_path)

    def convert(self, ann, meta: Union[Dict[str, Any], "sly.ProjectMeta"]) -> List[str]:
        class_names = self._prepare_class_names(meta)
        h, w = ann.img_size
        yolo_lines = []
        for label in ann.labels:
            lines = self._sly_label_to_yolo_lines(
                label=label,
                img_height=h,
                img_width=w,
                class_names=class_names,
                task_type=self.task_type,
            )
            yolo_lines.extend(lines)
        return yolo_lines

    def convert_files(
        self,
        source_files: Sequence[Union[str, Path]],
        dst_path: Union[str, Path],
        *args,
        **kwargs,
    ) -> str:
        dst_dir = Path(dst_path)
        dst_dir.parent.mkdir(parents=True, exist_ok=True)

        merged_lines: List[str] = []
        for src_path in source_files:
            ann, meta = self.read_source(Path(src_path))
            lines = self.convert(ann, meta)
            merged_lines.extend(lines)
        self.write_target(merged_lines, dst_path)
        return str(dst_path)

    def _check_meta(self, meta: Union[Dict[str, Any], "sly.ProjectMeta"]) -> "sly.ProjectMeta":
        import supervisely as sly

        if isinstance(meta, sly.ProjectMeta):
            return meta
        if isinstance(meta, dict):
            return sly.ProjectMeta.from_json(meta)

    def _prepare_class_names(self, meta: Union[Dict[str, Any], "sly.ProjectMeta"]) -> List[str]:
        meta = self._check_meta(meta)
        if meta is not None:
            return [obj_class.name for obj_class in meta.obj_classes]

    def _sly_label_to_yolo_lines(
        self,
        label: "sly.Label",
        img_height: int,
        img_width: int,
        class_names: List[str],
        task_type: Literal["detect", "segment", "pose"],
    ) -> List[str]:
        """
        Convert the Supervisely Label to a line in the YOLO format.
        """

        labels = convert_sly_geometry_if_needed(label, task_type)
        class_idx = class_names.index(label.obj_class.name)

        lines = []
        for label in labels:
            if task_type == YOLOTaskType.DETECT:
                yolo_line = sly_rectangle_to_yolo_line(
                    class_idx=class_idx,
                    geometry=label.geometry,
                    img_height=img_height,
                    img_width=img_width,
                )
            elif task_type == YOLOTaskType.SEGMENT:
                yolo_line = sly_polygon_to_yolo_line(
                    class_idx=class_idx,
                    geometry=label.geometry,
                    img_height=img_height,
                    img_width=img_width,
                )
            elif task_type == YOLOTaskType.POSE:
                nodes_field = label.obj_class.geometry_type.items_json_field
                max_kpts_count = len(label.obj_class.geometry_config[nodes_field])
                yolo_line = sly_keypoints_to_yolo_line(
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


class YoloToSlyConverter(AnnotationConverter):
    """Convert YOLO annotation files into Supervisely format."""

    source_format = AnnotationFormat.YOLO
    target_format = AnnotationFormat.SLY
    output_suffix = ".json"

    @property
    def _applicable_geometries_map(self):
        return {
            is_applicable_for_rectangles: sly.Rectangle,
            is_applicable_for_polygons: sly.Polygon,
            is_applicable_for_keypoints: sly.GraphNodes,
        }

    @property
    def _geometry_converters(self):
        return {
            sly.Rectangle: convert_rectangle,
            sly.Polygon: convert_polygon,
            sly.GraphNodes: convert_keypoints,
        }

    def get_geometry(
        self,
        geometry_type: Union["sly.Rectangle", "sly.Polygon", "sly.GraphNodes", "sly.AnyGeometry"],
        img_height: int,
        img_width: int,
        with_keypoint: bool,
        num_keypoints: int,
        num_dims: int,
        coords: List[float],
    ) -> Union["sly.Rectangle", "sly.Polygon", "sly.GraphNodes", None]:
        """
        Get the geometry object based on the geometry type.
        """
        if geometry_type not in self._geometry_converters:
            geometry_type = self.detect_geometry(
                coords=coords,
                with_keypoint=with_keypoint,
                num_keypoints=num_keypoints,
                num_dims=num_dims,
            )

        if geometry_type is None:
            return None

        return self._geometry_converters[geometry_type](
            img_height=img_height,
            img_width=img_width,
            coords=coords,
            num_keypoints=num_keypoints,
            num_dims=num_dims,
        )

    def detect_geometry(
        self,
        coords: List[float],
        with_keypoint: bool,
        num_keypoints: int,
        num_dims: int,
    ) -> Union["sly.Rectangle", "sly.Polygon", "sly.GraphNodes", None]:
        """
        Detect the geometry type based on the coordinates and the configuration.
        """
        for geometry, is_applicable in self._applicable_geometries_map.items():
            if is_applicable(
                with_keypoint=with_keypoint,
                num_keypoints=num_keypoints,
                num_dims=num_dims,
                coords=coords,
            ):
                return geometry

    def read_source(self, path: Path) -> List[str]:
        import supervisely as sly

        with open(path, "r") as ann_file:
            lines = ann_file.readlines()
            return [line.strip() for line in lines]

    def write_target(self, ann: "sly.Annotation", path: Path) -> None:
        import supervisely as sly

        sly.json.dump_json_file(ann.to_json(), str(path))

    def convert_file(self, src: Path | str, dst_dir: Path | str) -> str:
        yolo_lines = self.read_source(Path(src))
        sly_ann = self.convert(yolo_lines)
        destination_path = Path(dst_dir) / f"{Path(src).stem}.json"
        self.write_target(sly_ann, destination_path)
        return str(destination_path)

    def convert(self, ann: List[str], img_height, img_width) -> "sly.Annotation":
        import supervisely as sly

        labels = []

        for line in ann:
            parts = line.strip().split()
            if len(parts) > 0:
                class_index, coords = get_coordinates(parts)
                for func, geometry_type in self._applicable_geometries_map.items():
                    if func(
                        with_keypoint=True,
                        num_keypoints=None,
                        num_dims=None,
                        coords=coords,
                    ):
                        break
                else:
                    geometry_type = None

                if geometry_type is None:
                    continue

                obj_class = sly.ObjClass(name=str(class_index), geometry_type=geometry_type)

                geometry = self.get_geometry(
                    geometry_type=geometry_type,
                    img_height=img_height,
                    img_width=img_width,
                    with_keypoint=False,
                    num_keypoints=0,
                    num_dims=0,
                    coords=coords,
                )

                if geometry is not None:
                    label = sly.Label(geometry=geometry, obj_class=obj_class)
                    labels.append(label)

        return sly.Annotation(img_size=(img_height, img_width), labels=labels)

    def convert_files(self, source_files, dst_path, img_height, img_width) -> str:
        dst_dir = Path(dst_path)
        dst_dir.parent.mkdir(parents=True, exist_ok=True)

        merged_labels: List["sly.Label"] = []
        for src_path in source_files:
            yolo_lines = self.read_source(Path(src_path))
            sly_ann = self.convert(yolo_lines, img_height, img_width)
            merged_labels.extend(sly_ann.labels)
        merged_ann = sly.Annotation(img_size=(img_height, img_width), labels=merged_labels)
        self.write_target(merged_ann, dst_path)
        return str(dst_path)


# __all__ = ["SlyToYoloConverter", "YoloToSlyConverter"]
