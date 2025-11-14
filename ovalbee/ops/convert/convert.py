from collections import deque
import tempfile
from pathlib import Path
from typing import List

from ovalbee.dto.annotation import AnnotationFormat
from ovalbee.io.fs import silent_remove
from ovalbee.ops.convert.yolo import SlyToYoloConverter, YoloToSlyConverter
from ovalbee.ops.convert.base import AnnotationConverter


def convert_sly_to_yolo(files: List[str], save_dir: str) -> List[str]:
    return []


def convert_yolo_to_sly(files: List[str], save_dir: str) -> List[str]:
    return []


converters = {
    (AnnotationFormat.SLY, AnnotationFormat.YOLO): SlyToYoloConverter,
    (AnnotationFormat.YOLO, AnnotationFormat.SLY): YoloToSlyConverter,
}


def find_convert_chain(from_format, to_format) -> List[AnnotationConverter]:
    if from_format == to_format:
        return []

    queue = deque([(from_format, [])])
    visited = {from_format}

    while queue:
        current_format, path = queue.popleft()

        for (source, target), converter in converters.items():
            if source == current_format and target not in visited:
                new_path = path + [converter]

                if target == to_format:
                    return new_path

                queue.append((target, new_path))
                visited.add(target)

    raise NotImplementedError(
        f"No conversion chain found from {from_format.value} to {to_format.value}"
    )


def can_convert(from_format: AnnotationFormat, to_format: AnnotationFormat):
    try:
        find_convert_chain(from_format, to_format)
        return True
    except NotImplementedError:
        return False


def convert(
    files: List[StopIteration],
    from_format: AnnotationFormat | str,
    to_format: AnnotationFormat | str,
    save_path: str | Path = None,
    img_height: int = None,
    img_width: int = None,
) -> str:
    if save_path is None:
        save_path = Path(tempfile.NamedTemporaryFile(
            prefix=f"converted_{from_format}_to_{to_format}_",
            suffix="",
            delete=False,
        ).name)
    converters_chain = find_convert_chain(from_format, to_format)
    for converter_cls in converters_chain:
        converter = converter_cls()
        tmp_file = converter.convert_files(files, save_path, img_height=img_height, img_width=img_width)
        files = [tmp_file]
    return files[0]
