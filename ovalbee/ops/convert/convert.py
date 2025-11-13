from collections import deque
from typing import List

from ovalbee.dto.annotation import AnnotationFormat


def convert_sly_to_yolo(files: List[str], save_dir: str) -> List[str]:
    return []


def convert_yolo_to_sly(files: List[str], save_dir: str) -> List[str]:
    return []


converters = {
    (AnnotationFormat.SLY, AnnotationFormat.YOLO): convert_sly_to_yolo,
    (AnnotationFormat.YOLO, AnnotationFormat.SLY): convert_yolo_to_sly,
}


def find_convert_chain(from_format, to_format):
    if from_format == to_format:
        return []

    queue = deque([(from_format, [])])
    visited = {from_format}

    while queue:
        current_format, path = queue.popleft()

        for (source, target), converter_func in converters.items():
            if source == current_format and target not in visited:
                new_path = path + [converter_func]

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
    from_format: AnnotationFormat,
    to_format: AnnotationFormat,
    save_dir: str,
) -> List[str]:
    converters_chain = find_convert_chain(from_format, to_format)
    for converter in converters_chain:
        files = converter(files, save_dir)
    return files
