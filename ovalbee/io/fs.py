import errno
import os
from pathlib import Path
from typing import Iterator, List, Union


def iter_files(dir_path: Union[str, Path], recursive: bool = False) -> Iterator[str]:
    dir_path = Path(dir_path)
    if not dir_path.exists():
        raise FileNotFoundError(f"Directory not found: {dir_path}")
    if not dir_path.is_dir():
        raise NotADirectoryError(f"Not a directory: {dir_path}")
    if recursive:
        yield from (str(p) for p in dir_path.rglob("*") if p.is_file())
    else:
        yield from (str(p) for p in dir_path.iterdir() if p.is_file())


def list_files(dir_path: Union[str, Path], recursive: bool = False) -> List[str]:
    return list(iter_files(dir_path, recursive))


def silent_remove(file_path: str) -> None:
    """
    Remove file which may not exist.

    :param file_path: File path.
    :type file_path: str
    :returns: None
    :rtype: :class:`NoneType`
    :Usage example:

     .. code-block:: python

        from supervisely.io.fs import silent_remove
        silent_remove('/home/admin/work/projects/examples/1.jpeg')
    """
    try:
        os.remove(file_path)
    except OSError as e:
        if e.errno != errno.ENOENT:  # errno.ENOENT = no such file or directory
            raise


def get_file_name(path: str) -> str:
    """
    Extracts file name from a given path.

    :param path: Path to file.
    :type path: str
    :returns: File name without extension
    :rtype: :class:`str`
    :Usage example:

     .. code-block::

        import supervisely as sly

        file_name = sly.fs.get_file_name("/home/admin/work/projects/lemons_annotated/ds1/img/IMG_0748.jpeg")

        print(file_name)
        # Output: IMG_0748
    """
    return os.path.splitext(os.path.basename(path))[0]


def get_file_ext(path: str) -> str:
    """
    Extracts file extension from a given path.

    :param path: Path to file.
    :type path: str
    :returns: File extension without name
    :rtype: :class:`str`
    :Usage example:

     .. code-block::

        import supervisely as sly

        file_ext = sly.fs.get_file_ext("/home/admin/work/projects/lemons_annotated/ds1/img/IMG_0748.jpeg")

        print(file_ext)
        # Output: .jpeg
    """
    return os.path.splitext(os.path.basename(path))[1]


def get_file_name_with_ext(path: str) -> str:
    """
    Extracts file name with ext from a given path.

    :param path: Path to file.
    :type path: str
    :returns: File name with extension
    :rtype: :class:`str`
    :Usage example:

     .. code-block::

        import supervisely as sly

        file_name_ext = sly.fs.get_file_name_with_ext("/home/admin/work/projects/lemons_annotated/ds1/img/IMG_0748.jpeg")

        print(file_name_ext)
        # Output: IMG_0748.jpeg
    """
    return os.path.basename(path)
