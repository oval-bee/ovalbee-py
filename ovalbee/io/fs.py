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
