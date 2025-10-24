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
