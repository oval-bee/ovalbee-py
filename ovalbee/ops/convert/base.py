from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Iterable, List, Sequence, Union

from ovalbee.dto.annotation import AnnotationFormat


class AnnotationConverter(ABC):
    """Base class for converting annotation files between formats."""

    #: Source annotation format the converter expects.
    source_format: AnnotationFormat
    #: Target annotation format the converter produces.
    target_format: AnnotationFormat
    #: Suffix for output files produced by the converter.
    output_suffix: str = ""

    @abstractmethod
    def convert(self, ann: Any, *args, **kwargs) -> Any:
        """Convert the annotation from source format to target format."""

    @abstractmethod
    def read_source(self, path: Path) -> Any:
        """Read the source annotation from a file."""

    @abstractmethod
    def write_target(self, ann: Any, path: Path) -> None:
        """Write the converted annotation to a file."""

    @abstractmethod
    def convert_files(
        self,
        source_files: Sequence[Union[str, Path]],
        dst_path: Union[str, Path],
        *args,
        **kwargs,
    ) -> str:
        """
        Convert multiple annotation files.
        * Note: All files (resources) must correspond to single Annotation object.
        """

    @abstractmethod
    def convert_file(self, src: Path | str, dst_dir: Path | str) -> str:
        """
        Convert a single annotation file.

        Implementations are expected to write the converted file(s) into
        ``dst`` and return the resulting paths.
        """

    def __repr__(self) -> str:  # pragma: no cover - trivial
        return (
            f"{self.__class__.__name__}"
            f"(source_format={self.source_format}, target_format={self.target_format})"
        )


__all__ = ["AnnotationConverter"]
