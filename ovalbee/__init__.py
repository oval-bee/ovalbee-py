"""
Public package interface for the Ovalbee SDK.

The module exposes a light facade (`Ovalbee`, `login`, `space`, `collection`)
that will be expanded as the architectural skeleton gains concrete behavior.
"""

from __future__ import annotations

from typing import Optional

# from ovalbee.api import api  # Existing stub preserved for backwards compatibility.
from ovalbee.api.api import Api
from ovalbee.domain.types.annotation import (
    Annotation,
    AnnotationFormat,
    AnnotationResource,
    AnnotationTask,
)
from ovalbee.domain.types.asset import AssetInfo, AssetType
from ovalbee.domain.types.collection import CollectionInfo
from ovalbee.domain.types.file import FileInfo, FileType
from ovalbee.domain.types.space import SpaceInfo

# from ovalbee.client import ClientConfig, Ovalbee, login


__all__ = [
    "Api",
    "Annotation",
    "AssetInfo",
    "CollectionInfo",
    "FileInfo",
    "SpaceInfo",
    "AssetType",
    "AnnotationFormat",
    "AnnotationResource",
    "AnnotationTask",
    "FileType",
]
