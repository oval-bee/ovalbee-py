"""
Public package interface for the Ovalbee SDK.

The module exposes a light facade (`Ovalbee`, `login`, `space`, `collection`)
that will be expanded as the architectural skeleton gains concrete behavior.
"""

from __future__ import annotations

from typing import Optional

# from ovalbee.api import api  # Existing stub preserved for backwards compatibility.
from ovalbee.api.api import Api
from ovalbee.dto.annotation import (
    Annotation,
    AnnotationFormat,
    AnnotationResource,
    AnnotationTask,
)
from ovalbee.dto.asset import AssetInfo, AssetType
from ovalbee.dto.collection import CollectionInfo
from ovalbee.dto.file import FileInfo, FileType
from ovalbee.dto.space import SpaceInfo

# from ovalbee.client import ClientConfig, Ovalbee, login
from ovalbee.io.url import parse_s3_url

# __all__ = [
#     "Api",
#     "Annotation",
#     "AssetInfo",
#     "CollectionInfo",
#     "FileInfo",
#     "SpaceInfo",
#     "AssetType",
#     "AnnotationFormat",
#     "AnnotationResource",
#     "AnnotationTask",
#     "FileType",
# ]
