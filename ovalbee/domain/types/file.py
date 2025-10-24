import enum
from typing import Optional

from pydantic import Field

from ovalbee.domain.types.base import BaseInfo


class FileType(str, enum.Enum):
    INTERNAL = "internal"
    EXTERNAL = "external"


class FileInfo(BaseInfo):
    key: Optional[str] = Field(default=None, description="Name of the file")
    type: FileType = Field(default=FileType.INTERNAL, description="Type of the file uploading")
    url: Optional[str] = Field(default=None, description="URL of the file")
