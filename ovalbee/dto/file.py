import asyncio
import datetime
import enum
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Optional

from pydantic import Field, field_serializer, model_validator

from ovalbee.dto.base import BaseInfo
from ovalbee.dto.s3 import S3Object
from ovalbee.io.fs import get_file_ext
from ovalbee.io.url import parse_s3_url

if TYPE_CHECKING:
    from ovalbee.api.api import Api


class FileType(str, enum.Enum):
    INTERNAL = "internal"
    EXTERNAL = "external"


class FileInfo(BaseInfo):
    key: Optional[str] = Field(default=None, description="Name of the file")
    type: FileType = Field(default=FileType.INTERNAL, description="Type of the file uploading")
    url: Optional[str] = Field(default=None, description="URL of the file")

    @field_serializer("key")
    def serialize_metadata(self, key: Optional[str]) -> Optional[str]:
        """Auto-set key field when serializing"""
        if key is None:
            _, key = parse_s3_url(self.url)
        return key

    @model_validator(mode="before")
    def extract_format_from_url(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """Get key from url when loading the model"""
        url = values.get("url")
        key = values.get("key")
        if url is not None and key is None:
            _, parsed_key = parse_s3_url(url)
            values["key"] = parsed_key
        return values

    def download(
        self, api: "Api", save_dir: str = None, prefix: str = None, suffix: str = None
    ) -> str:
        """Download resource file and return path to it"""
        bucket, key = parse_s3_url(self.url)
        prefix = prefix or f"File_{Path(self.key).name}_"
        if "/" in prefix:
            prefix = prefix.replace("/", "_")
        suffix = suffix or get_file_ext(self.url)
        temp_file = tempfile.NamedTemporaryFile(
            prefix=prefix, suffix=suffix, dir=save_dir, delete=False
        )
        file_path = temp_file.name
        api.storage.download(key=key, bucket=bucket, file_path=file_path)
        return file_path

    async def download_async(self, api: "Api", save_dir: str = None, prefix: str = None):
        bucket, key = parse_s3_url(self.url)
        prefix = prefix or f"File_{Path(self.key).name}_"
        temp_file = tempfile.NamedTemporaryFile(prefix=prefix, dir=save_dir, delete=False)
        file_path = temp_file.name
        await api.storage.download_async(key=key, bucket=bucket, file_path=file_path)
        return file_path

    @classmethod
    def from_s3_object(cls, s3_object: S3Object) -> "FileInfo":
        """Create FileInfo from S3Object"""
        bucket, key = parse_s3_url(s3_object.key)
        return cls(
            key=key,
            url=f"s3://{bucket}/{key}",
            metadata={
                "size": s3_object.size,
                "last_modified": (
                    s3_object.last_modified.isoformat()
                    if isinstance(s3_object.last_modified, datetime.datetime)
                    else s3_object.last_modified
                ),
                "etag": s3_object.etag,
                "storage_class": s3_object.storage_class,
            },
            type=FileType.INTERNAL,
        )
