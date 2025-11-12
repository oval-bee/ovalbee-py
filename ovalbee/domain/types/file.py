import asyncio
import enum
import tempfile
from typing import Any, Dict, Optional

from pydantic import Field, field_serializer, model_validator

from ovalbee.api.api import Api
from ovalbee.domain.types.base import BaseInfo
from ovalbee.io.url import parse_s3_url


class FileType(str, enum.Enum):
    INTERNAL = "internal"
    EXTERNAL = "external"


class FileInfo(BaseInfo):
    key: Optional[str] = Field(default=None, description="Name of the file")
    type: FileType = Field(default=FileType.INTERNAL, description="Type of the file uploading")
    url: Optional[str] = Field(default=None, description="URL of the file")

    @field_serializer("key")
    def serialize_metadata(self, v: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Auto-set key field when serializing"""
        if v is None:
            v = {}
        if self.key is not None:
            v["key"] = self.key
        else:
            _, key = parse_s3_url(self.url)
            v["key"] = key
        return v

    @model_validator(mode="before")
    def extract_format_from_url(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """Get key from url when loading the model"""
        url = values.get("url")
        key = values.get("key")
        if url is not None and key is None:
            _, parsed_key = parse_s3_url(url)
            values["key"] = parsed_key
        return values

    def download(self, api: Api, save_dir: str = None, prefix: str = None) -> str:
        """Download resource file and return path to it"""
        bucket, key = parse_s3_url(self.url)
        prefix = prefix or f"File_{self.key}_"
        temp_file = tempfile.NamedTemporaryFile(prefix=prefix, dir=save_dir, delete=False)
        file_path = temp_file.name
        api.storage.download(key=key, bucket=bucket, file_path=file_path)
        return file_path

    async def download_async(self, api: Api, save_dir: str = None, prefix: str = None):
        bucket, key = parse_s3_url(self.url)
        prefix = prefix or f"File_{self.key}_"
        temp_file = tempfile.NamedTemporaryFile(prefix=prefix, dir=save_dir, delete=False)
        file_path = temp_file.name
        await api.storage.download_async(key=key, bucket=bucket, file_path=file_path)
        return file_path
