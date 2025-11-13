from datetime import datetime
from typing import Optional, Union

from pydantic import BaseModel, Field, field_validator


class S3Object(BaseModel):
    """S3 object metadata"""

    key: str = Field(..., description="Key of the S3 object", alias="Key")
    size: int = Field(..., description="Size of the S3 object in bytes", alias="Size")
    last_modified: Union[str | datetime] = Field(
        ..., description="Last modified timestamp", alias="LastModified"
    )
    etag: str = Field(..., description="ETag of the S3 object", alias="ETag")
    storage_class: Optional[str] = Field(
        default=None, description="Storage class of the S3 object", alias="StorageClass"
    )

    @field_validator("last_modified")
    def validate_last_modified(cls, v):
        if isinstance(v, str):
            try:
                return datetime.fromisoformat(v.replace("Z", "+00:00"))
            except ValueError:
                raise ValueError("last_modified must be a valid ISO 8601 datetime string")
        return v
