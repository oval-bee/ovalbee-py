from typing import Optional

from pydantic import BaseModel, Field, field_validator


class S3Object(BaseModel):
    """S3 object metadata"""

    key: str = Field(..., description="Key of the S3 object", alias="Key")
    size: int = Field(..., description="Size of the S3 object in bytes", alias="Size")
    last_modified: str = Field(..., description="Last modified timestamp", alias="LastModified")
    etag: str = Field(..., description="ETag of the S3 object", alias="ETag")
    storage_class: Optional[str] = Field(
        default=None, description="Storage class of the S3 object", alias="StorageClass"
    )
