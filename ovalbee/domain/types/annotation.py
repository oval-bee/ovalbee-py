from typing import Any, List

from pydantic import Field

from ovalbee.domain.types.base import BaseInfo


class AnnotationInfo(BaseInfo):
    data: List[Any] = Field(default_factory=list, description="List of annotation data")
