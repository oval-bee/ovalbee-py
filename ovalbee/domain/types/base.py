from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field


def _snake_to_camel(name: str) -> str:
    components = name.split("_")
    return components[0] + "".join(x.title() for x in components[1:])


class BaseInfo(BaseModel):
    id: Optional[str] = None
    metadata: Union[Dict[str, Any], None] = Field(default_factory=dict)
    # created_at: datetime = Field(default_factory=datetime.now)
    # updated_at: datetime = Field(default_factory=datetime.now)

    model_config = ConfigDict(
        populate_by_name=True,
        frozen=True,
        alias_generator=_snake_to_camel,
        serialize_by_alias=True,
        use_enum_values=True,
    )
