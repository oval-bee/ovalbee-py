from typing import List

from pydantic.dataclasses import dataclass

from ovalbee.domain.types.asset import CollectionInfo
from ovalbee.domain.types.base import BaseInfo


@dataclass
class SpaceInfo(BaseInfo):
    collections: List[CollectionInfo] = []
