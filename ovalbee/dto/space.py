from typing import List

from ovalbee.dto.base import BaseInfo
from ovalbee.dto.collection import CollectionInfo


class SpaceInfo(BaseInfo):
    collections: List[CollectionInfo] = []
