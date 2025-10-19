from typing import List

from ovalbee.domain.types.base import BaseInfo
from ovalbee.domain.types.collection import CollectionInfo


class SpaceInfo(BaseInfo):
    collections: List[CollectionInfo] = []
