from typing import Optional

from ovalbee.api._api import _Api
from ovalbee.api.annotation_api import AnnotationApi
from ovalbee.api.asset_api import AssetApi
from ovalbee.api.collection_api import CollectionApi
from ovalbee.api.storage_api import StorageApi
from ovalbee.api.task_api import TaskApi


class Api(_Api):

    def __init__(
        self,
        server_address: Optional[str] = None,
        token: Optional[str] = None,
        retry_count: Optional[int] = 10,
        retry_sleep_sec: Optional[int] = None,
    ):
        super().__init__(
            server_address=server_address,
            token=token,
            retry_count=retry_count,
            retry_sleep_sec=retry_sleep_sec,
        )

        self._storage_api = None
        self.annotation = AnnotationApi(self)
        self.asset = AssetApi(self)
        self.collection = CollectionApi(self)
        self.task = TaskApi(self)


    @property
    def storage(self) -> StorageApi:
        """Storage API client."""
        if self._storage_api is None:
            self._storage_api = StorageApi(self)
        return self._storage_api
