import asyncio
from abc import ABC, abstractmethod
from typing import Any, AsyncGenerator, Generator, List, Optional

import requests
from pydantic import BaseModel


def _get_single_item(items):
    """_get_single_item"""
    if len(items) == 0:
        return None
    if len(items) > 1:
        raise RuntimeError("There are several items with the same name")
    return items[0]


class ModuleApiTemplate(ABC):
    def __init__(self, api_client):
        self.api_client = api_client

    def _endpoint_prefix(self) -> str:
        pass

    @staticmethod
    @abstractmethod
    def _info_class():
        pass

    def _creation_endpoint_name(self) -> str:
        pass

    def _create_field_name(self) -> str:
        pass

    @abstractmethod
    def get_list(self) -> List[Any]:
        pass

    @abstractmethod
    def create(self, item: Any) -> Any:
        pass

    @abstractmethod
    def create_bulk(self, items: List[Any]) -> List[Any]:
        pass

    @abstractmethod
    def get_info_by_id(self, space_id: int, id: int) -> Optional[Any]:
        pass


class ModuleApi(ModuleApiTemplate):

    def __init__(self, api: "Api"):
        self._api = api

    def _get_response_by_id(self, space_id, id, method):
        """_get_response_by_id"""
        try:
            params = {"workspaceId": space_id}
            method = f"{method.rstrip('/')}/{id}"
            return self._api.get(method=method, params=params)
        except requests.exceptions.HTTPError as error:
            if error.response.status_code == 404:
                return None
            else:
                raise error

    def _get_info_by_id(self, space_id, id):
        """_get_info_by_id"""
        response = self._get_response_by_id(space_id, id, self._endpoint_prefix())
        return self._info_class()(**response.json()) if response is not None else None

    def _get_list_all_pages(self, space_id: int) -> List[Any]:
        """_get_list_all_pages"""
        # TODO: implement pagination
        # for now, we assume that all items fit in a single page
        return list(self._get_list_all_pages_generator(space_id))

    def _get_list_all_pages_generator(self, space_id: int) -> Generator[Any, None, None]:
        """_get_list_all_pages_generator"""
        # TODO: implement pagination
        # for now, we assume that all items fit in a single page
        params = {"workspaceId": space_id}
        resp = self._api.get(method=self._endpoint_prefix(), params=params)
        resp_json = resp.json()
        if "items" in resp_json:
            resp_json = resp_json["items"]
        for item in resp_json:
            yield self._info_class()(**item)

    def _create_bulk(self, items: List[BaseModel]) -> List[Any]:
        """_create_bulk"""
        data = {}
        workspace_id = items[0].workspace_id if items else None  # temporary
        items = [item.model_dump() for item in items]
        if self._create_field_name():
            data[self._create_field_name()] = items
        else:
            data = items

        method = self._endpoint_prefix().rstrip("/")
        if self._creation_endpoint_name():
            method += f"/{self._creation_endpoint_name()}"
        resp = self._api.post(method, data=data)
        resp_json = resp.json()
        # TODO: update response handling / change API to return full objects
        ids = [item.get("id") for item in resp_json]
        return [self.get_info_by_id(space_id=workspace_id, id=item_id) for item_id in ids]


class UpdatableModuleApi(ModuleApi):

    @staticmethod
    def _endpoint_name_update() -> str:
        pass

    def update(self, item: Any) -> Any:
        pass

    def _update(self, item: BaseModel) -> List[Any]:
        """_update"""
        if item.id is None:
            raise ValueError("Item must have an ID for update")
        data = item.model_dump(exclude_unset=True)
        method = f"{self._endpoint_prefix()}/{item.id}"
        resp = self._api.put(method, data=data)
        resp_json = resp.json()
        return self._info_class()(**resp_json)


class DeletableModuleApi(ModuleApi):

    @staticmethod
    def _endpoint_name_delete() -> str:
        pass

    def delete(self, id: int) -> None:
        pass

    def _delete(self, id: int) -> None:
        """_delete"""
        method = f"{self._endpoint_prefix()}/{id}"
        self._api.delete(method)


class CRUDModuleApi(UpdatableModuleApi, DeletableModuleApi):
    pass
