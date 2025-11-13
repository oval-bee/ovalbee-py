from __future__ import annotations

from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Generator,
    Iterable,
    List,
    Optional,
    Type,
    TypeVar,
    Union,
)

import requests
from pydantic import BaseModel

if TYPE_CHECKING:
    from ovalbee.api.api import Api


TInfo = TypeVar("TInfo", bound=BaseModel)


@dataclass
class ListPage(Iterable[TInfo]):
    items: List[TInfo]
    next_page_token: Optional[str] = None
    total: Optional[int] = None

    def __iter__(self) -> Generator[TInfo, None, None]:
        yield from self.items


class ModuleApi:
    """Base class for concrete API clients."""

    def __init__(self, api: "Api"):
        self._api = api

    def _endpoint_prefix(self) -> str:
        raise NotImplementedError()

    @property
    def endpoint(self) -> str:
        return self._endpoint_prefix().rstrip("/")


class CreateableModuleApi(ModuleApi):
    """Mixin with helpers for create operations."""

    support_bulk_creation: bool = True

    def _creation_endpoint_name(self) -> Optional[str]:
        return None

    def _create_field_name(self) -> Optional[str]:
        return None

    def create(self, item: BaseModel) -> BaseModel:
        created = self._create_bulk([item])
        if not created:
            raise RuntimeError("Create operation returned no items.")
        return created[0]

    def create_bulk(self, items: List[BaseModel]) -> List[BaseModel]:
        return self._create_bulk(items, support_bulk=self.support_bulk_creation)

    def _create_bulk(self, items: List[BaseModel]) -> List[BaseModel]:
        if not items:
            if self.support_bulk_creation:
                return []
            else:
                return None

        if not self.support_bulk_creation and len(items) > 1:
            raise ValueError("Bulk creation is not supported for this module.")
        if self.support_bulk_creation:
            payload_items = [self._serialize_model(item) for item in items]
        else:
            payload_items = self._serialize_model(items[0])
        data: Union[List[Dict[str, Any]], Dict[str, Any]]
        field_name = self._create_field_name()
        if field_name:
            data = {field_name: payload_items}
        else:
            data = payload_items

        method = self.endpoint
        creation_endpoint = self._creation_endpoint_name()
        if creation_endpoint:
            method = f"{method}/{creation_endpoint}"

        response = self._api.post(method, data=data)
        response_data = response.json()
        return self._coerce_created_items(response_data, items)

    def _serialize_model(self, item: BaseModel) -> Dict[str, Any]:
        return item.model_dump(by_alias=True, exclude_none=True)

    def _coerce_created_items(
        self,
        response_data: Any,
        source_items: List[BaseModel],
    ) -> List[BaseModel]:
        raw_items = self._unwrap_collection(response_data)
        coerced: List[BaseModel] = []

        for idx, raw in enumerate(raw_items):
            source = source_items[min(idx, len(source_items) - 1)]
            if not hasattr(self, "_coerce_to_model"):
                coerced.append(raw)
                continue
            space_id = getattr(source, "space_id", None)
            source_id = getattr(source, "source_id", None)  # ! temp workaround:
            coerced.append(self._coerce_to_model(raw, space_id=space_id, source_id=source_id))
        return coerced


class RetrievableModuleApi(ModuleApi):
    """Mixin with helpers for read operations."""

    @staticmethod
    def _info_class() -> Type[TInfo]:
        raise NotImplementedError()

    def get_info_by_id(self, space_id: int, id: Union[int, str]) -> Optional[BaseModel]:
        response = self._get_response_by_id(space_id, id)
        if response is None:
            return None
        return self._coerce_to_model(response.json())

    def list_page(
        self,
        space_id: int,
        *,
        item_type: Optional[str] = None,
        source_id: Union[str, List[str], None] = None,
        page_size: Optional[int] = None,
        page_token: Optional[str] = None,
    ) -> ListPage:
        params: Dict[str, Any] = {"workspaceId": space_id}
        if item_type is not None:
            params["type"] = item_type
        if source_id is not None:
            params["sourceId"] = source_id
        if page_size is not None:
            params["pageSize"] = page_size
        if page_token is not None:
            params["pageToken"] = page_token

        response = self._api.get(method=self.endpoint, params=params)
        payload = response.json()
        items_data = self._unwrap_collection(payload)
        items = [self._coerce_to_model(item) for item in items_data]
        next_token = None
        total = None
        if isinstance(payload, dict):
            next_token = payload.get("continuationToken")
            total = payload.get("total") or len(items)
        return ListPage(items=items, next_page_token=next_token, total=total)

    def iter_list(
        self,
        space_id: int,
        *,
        item_type: Optional[str] = None,
        source_id: Union[str, List[str], None] = None,
        page_size: Optional[int] = None,
    ) -> Generator[BaseModel, None, None]:
        page_token: Optional[str] = None
        while True:
            page = self.list_page(
                space_id,
                item_type=item_type,
                source_id=source_id,
                page_size=page_size,
                page_token=page_token,
            )
            for item in page.items:
                yield item
            if page.next_page_token is None:
                break
            page_token = page.next_page_token

    def get_list(
        self,
        space_id: int,
        *,
        item_type: Optional[str] = None,
        source_id: Union[str, List[str], None] = None,
    ) -> List[BaseModel]:
        return list(
            self.iter_list(space_id, item_type=item_type, source_id=source_id, page_size=None)
        )

    def _get_response_by_id(self, space_id: int, id: Union[int, str]):
        try:
            params = {"workspaceId": space_id}
            method = f"{self.endpoint}/{id}"
            return self._api.get(method=method, params=params)
        except requests.exceptions.HTTPError as error:
            if error.response.status_code == 404:
                return None
            raise

    def _unwrap_collection(self, data: Any) -> List[Any]:
        if isinstance(data, dict):
            if "items" in data and isinstance(data["items"], list):
                return data["items"]
            if "data" in data and isinstance(data["data"], list):
                return data["data"]
            return [data]
        if isinstance(data, list):
            return data
        if data is None:
            return []
        return [data]

    def _coerce_to_model(self, data: Any, space_id: Optional[int] = None, source_id: Optional[int] = None) -> BaseModel:
        info_cls = self._info_class()
        if isinstance(data, info_cls):
            return data
        if isinstance(data, dict):
            if len(data.keys()) == 1 and "id" in data:
                return self._refetch(space_id, data["id"])
            return info_cls(space_id=space_id, source_id=source_id, **data)
        if isinstance(data, str):
            return self._refetch(space_id, data)
        raise TypeError(f"Cannot convert response item to {info_cls.__name__}: {data!r}")

    def _refetch(self, space_id: Optional[int], id_value: Union[str, int]) -> BaseModel:
        if space_id is None:
            raise ValueError(
                "Cannot re-fetch item: space_id is required when response only returns IDs."
            )
        refreshed = self.get_info_by_id(space_id=space_id, id=id_value)
        if refreshed is None:
            raise RuntimeError(f"Item with id={id_value!r} not found after creation.")
        return refreshed


class UpdatableModuleApi(RetrievableModuleApi):
    """Mixin with helpers for update operations."""

    def _endpoint_name_update(self) -> Optional[str]:
        return None

    def update(self, item: BaseModel) -> BaseModel:
        return self._update(item)

    def _update(self, item: BaseModel) -> BaseModel:
        if getattr(item, "id", None) is None:
            raise ValueError("Item must have an ID for update.")

        payload = item.model_dump(by_alias=True, exclude_none=True, exclude_unset=True)
        method = self.endpoint
        update_endpoint = self._endpoint_name_update()
        if update_endpoint:
            method = f"{method}/{update_endpoint}"
        method = f"{method}/{item.id}"

        response = self._api.put(method, data=payload)
        body = response.json()
        # Reuse parsing pipeline to ensure consistent models
        items = self._unwrap_collection(body)
        if items:
            return self._coerce_to_model(items[0])
        return self._coerce_to_model(body)


class DeletableModuleApi(ModuleApi):
    """Mixin with helpers for delete operations."""

    def _endpoint_name_delete(self) -> Optional[str]:
        return None

    def delete(self, id: Union[int, str]) -> None:
        self._delete(id)

    def _delete(self, id_value: Union[int, str]) -> None:
        method = self.endpoint
        delete_endpoint = self._endpoint_name_delete()
        if delete_endpoint:
            method = f"{method}/{delete_endpoint}"
        method = f"{method}/{id_value}"
        self._api.delete(method)


class CRUDModuleApi(
    CreateableModuleApi,
    UpdatableModuleApi,
    DeletableModuleApi,
):
    """Full CRUD mixin set."""

    pass
