import timeit
from collections import namedtuple
from dataclasses import asdict, dataclass
from typing import TypedDict

from pydantic import BaseModel
from pydantic import dataclasses as pydantic_dataclasses

test_data = {"x": 10, "y": 20, "label": "point1"}


PointNamedTuple = namedtuple("PointNamedTuple", ["x", "y", "label", "number"])


# TypedDict
class PointTypedDict(TypedDict):
    x: int
    y: int
    label: str
    number: int


# Standard dataclass
@dataclass
class PointDataClass:
    x: int
    y: int
    label: str
    number: int


# Pydantic dataclass
@pydantic_dataclasses.dataclass
class PointPydanticDataclass:
    x: int
    y: int
    label: str
    number: int


# Pydantic BaseModel
class PointPydanticModel(BaseModel):
    x: int
    y: int
    label: str
    number: int


COUNT = 10000


def test_namedtuple():
    objs = [PointNamedTuple(**test_data, number=i) for i in range(COUNT)]
    dicts = [obj._asdict() for obj in objs]


def test_dataclass():
    objs = [PointDataClass(**test_data, number=i) for i in range(COUNT)]
    dicts = [asdict(obj) for obj in objs]


def test_pydantic_dataclass():
    objs = [PointPydanticDataclass(**test_data, number=i) for i in range(COUNT)]
    dicts = [asdict(obj) for obj in objs]


def test_pydantic_model():
    objs = [PointPydanticModel(**test_data, number=i) for i in range(COUNT)]
    dicts = [obj.model_dump_json() for obj in objs]  # .dict() in Pydantic v1


def test_typed_dict():
    objs = [PointTypedDict(**test_data, number=i) for i in range(COUNT)]
    dicts = [obj.copy() for obj in objs]


# print("Dataclass:", timeit.timeit(test_dataclass, number=10))
# print("Pydantic Dataclass:", timeit.timeit(test_pydantic_dataclass, number=10))
print("Pydantic Model:", timeit.timeit(test_pydantic_model, number=10))
print("NamedTuple:", timeit.timeit(test_namedtuple, number=10))
# print("TypedDict:", timeit.timeit(test_typed_dict, number=10))
