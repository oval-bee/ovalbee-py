import tracemalloc
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


COUNT = 30000


def measure_memory(func):
    tracemalloc.start()
    func()
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return current, peak


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


# Memory usage tests
current, peak = measure_memory(test_pydantic_model)
print(f"Pydantic Model - Current: {current / 1024 / 1024:.2f} MB, Peak: {peak / 1024 / 1024:.2f} MB")

current, peak = measure_memory(test_namedtuple)
print(f"NamedTuple - Current: {current / 1024 / 1024:.2f} MB, Peak: {peak / 1024 / 1024:.2f} MB")
