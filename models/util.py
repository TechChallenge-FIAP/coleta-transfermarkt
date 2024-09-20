from dataclasses import dataclass
from typing import Generic, Type, TypeVar

from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)


@dataclass
class SoccerInfo(Generic[T]):
    id: str
    schema: Type[T]
    endpoint: str
