"""
datatypes.py

Custom type definitions
"""

import itertools
from enum import Enum
from dataclasses import dataclass, field
from typing import List, Callable


class Unit(Enum):
    SECONDS = 1
    MINUTES = 2
    HOURS = 3
    DAYS = 4


class Priority(Enum):
    LOW = 1
    MEDIUM = 2
    HIGH = 3

    def __lt__(self, other):
        if self.__class__ is other.__class__:
            return self.value < other.value
        return NotImplemented


@dataclass
class Entity:
    entity_type: str
    entity_id: int = field(default_factory=itertools.count().__next__)
    attr: dict = field(default_factory=dict)
