"""
datatypes.py

Custom type definitions
"""

from enum import Enum
from dataclasses import dataclass, field


class Unit(Enum):
    SECONDS = 1
    MINUTES = 2
    HOURS = 3
    DAYS = 4


class Priority(Enum):
    LOW = 1
    MEDIUM = 2
    HIGH = 3


@dataclass
class Entity:
    entity_type: str
    attr: dict = field(default_factory=dict)


@dataclass
class Resource:
    name: str
    capacity: int

    def __post_init__(self):
        self.available = self.capacity

    def seize(self, num_resources: int):
        self.available -= num_resources

    def release(self, num_resources: int):
        if num_resources > self.capacity - self.available:
            print('ERROR: tried releasing resources in excess of capacity', self.name, self.capacity, self.available, num_resources)
        self.available += num_resources


@dataclass
class Event:
    event_time: float
    event_name: str
    attr: dict = field(default_factory=dict)
