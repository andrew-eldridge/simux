"""
datatypes.py

Custom type definitions
"""

import heapq
from itertools import count
from enum import Enum
from dataclasses import dataclass, field
from typing import List, Callable, Optional


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
    entity_id: int = field(default_factory=count().__next__)
    attr: dict = field(default_factory=dict)


@dataclass
class Event:
    event_time: float
    event_name: str
    event_handler: Callable
    event_entity: Entity
    attr: dict = field(default_factory=dict)

    def __str__(self):
        return f'Event: {self.event_name}\nTime: {self.event_time}'


@dataclass
class Resource:
    name: str
    capacity: int
    queue: List[tuple[float, int, int, Entity, Callable]] = field(default_factory=list)

    def __post_init__(self):
        self.available = self.capacity

    def queue_entity(self, entity: Entity, num_resources: int, queue_entry_time: float, event_handler: Callable):
        heapq.heappush(self.queue, (queue_entry_time, num_resources, entity.entity_id, entity, event_handler))

    def seize(self, num_resources: int):
        self.available -= num_resources

    def release(self, num_resources: int, release_time: float) -> Optional[Event]:
        """
        Releases given number of resources and, if possible, reassigns resources to next entity in queue

        :param num_resources: number of resources to release
        :param release_time: system time at release event
        """

        # release given number of resources
        if num_resources > self.capacity - self.available:
            raise ValueError('ERROR: tried releasing resources in excess of capacity', self.name, self.capacity, self.available, num_resources)
        self.available += num_resources

        # check if there are enough resources for next entity in queue
        if len(self.queue) != 0:
            _, required_resources, _, next_entity, event_handler = self.queue[0]
            if required_resources <= self.available:
                self.available -= required_resources
                heapq.heappop(self.queue)
                return Event(
                    event_time=release_time,
                    event_name=f'{next_entity.entity_type} {next_entity.entity_id} entity seized {required_resources} {self.name} resources',
                    event_handler=event_handler,
                    event_entity=next_entity
                )
        return None
