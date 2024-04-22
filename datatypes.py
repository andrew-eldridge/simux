"""
datatypes.py

Custom type definitions
"""

import heapq
from itertools import count
from enum import Enum
from dataclasses import dataclass, field
from typing import List, Tuple, Callable, Optional


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
    arrival_time: float
    entity_ind: int = field(default_factory=count().__next__)
    attr: dict = field(default_factory=dict)


@dataclass
class Event:
    event_time: float
    event_name: str
    event_message: str
    event_handler: Callable
    event_entity: Entity
    attr: dict = field(default_factory=dict)

    def __str__(self):
        return f'{self.event_name} Event: {self.event_message}\nTime: {self.event_time}'


@dataclass
class Resource:
    name: str
    capacity: int
    queue: List[Tuple[float, int, int, Entity, Callable]] = field(default_factory=list)
    availability_log: List[Tuple[float, int]] = field(default_factory=list)

    def __post_init__(self):
        self.available = self.capacity

    def queue_entity(self, entity: Entity, num_resources: int, queue_entry_time: float, event_handler: Callable):
        """
        Queue entity for resource

        :param entity: queued entity
        :param num_resources: number of resources required
        :param queue_entry_time: system time when entity queued
        :param event_handler: event handler of module that queued entity for resource
        """

        heapq.heappush(self.queue, (queue_entry_time, num_resources, entity.entity_ind, entity, event_handler))

    def seize(self, num_resources: int, seize_time: float):
        """
        Seize given number of resources

        :param num_resources: number of resources to seize
        :param seize_time: system time at seize event
        """

        if self.available - num_resources < 0:
            raise ValueError(f'Unable to seize {self.name} resources in excess of available. Seized: {num_resources}, Available: {self.available}.')
        self.available -= num_resources
        self.availability_log.append((seize_time, self.available))

    def release(self, num_resources: int, release_time: float) -> Optional[Event]:
        """
        Release given number of resources and, if possible, reassign resources to next entity in queue

        :param num_resources: number of resources to release
        :param release_time: system time at release event
        """

        # release given number of resources
        if num_resources > self.capacity - self.available:
            raise ValueError(f'Unable to release {self.name} resources in excess of capacity. Released: {num_resources}, Available: {self.available}, Capacity: {self.capacity}.')
        self.available += num_resources
        self.availability_log.append((release_time, self.available))

        # check if there are enough resources for next entity in queue, if so seize resources
        if len(self.queue) != 0:
            _, required_resources, _, next_entity, event_handler = self.queue[0]
            if required_resources <= self.available:
                self.seize(num_resources, release_time)
                heapq.heappop(self.queue)
                return Event(
                    event_time=release_time,
                    event_name='Seize',
                    event_message=f'{next_entity.entity_type} {next_entity.entity_ind} entity seized {required_resources} {self.name} resources',
                    event_handler=event_handler,
                    event_entity=next_entity
                )
        return None

    def calc_utilization(self, duration: float) -> float:
        """
        Calculate resource utilization (integral of availability curve)

        :param duration: simulation duration
        """

        resource_utilization = 0
        for (t1, a1), (t2, _) in zip(self.availability_log, self.availability_log[1:]):
            resource_utilization += (t2 - t1) * a1
        max_utilization = self.capacity * duration
        return resource_utilization / max_utilization
