"""
modules.py

Simulation module definitions
"""

import heapq
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Callable, Generator, List, Optional

from datatypes import *


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
            raise ValueError('ERROR: tried releasing resources in excess of capacity', self.name, self.capacity,
                             self.available, num_resources)
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


@dataclass
class Module(ABC):
    name: str

    @abstractmethod
    def ingest_entity(self, entity: Entity, ingest_time: float) -> List[Event]:
        ...

    @abstractmethod
    def process_event(self, event: Event) -> List[Event]:
        ...


@dataclass
class CreateModule(Module):
    next_module: Module
    gen_entity_type: str
    arrival_generator: Callable[[float, float], Generator[float, None, None]]
    entities_per_arrival: int = 1
    max_arrivals: int = -1
    first_arrival_time: float = 0.

    def generate_arrivals(self, end_time: float):
        arrivals = self.arrival_generator(self.first_arrival_time, end_time)
        arrival_events = []
        for arrival_time in arrivals:
            entity = Entity(
                entity_type=self.gen_entity_type
            )
            arrival_events.append(Event(
                event_time=arrival_time,
                event_name=f'{entity.entity_type} {entity.entity_id} entity arrival',
                event_handler=self.process_event,
                event_entity=entity
            ))
        return arrival_events

    def ingest_entity(self, entity: Entity, ingest_time: float) -> List[Event]:
        return []

    def process_event(self, event: Event) -> List[Event]:
        """
        Process entity at Create Module, generate and return event at next module

        :param event: target event
        """

        print(event)
        return self.next_module.ingest_entity(event.event_entity, event.event_time)


@dataclass
class SeizeModule(Module):
    next_module: Module
    resource: Resource
    num_resources: int

    def ingest_entity(self, entity: Entity, ingest_time: float) -> List[Event]:
        if self.resource.available < self.num_resources:
            self.resource.queue_entity(entity, self.num_resources, ingest_time, self.process_event)
            return []
        else:
            self.resource.seize(self.num_resources)
            return [Event(
                event_time=ingest_time,
                event_name=f'{entity.entity_type} {entity.entity_id} entity seized {self.num_resources} {self.resource.name} resources',
                event_handler=self.process_event,
                event_entity=entity
            )]

    def process_event(self, event: Event) -> List[Event]:
        print(event)
        return self.next_module.ingest_entity(event.event_entity, event.event_time)


@dataclass
class DelayModule(Module):
    next_module: Module
    delay_generator: Generator[float, None, None]

    def ingest_entity(self, entity: Entity, ingest_time: float) -> List[Event]:
        return [Event(
            event_time=ingest_time + next(self.delay_generator),
            event_name=f'{entity.entity_type} {entity.entity_id} entity completed delay',
            event_handler=self.process_event,
            event_entity=entity
        )]

    def process_event(self, event: Event) -> List[Event]:
        print(event)
        return self.next_module.ingest_entity(event.event_entity, event.event_time)


@dataclass
class ReleaseModule(Module):
    next_module: Module
    resource: Resource
    num_resources: int

    def ingest_entity(self, entity: Entity, ingest_time: float) -> List[Event]:
        events = []

        # release resources, get next seize event if one occurs
        new_seize_event = self.resource.release(self.num_resources, ingest_time)
        if new_seize_event:
            events.append(new_seize_event)

        events.append(Event(
            event_time=ingest_time,
            event_name=f'{entity.entity_type} {entity.entity_id} entity released {self.num_resources} {self.resource.name} resources',
            event_handler=self.process_event,
            event_entity=entity
        ))
        return events

    def process_event(self, event: Event) -> List[Event]:
        print(event)
        return self.next_module.ingest_entity(event.event_entity, event.event_time)


@dataclass
class DisposeModule(Module):

    def ingest_entity(self, entity: Entity, ingest_time: float) -> List[Event]:
        return [Event(
            event_time=ingest_time,
            event_name=f'{entity.entity_type} {entity.entity_id} entity disposed',
            event_handler=self.process_event,
            event_entity=entity
        )]

    def process_event(self, event: Event) -> List[Event]:
        print(event)
        return []
