"""
modules.py

Simulation module definitions
"""

from dataclasses import dataclass, field
from typing import Callable, Generator, List

from datatypes import *


@dataclass
class Module:
    name: str


@dataclass
class CreateModule(Module):
    next_module: Module
    gen_entity_type: str
    arrival_generator: Callable[[float, float], Generator[float, None, None]]
    entities_per_arrival: int = 1
    max_arrivals: int = -1
    first_arrival: float = 0.

    def generate_arrivals(self, end_time: float):
        arrivals = self.arrival_generator(self.first_arrival, end_time)
        for arrival_time in arrivals:
            entity = Entity(
                entity_type=self.gen_entity_type
            )
            arrival_event = Event(
                event_time=arrival_time,
                event_name='Entity arrival'
            )
            print(arrival_time)


@dataclass
class SeizeModule(Module):
    next_module: Module
    resource: Resource
    num_resources: int
    queue: List[Entity] = field(default_factory=list)

    def seize_resources(self):
        print('Seized resource', self.resource.name)


@dataclass
class DelayModule(Module):
    next_module: Module
    delay_generator: Callable[[], float]

    def generate_delay(self):
        return self.delay_generator()


@dataclass
class ReleaseModule(Module):
    next_module: Module
    resource: Resource
    num_resources: int

    def release_resources(self):
        self.resource.release(self.num_resources)


@dataclass
class DisposeModule(Module):
    def dispose_entity(self, entity: Entity):
        # TODO: update metrics, dispose entity
        pass
