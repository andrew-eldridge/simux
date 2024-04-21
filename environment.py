import numpy as np
import random
import math
import heapq
from typing import List, Callable
from dataclasses import dataclass
from enum import Enum


class Unit(Enum):
    SECONDS = 1
    MINUTES = 2
    HOURS = 3
    DAYS = 4


@dataclass
class Module:
    name: str


@dataclass
class ModuleOneOutput(Module):
    next_module: Module


@dataclass
class ModuleTwoOutputs(Module):
    next_module: Module
    next_module2: Module


@dataclass
class CreateModule(ModuleOneOutput):
    entity_type: str
    arrival_generator: Callable
    entities_per_arrival: int
    max_arrivals: int = -1
    first_arrival: float = 0.

    def generate_arrivals(self, end_time: float):
        return self.arrival_generator(self.first_arrival, end_time)


@dataclass
class Event:
    start_time: int
    event_name: str
    attr: dict


class Environment:
    def __init__(self, modules: List[Module] = None):
        self.modules = modules if modules else []
        self.event_queue = []

    def add_event(self, event: Event):
        heapq.heappush(self.event_queue, (event.start_time, event))
