"""
environment.py

Environment containing all simulation modules and simulation-level variables
"""

import numpy as np
import random
import math
import heapq
from typing import List, Callable, Dict
from dataclasses import dataclass, field
from enum import Enum

import generators
from generators import *
from modules import *
from datatypes import *


@dataclass
class Environment:
    root_modules: List[CreateModule] = field(default_factory=list)
    event_queue: List[tuple[float, int, Event]] = field(default_factory=list)
    resources: dict[str, Resource] = field(default_factory=dict)
    total_entity_system_time: float = 0.
    total_resource_utilization_time: float = 0.

    def run_simulation(self, duration: float):
        self.__populate_event_queue_arrivals(duration)
        while len(self.event_queue) != 0 and self.event_queue[0][2].event_time <= duration:
            curr_event_time, _, curr_event = heapq.heappop(self.event_queue)
            new_events: List[Event] = curr_event.event_handler(curr_event)
            for event in new_events:
                self.add_event(event)

    def __populate_event_queue_arrivals(self, end_time: float):
        for root_mod in self.root_modules:
            arrival_events = root_mod.generate_arrivals(end_time)
            for event in arrival_events:
                self.add_event(event)

    def add_event(self, event: Event):
        heapq.heappush(self.event_queue, (event.event_time, event.event_entity.entity_id, event))

    def add_resource(self, resource: Resource):
        self.resources[resource.name] = resource
