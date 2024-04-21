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
    event_queue: List[tuple[float, Event]] = field(default_factory=list)
    resources: dict[str, Resource] = field(default_factory=dict)

    def add_event(self, event: Event):
        heapq.heappush(self.event_queue, (event.event_time, event))

    def add_resource(self, resource: Resource):
        self.resources[resource.name] = resource


if __name__ == '__main__':
    server_resource = Resource(
        name='Server',
        capacity=3
    )

    create_mod = CreateModule(
        name='Test Create',
        next_module=SeizeModule(
            name='Test Seize',
            next_module=DelayModule(
                name='Test Delay',
                next_module=ReleaseModule(
                    name='Test Release',
                    next_module=DisposeModule(
                        name='Test Dispose'
                    ),
                    resource=server_resource,
                    num_resources=1
                ),
                delay_generator=lambda: 1.
            ),
            resource=server_resource,
            num_resources=1
        ),
        gen_entity_type='Test',
        arrival_generator=generators.exp_generator(1.)
    )

    create_mod.generate_arrivals(100)
