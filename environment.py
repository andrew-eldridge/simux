"""
environment.py

Environment containing all simulation modules and simulation-level variables
"""

import pandas as pd
import numpy as np
import random
import math
import heapq
from typing import List, Tuple, Callable, Dict
from dataclasses import dataclass, field
from enum import Enum

import generators
from generators import *
from modules import *
from datatypes import *


@dataclass
class Environment:
    arrival_modules: List[ArrivalModule] = field(default_factory=list)
    event_queue: List[Tuple[float, int, Event]] = field(default_factory=list)
    total_entity_system_time: float = 0.
    system_attr: dict = field(default_factory=dict)

    def run_simulation(self, duration: float) -> pd.DataFrame:
        """
        Run simulation from initial state for given duration and calculate metrics

        :param duration: duration of simulation
        """

        self.__populate_event_queue_arrivals(duration)
        while len(self.event_queue) != 0 and self.event_queue[0][2].event_time <= duration:
            # get next event and call handler function
            curr_event_time, _, curr_event = heapq.heappop(self.event_queue)
            new_events: List[Event]
            updated_attr: dict
            new_events, updated_attr = curr_event.event_handler(curr_event)

            # update system attributes for entity if any provided
            if updated_attr:
                curr_event_entity_ind = curr_event.event_entity.entity_ind
                if curr_event_entity_ind not in self.system_attr:
                    self.system_attr[curr_event_entity_ind] = {}
                self.system_attr[curr_event_entity_ind].update(updated_attr)

            # add new events to event queue
            for event in new_events:
                self.add_event(event)

        # calculate metrics
        system_attr_df = pd.DataFrame.from_dict(self.system_attr, orient='index')
        system_attr_df['system_time'] = system_attr_df['dispose_time'] - system_attr_df['create_time']
        self.total_entity_system_time = sum(system_attr_df['system_time'].dropna())

        print(system_attr_df.head())
        print(system_attr_df.shape)
        print('total entity system time', self.total_entity_system_time)

        return system_attr_df

    def __populate_event_queue_arrivals(self, end_time: float):
        """
        Populate the event queue with initial arrivals from entry point modules

        :param end_time: simulation end time
        """

        for arrive_mod in self.arrival_modules:
            arrival_events = arrive_mod.generate_arrivals(end_time)
            for event in arrival_events:
                self.add_event(event)

    def add_event(self, event: Event):
        """
        Add event to event queue (min heap based on event time)

        :param event: event to add to queue
        """

        heapq.heappush(self.event_queue, (event.event_time, event.event_entity.entity_ind, event))
