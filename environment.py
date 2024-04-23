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
    module_chains: List[ArrivalModule] = field(default_factory=list)
    event_queue: List[Tuple[float, int, Event]] = field(default_factory=list)
    variables: List[Tuple[str, any]] = field(default_factory=list)
    sys_var: dict = field(default_factory=dict)

    def __populate_event_queue_arrivals(self, end_time: float):
        """
        Populate the event queue with initial arrivals from entry point modules

        :param end_time: simulation end time
        """

        for arrival_mod in self.module_chains:
            arrival_events = arrival_mod.generate_arrivals(end_time)
            for event in arrival_events:
                self.__add_event(event)

    def __add_event(self, event: Event):
        """
        Add event to event queue (min heap based on event time)

        :param event: event to add to queue
        """

        heapq.heappush(self.event_queue, (event.event_time, event.event_entity.entity_ind, event))

    def add_variable(self, var: str, val: object):
        self.variables.append((var, val))

    def run_simulation(self, duration: float) -> Tuple[dict, pd.DataFrame]:
        """
        Run simulation from initial state for given duration and calculate metrics

        :param duration: duration of simulation
        """

        self.sys_var = {
            'entity': {
                'metrics': {},
                'trace': {}
            },
            'variables': {
                var: v
                for var, v in self.variables
            },
            'metrics': {}
        }
        self.__populate_event_queue_arrivals(duration)
        while len(self.event_queue) != 0 and self.event_queue[0][2].event_time <= duration:
            # get next event and call handler function
            curr_event_time, _, curr_event = heapq.heappop(self.event_queue)
            new_events = curr_event.event_handler(curr_event, self.sys_var)

            # add new events to event queue
            for event in new_events:
                self.__add_event(event)

        # calculate metrics
        sys_entity_metrics_df = pd.DataFrame.from_dict(self.sys_var['entity']['metrics'], orient='index')
        sys_entity_metrics_df['Time in System'] = sys_entity_metrics_df['Disposed At'] - sys_entity_metrics_df['Created At']
        self.sys_var['metrics']['Total Entity System Time'] = sum(sys_entity_metrics_df['Time in System'].dropna())

        return self.sys_var, sys_entity_metrics_df
