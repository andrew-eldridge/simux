"""
modules.py

Simulation module definitions
"""

import heapq
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Callable, Generator, List, Tuple, Optional
from itertools import count

from datatypes import *


@dataclass
class Module(ABC):
    name: str

    @abstractmethod
    def process_event(self, event: Event, sys_var: dict, sys_entity_attr: dict) -> List[Event]:
        """
        Process module event and pass entity to next module in the chain

        :param event: event to process
        :param sys_var: system global variables
        :param sys_entity_attr: system global entity attributes
        """
        ...


class ArrivalModule(Module):
    @abstractmethod
    def generate_arrivals(self, end_time: float) -> List[Event]:
        """
        Generate arrival events through simulation end time

        :param end_time: simulation end time
        """
        ...


class IngestModule(Module):
    @abstractmethod
    def ingest_entity(self, entity: Entity, ingest_time: float) -> List[Event]:
        """
        Generate events corresponding to entity arrival at a module to be added to the event queue

        :param entity: ingested entity
        :param ingest_time: system time at ingest event
        """
        ...


def update_sys_entity_attr(sys_entity_attr: dict, entity_ind: int, updated_attr: dict):
    """
    Update system global entity attributes on target entity from attr update dictionary

    :param sys_entity_attr: system global entity attributes
    :param entity_ind: entity index to update
    :param updated_attr: attr update dictionary
    """

    if entity_ind not in sys_entity_attr:
        sys_entity_attr[entity_ind] = {}
    sys_entity_attr[entity_ind].update(updated_attr)


@dataclass
class CreateModule(ArrivalModule):
    next_module: IngestModule
    gen_entity_type: str
    arrival_generator: Callable[[float, float], Generator[float, None, None]]
    module_ind: int = field(default_factory=count().__next__)
    entities_per_arrival: int = 1
    max_arrivals: int = -1
    first_arrival_time: float = 0.

    def generate_arrivals(self, end_time: float):
        """
        Generate arrival events through simulation end time according to provided generator function

        :param end_time: simulation end time
        """

        arrivals = self.arrival_generator(self.first_arrival_time, end_time)
        arrival_events = []
        for arrival_time in arrivals:
            entity = Entity(
                entity_type=self.gen_entity_type,
                arrival_time=arrival_time
            )
            arrival_events.append(Event(
                event_time=arrival_time,
                event_name='Create',
                event_message=f'{entity.entity_type} {entity.entity_ind} entity arrival',
                event_handler=self.process_event,
                event_entity=entity
            ))
        return arrival_events

    def process_event(self, event: Event, sys_var: dict, sys_entity_attr: dict) -> List[Event]:
        """
        Process event at Create Module, pass to next module

        :param event: event to process
        :param sys_var: system global variables
        :param sys_entity_attr: system global entity attributes
        """

        logging.debug(event)

        update_sys_entity_attr(sys_entity_attr, event.event_entity.entity_ind, {
            'create_time': event.event_time,
            f'create_{self.module_ind}_exit_time': event.event_time
        })

        return self.next_module.ingest_entity(event.event_entity, event.event_time)


@dataclass
class SeizeModule(IngestModule):
    next_module: IngestModule
    resource: Resource
    num_resources: int
    module_ind: int = field(default_factory=count().__next__)

    def ingest_entity(self, entity: Entity, ingest_time: float) -> List[Event]:
        """
        Seize resource if available, otherwise add entity to resource queue

        :param entity: ingested entity
        :param ingest_time: system time at ingest event
        """

        if self.resource.available < self.num_resources:
            self.resource.queue_entity(entity, self.num_resources, ingest_time, self.process_event)
            return []
        else:
            self.resource.seize(self.num_resources, ingest_time)
            return [Event(
                event_time=ingest_time,
                event_name='Seize',
                event_message=f'{entity.entity_type} {entity.entity_ind} entity seized {self.num_resources} {self.resource.name} resources',
                event_handler=self.process_event,
                event_entity=entity
            )]

    def process_event(self, event: Event, sys_var: dict, sys_entity_attr: dict) -> List[Event]:
        """
        Process event at Seize Module, pass to next module

        :param event: event to process
        :param sys_var: system global variables
        :param sys_entity_attr: system global entity attributes
        """

        logging.debug(event)

        update_sys_entity_attr(sys_entity_attr, event.event_entity.entity_ind, {
            f'seize_{self.module_ind}_exit_time': event.event_time
        })

        return self.next_module.ingest_entity(event.event_entity, event.event_time)


@dataclass
class DelayModule(IngestModule):
    next_module: IngestModule
    delay_generator: Generator[float, None, None]
    module_ind: int = field(default_factory=count().__next__)

    def ingest_entity(self, entity: Entity, ingest_time: float) -> List[Event]:
        """
        Delay entity for time given by delay generator

        :param entity: ingested entity
        :param ingest_time: system time at ingest event
        """

        return [Event(
            event_time=ingest_time + next(self.delay_generator),
            event_name='Delay',
            event_message=f'{entity.entity_type} {entity.entity_ind} entity completed delay',
            event_handler=self.process_event,
            event_entity=entity
        )]

    def process_event(self, event: Event, sys_var: dict, sys_entity_attr: dict) -> List[Event]:
        """
        Process event at Delay Module, pass to next module

        :param event: event to process
        :param sys_var: system global variables
        :param sys_entity_attr: system global entity attributes
        """

        logging.debug(event)

        update_sys_entity_attr(sys_entity_attr, event.event_entity.entity_ind, {
            f'delay_{self.module_ind}_exit_time': event.event_time
        })

        return self.next_module.ingest_entity(event.event_entity, event.event_time)


@dataclass
class ReleaseModule(IngestModule):
    next_module: IngestModule
    resource: Resource
    num_resources: int
    module_ind: int = field(default_factory=count().__next__)

    def ingest_entity(self, entity: Entity, ingest_time: float) -> List[Event]:
        """
        Release resources seized by entity

        :param entity: ingested entity
        :param ingest_time: system time at ingest event
        """

        events = []

        # release resources, get next seize event if one occurs
        new_seize_event = self.resource.release(self.num_resources, ingest_time)
        if new_seize_event:
            events.append(new_seize_event)

        events.append(Event(
            event_time=ingest_time,
            event_name='Release',
            event_message=f'{entity.entity_type} {entity.entity_ind} entity released {self.num_resources} {self.resource.name} resources',
            event_handler=self.process_event,
            event_entity=entity
        ))
        return events

    def process_event(self, event: Event, sys_var: dict, sys_entity_attr: dict) -> List[Event]:
        """
        Process event at Release Module, pass to next module

        :param event: event to process
        :param sys_var: system global variables
        :param sys_entity_attr: system global entity attributes
        """

        logging.debug(event)

        update_sys_entity_attr(sys_entity_attr, event.event_entity.entity_ind, {
            f'release_{self.module_ind}_exit_time': event.event_time
        })

        return self.next_module.ingest_entity(event.event_entity, event.event_time)


@dataclass
class AssignModule(IngestModule):
    next_module: IngestModule
    assignments: List[Assignment]
    module_ind: int = field(default_factory=count().__next__)

    def ingest_entity(self, entity: Entity, ingest_time: float) -> List[Event]:
        """
        Perform given assignments

        :param entity: ingested entity
        :param ingest_time: system time at ingest event
        """

        assignments_str_lst = [f'[{assignment.assign_type.value}]' for assignment in self.assignments]
        for assignment in self.assignments:
            if assignment.assign_type == AssignType.ENTITY_TYPE:
                assignments_str_lst.append(f'[ENTITY TYPE] {assignment.assign_name}')
            else:
                assignments_str_lst.append(f'[{assignment.assign_type.value}] {assignment.assign_name}:{assignment.assign_value}')
        assignments_str = ', '.join(assignments_str_lst)
        assign_event_msg = f'{entity.entity_type} {entity.entity_ind} entity performed assignments: {assignments_str}'

        return [Event(
            event_time=ingest_time,
            event_name='Assign',
            event_message=assign_event_msg,
            event_handler=self.process_event,
            event_entity=entity
        )]

    def process_event(self, event: Event, sys_var: dict, sys_entity_attr: dict) -> List[Event]:
        """
        Process event at Assign Module, pass to next module

        :param event: event to process
        :param sys_var: system global variables
        :param sys_entity_attr: system global entity attributes
        """

        logging.debug(event)

        sys_entity_attr_updates = {}
        for assignment in self.assignments:
            if assignment.assign_type == AssignType.VARIABLE:
                sys_entity_attr_updates[assignment.assign_name] = assignment.assign_value
            elif assignment.assign_type == AssignType.ATTRIBUTE:
                event.event_entity.attr[assignment.assign_name] = assignment.assign_value
            elif assignment.assign_type == AssignType.ENTITY_TYPE:
                event.event_entity.entity_type = assignment.assign_name

        sys_entity_attr_updates[f'assign_{self.module_ind}_exit_time'] = event.event_time
        update_sys_entity_attr(sys_entity_attr, event.event_entity.entity_ind, sys_entity_attr_updates)

        return self.next_module.ingest_entity(event.event_entity, event.event_time)


@dataclass
class DisposeModule(IngestModule):
    module_ind: int = field(default_factory=count().__next__)

    def ingest_entity(self, entity: Entity, ingest_time: float) -> List[Event]:
        """
        Dispose entity

        :param entity: ingested entity
        :param ingest_time: system time at ingest event
        """

        return [Event(
            event_time=ingest_time,
            event_name='Dispose',
            event_message=f'{entity.entity_type} {entity.entity_ind} entity disposed',
            event_handler=self.process_event,
            event_entity=entity
        )]

    def process_event(self, event: Event, sys_var: dict, sys_entity_attr: dict) -> List[Event]:
        """
        Process event at Dispose Module, pass to next module

        :param event: event to process
        :param sys_var: system global variables
        :param sys_entity_attr: system global entity attributes
        """

        logging.debug(event)

        update_sys_entity_attr(sys_entity_attr, event.event_entity.entity_ind, {
            f'dispose_{self.module_ind}_exit_time': event.event_time,
            'dispose_time': event.event_time
        })

        return []
