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
from collections import deque

from datatypes import *


@dataclass
class Module(ABC):
    name: str

    @abstractmethod
    def process_event(self, event: Event, sys_var: dict) -> List[Event]:
        """
        Process module event and pass entity to next module in the chain

        :param event: event to process
        :param sys_var: system global variables
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


def init_sys_var_entity(sys_var: dict, entity_type: str, entity_ind: int):
    """
    Initialize entity entry in system global variables dict

    :param sys_var: system global variables
    :param entity_type: entity type
    :param entity_ind: entity index
    """

    sys_var['entity']['metrics'][entity_ind] = {
        'Entity Type': entity_type,
        'Value-Added Time': 0.,
        'Non-Value-Added Time': 0.,
        'Wait Time': 0.,
        'Transfer Time': 0.,
        'Other Time': 0.
    }
    sys_var['entity']['trace'][entity_ind] = []


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

    def process_event(self, event: Event, sys_var: dict) -> List[Event]:
        """
        Process event at Create Module, pass to next module

        :param event: event to process
        :param sys_var: system global variables
        """

        logging.debug(event)

        event_entity_ind = event.event_entity.entity_ind
        init_sys_var_entity(sys_var, event.event_entity.entity_type, event_entity_ind)
        sys_var['entity']['metrics'][event_entity_ind]['Created At'] = event.event_time
        sys_var['entity']['trace'][event_entity_ind].append((f'Exit {self.name}', event.event_time))

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

    def process_event(self, event: Event, sys_var: dict) -> List[Event]:
        """
        Process event at Seize Module, pass to next module

        :param event: event to process
        :param sys_var: system global variables
        """

        logging.debug(event)

        event_entity_ind = event.event_entity.entity_ind
        sys_var['entity']['trace'][event_entity_ind].append((f'Exit {self.name}', event.event_time))
        if 'wait_time' in event.attr:
            sys_var['entity']['metrics'][event_entity_ind]['Wait Time'] += event.attr['wait_time']

        if isinstance(event.event_entity, BatchEntity):
            for entity in event.event_entity.batched_entities:
                entity_ind = entity.entity_ind
                sys_var['entity']['trace'][entity_ind].append((f'Exit {self.name}', event.event_time))
                if 'wait_time' in event.attr:
                    sys_var['entity']['metrics'][entity_ind]['Wait Time'] += event.attr['wait_time']

        return self.next_module.ingest_entity(event.event_entity, event.event_time)


@dataclass
class DelayModule(IngestModule):
    next_module: IngestModule
    delay_generator: Generator[float, None, None]
    cost_allocation: CostType = CostType.VALUE_ADDED
    module_ind: int = field(default_factory=count().__next__)

    def ingest_entity(self, entity: Entity, ingest_time: float) -> List[Event]:
        """
        Delay entity for time given by delay generator

        :param entity: ingested entity
        :param ingest_time: system time at ingest event
        """

        delay_time = next(self.delay_generator)
        return [Event(
            event_time=ingest_time + delay_time,
            event_name='Delay',
            event_message=f'{entity.entity_type} {entity.entity_ind} entity completed delay',
            event_handler=self.process_event,
            event_entity=entity,
            attr={
                'delay_time': delay_time
            }
        )]

    def process_event(self, event: Event, sys_var: dict) -> List[Event]:
        """
        Process event at Delay Module, pass to next module

        :param event: event to process
        :param sys_var: system global variables
        """

        logging.debug(event)

        event_entity_ind = event.event_entity.entity_ind
        sys_var['entity']['metrics'][event_entity_ind][f'{self.cost_allocation.value} Time'] += event.attr['delay_time']
        sys_var['entity']['trace'][event_entity_ind].append((f'Exit {self.name}', event.event_time))

        if isinstance(event.event_entity, BatchEntity):
            for entity in event.event_entity.batched_entities:
                entity_ind = entity.entity_ind
                sys_var['entity']['metrics'][entity_ind][f'{self.cost_allocation.value} Time'] += event.attr['delay_time']
                sys_var['entity']['trace'][entity_ind].append((f'Exit {self.name}', event.event_time))

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

    def process_event(self, event: Event, sys_var: dict) -> List[Event]:
        """
        Process event at Release Module, pass to next module

        :param event: event to process
        :param sys_var: system global variables
        """

        logging.debug(event)

        event_entity_ind = event.event_entity.entity_ind
        sys_var['entity']['trace'][event_entity_ind].append((f'Exit {self.name}', event.event_time))

        if isinstance(event.event_entity, BatchEntity):
            for entity in event.event_entity.batched_entities:
                entity_ind = entity.entity_ind
                sys_var['entity']['trace'][entity_ind].append((f'Exit {self.name}', event.event_time))

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

        assignment_names = ', '.join([assignment.assign_name for assignment in self.assignments])
        assign_event_msg = f'{entity.entity_type} {entity.entity_ind} entity performed assignments: {assignment_names}'

        return [Event(
            event_time=ingest_time,
            event_name='Assign',
            event_message=assign_event_msg,
            event_handler=self.process_event,
            event_entity=entity
        )]

    def process_event(self, event: Event, sys_var: dict) -> List[Event]:
        """
        Process event at Assign Module, pass to next module

        :param event: event to process
        :param sys_var: system global variables
        """

        logging.debug(event)

        for assignment in self.assignments:
            if assignment.assign_type == AssignType.VARIABLE:
                sys_var['variables'][assignment.assign_name] = assignment.assign_value_handler(sys_var['variables'], event.event_entity.attr)
            elif assignment.assign_type == AssignType.ATTRIBUTE:
                event.event_entity.attr[assignment.assign_name] = assignment.assign_value_handler(sys_var['variables'], event.event_entity.attr)
            elif assignment.assign_type == AssignType.ENTITY_TYPE:
                event.event_entity.entity_type = assignment.assign_name
            else:
                raise ValueError(f'Invalid AssignType: {assignment.assign_type}')

        event_entity_ind = event.event_entity.entity_ind
        sys_var['entity']['trace'][event_entity_ind].append((f'Exit {self.name}', event.event_time))

        if isinstance(event.event_entity, BatchEntity):
            for entity in event.event_entity.batched_entities:
                entity_ind = entity.entity_ind
                sys_var['entity']['trace'][entity_ind].append((f'Exit {self.name}', event.event_time))

        return self.next_module.ingest_entity(event.event_entity, event.event_time)


@dataclass
class DuplicateModule(IngestModule):
    next_module_orig: IngestModule
    next_module_dup: IngestModule
    module_ind: int = field(default_factory=count().__next__)

    def ingest_entity(self, entity: Entity, ingest_time: float) -> List[Event]:
        """
        Generate duplicate entity event

        :param entity: ingested entity
        :param ingest_time: system time at ingest event
        """

        if isinstance(entity, BatchEntity):
            raise TypeError(f'Invalid entity type provided to Duplicate Module. Expected: Entity. Received: BatchEntity.')

        return [Event(
            event_time=ingest_time,
            event_name='Duplicate',
            event_message=f'{entity.entity_type} {entity.entity_ind} entity duplicated',
            event_handler=self.process_event,
            event_entity=entity
        )]

    def process_event(self, event: Event, sys_var: dict) -> List[Event]:
        """
        Process event at Duplicate Module, pass to next modules

        :param event: event to process
        :param sys_var: system global variables
        """

        logging.debug(event)

        # create duplicate entity with same serial and different entity_ind
        orig_entity = event.event_entity
        dup_entity = Entity(
            entity_type=orig_entity.entity_type,
            arrival_time=event.event_time,
            serial=orig_entity.serial,
            attr=orig_entity.attr
        )

        # entity trace and metrics updates
        event_entity_ind = event.event_entity.entity_ind
        sys_var['entity']['trace'][event_entity_ind].append((f'Exit {self.name}', event.event_time))

        dup_entity_ind = dup_entity.entity_ind
        init_sys_var_entity(sys_var, dup_entity.entity_type, dup_entity_ind)
        sys_var['entity']['metrics'][dup_entity_ind]['Created At'] = event.event_time
        sys_var['entity']['trace'][dup_entity_ind].append((f'Exit {self.name}', event.event_time))

        # retrieve next events for orig and dup entities
        next_events = self.next_module_orig.ingest_entity(orig_entity, event.event_time)
        next_events.extend(self.next_module_dup.ingest_entity(dup_entity, event.event_time))

        return next_events


@dataclass
class BatchModule(IngestModule):
    next_module: IngestModule
    batch_type: BatchType
    batch_size: int
    batch_attr: Optional[str] = None
    batch_entity_type: Optional[str] = None
    queue: deque[Tuple[Entity, float]] = field(default_factory=deque)
    module_ind: int = field(default_factory=count().__next__)

    def ingest_entity(self, entity: Entity, ingest_time: float) -> List[Event]:
        """
        Generate batch entity event if a match is queued, otherwise queue ingested entity

        :param entity: ingested entity
        :param ingest_time: system time at ingest event
        """

        if isinstance(entity, BatchEntity):
            raise TypeError(f'Invalid entity type provided to Batch Module. Expected: Entity. Received: BatchEntity.')

        if self.batch_type == BatchType.ATTRIBUTE:
            needed_to_batch = self.batch_size - 1
            match_indices = []

            # search queue for matching entities to batch by attribute
            for i, (q_entity, _) in enumerate(self.queue):
                if self.batch_attr in q_entity.attr and self.batch_attr in entity.attr and \
                        q_entity.attr[self.batch_attr] == entity.attr[self.batch_attr]:
                    match_indices.append(i)
                    needed_to_batch -= 1
                    if needed_to_batch == 0:
                        break

            if needed_to_batch == 0:
                # sufficient matches found in queue, remove matches from queue and return batch event
                batch_entities: List[Tuple[Entity, float]] = [(entity, ingest_time)]
                for i in match_indices:
                    batch_entities.append(self.queue[i])
                for i in match_indices:
                    del self.queue[i]

                event_msg = 'Batched entities: '
                event_msg += ', '.join([f'{e.entity_type} {e.entity_ind}' for e, _ in batch_entities])
                return [Event(
                    event_time=ingest_time,
                    event_name='Batch',
                    event_message=event_msg,
                    event_handler=self.process_event,
                    event_entity=entity,
                    attr={
                        'batch_entities': batch_entities
                    }
                )]
            else:
                # not enough matches found in queue, add ingested entity to queue
                self.queue.append((entity, ingest_time))
                return []

        elif self.batch_type == BatchType.ANY:
            if len(self.queue) >= self.batch_size - 1:
                # sufficient entities in queue, remove batch_size-1 items from queue and return batch event
                batch_entities: List[Tuple[Entity, float]] = [(entity, ingest_time)]
                for _ in range(self.batch_size - 1):
                    batch_entities.append(self.queue.popleft())

                event_msg = 'Batched entities: '
                event_msg += ', '.join([f'{e.entity_type} {e.entity_ind}' for e, _ in batch_entities])
                return [Event(
                    event_time=ingest_time,
                    event_name='Batch',
                    event_message=event_msg,
                    event_handler=self.process_event,
                    event_entity=entity,
                    attr={
                        'batch_entities': batch_entities
                    }
                )]
            else:
                # not enough entities in queue, add ingested entity to queue
                self.queue.append((entity, ingest_time))
                return []
        else:
            raise ValueError(f'Invalid BatchType: {self.batch_type}')

    def process_event(self, event: Event, sys_var: dict) -> List[Event]:
        """
        Process event at Batch Module, pass to next module

        :param event: event to process
        :param sys_var: system global variables
        """

        logging.debug(event)

        # combine entities into a single batch entity
        batch_entity = BatchEntity(
            entity_type=self.batch_entity_type or event.event_entity.entity_type,
            arrival_time=event.event_time,
            batched_entities=[e for e, _ in event.attr['batch_entities']]
        )

        batch_entity_ind = batch_entity.entity_ind
        init_sys_var_entity(sys_var, batch_entity.entity_type, batch_entity_ind)
        sys_var['entity']['metrics'][batch_entity_ind]['Created At'] = event.event_time
        sys_var['entity']['trace'][batch_entity_ind].append((f'Exit {self.name}', event.event_time))

        for entity, queue_entry_time in event.attr['batch_entities']:
            entity_ind = entity.entity_ind
            sys_var['entity']['metrics'][entity_ind]['Wait Time'] += (event.event_time - queue_entry_time)
            sys_var['entity']['trace'][entity_ind].append((f'Exit {self.name}', event.event_time))

        return self.next_module.ingest_entity(batch_entity, event.event_time)


@dataclass
class SeparateModule(IngestModule):
    next_module: IngestModule
    module_ind: int = field(default_factory=count().__next__)

    def ingest_entity(self, entity: Entity, ingest_time: float) -> List[Event]:
        """
        Generate separate batch entity event

        :param entity: ingested batch entity
        :param ingest_time: system time at ingest event
        """

        if not isinstance(entity, BatchEntity):
            raise TypeError(f'Invalid entity type provided to Separate Module. Expected: BatchEntity. Received: {type(entity)}.')

        return [Event(
            event_time=ingest_time,
            event_name='Separate',
            event_message=f'{entity.entity_type} {entity.entity_ind} entity separated',
            event_handler=self.process_event,
            event_entity=entity
        )]

    def process_event(self, event: Event, sys_var: dict) -> List[Event]:
        """
        Process event at Separate Module, pass to next module

        :param event: event to process
        :param sys_var: system global variables
        """

        logging.debug(event)

        assert isinstance(event.event_entity, BatchEntity)

        batch_entity_ind = event.event_entity.entity_ind
        sys_var['entity']['metrics'][batch_entity_ind]['Disposed At'] = event.event_time
        sys_var['entity']['trace'][batch_entity_ind].append((f'Exit {self.name}', event.event_time))

        next_events = []
        for entity in event.event_entity.batched_entities:
            entity_ind = entity.entity_ind
            sys_var['entity']['trace'][entity_ind].append((f'Exit {self.name}', event.event_time))
            next_events.extend(self.next_module.ingest_entity(entity, event.event_time))

        return next_events


@dataclass
class DecideTwoWayByConditionModule(IngestModule):
    true_next_module: IngestModule
    false_next_module: IngestModule
    condition_handler: Callable[[dict, dict], bool]
    module_ind: int = field(default_factory=count().__next__)

    def ingest_entity(self, entity: Entity, ingest_time: float) -> List[Event]:
        """
        Generate decide two-way by condition event

        :param entity: ingested entity
        :param ingest_time: system time at ingest event
        """

        return [Event(
            event_time=ingest_time,
            event_name='Decide Two-Way By Condition',
            event_message=f'{entity.entity_type} {entity.entity_ind} entity decided two-way path by condition',
            event_handler=self.process_event,
            event_entity=entity
        )]

    def process_event(self, event: Event, sys_var: dict) -> List[Event]:
        """
        Process event at Decide two-way by condition Module, pass to next module

        :param event: event to process
        :param sys_var: system global variables
        """

        logging.debug(event)

        event_entity_ind = event.event_entity.entity_ind
        sys_var['entity']['trace'][event_entity_ind].append((f'Exit {self.name}', event.event_time))

        if isinstance(event.event_entity, BatchEntity):
            for entity in event.event_entity.batched_entities:
                entity_ind = entity.entity_ind
                sys_var['entity']['trace'][entity_ind].append((f'Exit {self.name}', event.event_time))

        if self.condition_handler(sys_var['variables'], event.event_entity.attr):
            return self.true_next_module.ingest_entity(event.event_entity, event.event_time)
        else:
            return self.false_next_module.ingest_entity(event.event_entity, event.event_time)


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

    def process_event(self, event: Event, sys_var: dict) -> List[Event]:
        """
        Process event at Dispose Module, pass to next module

        :param event: event to process
        :param sys_var: system global variables
        """

        logging.debug(event)

        event_entity_ind = event.event_entity.entity_ind
        sys_var['entity']['metrics'][event_entity_ind]['Disposed At'] = event.event_time
        sys_var['entity']['trace'][event_entity_ind].append((f'Exit {self.name}', event.event_time))

        if isinstance(event.event_entity, BatchEntity):
            for entity in event.event_entity.batched_entities:
                entity_ind = entity.entity_ind
                sys_var['entity']['metrics'][entity_ind]['Disposed At'] = event.event_time
                sys_var['entity']['trace'][entity_ind].append((f'Exit {self.name}', event.event_time))

        return []
