import heapq
from dataclasses import dataclass


@dataclass
class Event:
    start_time: int
    event_name: str


class EventQueue:
    def __init__(self):
        self.event_queue = []

    def add_event(self, event: Event):
        heapq.heappush(self.event_queue, (event.start_time, event))
