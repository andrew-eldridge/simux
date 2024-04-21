"""
generators.py

Generator functions for common arrival distributions
"""

import math
import random
from typing import Generator, Callable


def exp_generator(l: float) -> Callable[[float, float], Generator[float, None, None]]:
    """
    Returns a generator function for arrivals from a homogeneous Poisson process in [start_time, end_time]

    :param l: lambda, mean inter-arrival time
    """

    def exp_generator_bounded(start_time: float, end_time: float) -> Generator[float, None, None]:
        time = start_time
        while time < end_time:
            u = random.uniform(0, 1)
            time += -1/l * math.log(u)
            if time <= end_time:
                yield time

    return exp_generator_bounded


def tria_generator(low: float, high: float) -> Callable[[float, float], Generator[float, None, None]]:
    """
    Returns a generator function for arrivals according to a triangular distribution in [start_time, end_time]

    :param low: lowest possible inter-arrival time
    :param high: highest possible inter-arrival time
    """

    def tria_generator_bounded(start_time: float, end_time: float) -> Generator[float, None, None]:
        time = start_time
        while time < end_time:
            time += random.triangular(low, high)
            if time <= end_time:
                yield time

    return tria_generator_bounded


def const_generator(interval: float) -> Callable[[float, float], Generator[float, None, None]]:
    """
    Returns a generator function for arrivals at a constant interval in [start_time, end_time]

    :param interval: constant inter-arrival time
    """

    def const_generator_bounded(start_time: float, end_time: float):
        time = start_time
        while time < end_time:
            time += interval
            if time <= end_time:
                yield time

    return const_generator_bounded
