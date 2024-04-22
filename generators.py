"""
generators.py

Generator functions for common arrival distributions
"""

import math
import random
from typing import Generator, Callable


def exp_agg_generator(lambd: float) -> Callable[[float, float], Generator[float, None, None]]:
    """
    Returns a generator function for arrivals from a homogeneous Poisson process in [start_time, end_time]

    :param lambd: mean arrivals per time unit
    """

    def exp_agg_generator_parameterized(start_time: float, end_time: float) -> Generator[float, None, None]:
        time = start_time
        while time < end_time:
            u = random.uniform(0, 1)
            time += -1 / lambd * math.log(u)
            if time <= end_time:
                yield time

    return exp_agg_generator_parameterized


def exp_generator(lambd: float) -> Generator[float, None, None]:
    """
    Generator function for independent exponential realizations

    :param lambd: mean arrivals per time unit
    """

    while True:
        u = random.uniform(0, 1)
        yield -1 / lambd * math.log(u)


def tria_agg_generator(low: float, high: float) -> Callable[[float, float], Generator[float, None, None]]:
    """
    Returns a generator function for arrivals according to a triangular distribution in [start_time, end_time]

    :param low: lowest possible inter-arrival time
    :param high: highest possible inter-arrival time
    """

    def tria_agg_generator_parameterized(start_time: float, end_time: float) -> Generator[float, None, None]:
        time = start_time
        while time < end_time:
            time += random.triangular(low, high)
            if time <= end_time:
                yield time

    return tria_agg_generator_parameterized


def tria_generator(low: float, high: float) -> Generator[float, None, None]:
    """
    Generator function for independent triangular realizations

    :param low: lowest possible distribution value
    :param high: highest possible distribution value
    """

    while True:
        yield random.triangular(low, high)


def const_agg_generator(const: float) -> Callable[[float, float], Generator[float, None, None]]:
    """
    Returns a generator function for arrivals at a constant interval in [start_time, end_time]

    :param const: constant time interval
    """

    def const_agg_generator_bounded(start_time: float, end_time: float) -> Generator[float, None, None]:
        time = start_time
        while time < end_time:
            time += const
            if time <= end_time:
                yield time

    return const_agg_generator_bounded


def const_generator(const: float) -> Generator[float, None, None]:
    """
    Generator function for (trivial) constant realizations

    :param const: constant to be yielded
    """

    while True:
        yield const
