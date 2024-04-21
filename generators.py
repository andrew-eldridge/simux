import math
import random


def exp_generator(l: float, start_time: int, end_time: int):
    time = start_time
    while time < end_time:
        u = random.uniform(0, 1)
        time += -1/l * math.log(u)
        yield time


def tria_generator(low: float, high: float, start_time: int, end_time: int):
    time = start_time
    while time < end_time:
        u = random.uniform(0, 1)
        time += random.triangular(low, high)
        yield time


# exp_gen = inv_exponential(1.)
# exp_dist = DistGenerator(exp_gen, start_time=0, end_time=100)
arrival_times = list(exp_generator(1., 0, 100))
print(arrival_times)
print(len(arrival_times))
