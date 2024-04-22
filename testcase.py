"""
testcase.py

Full test case for a simple simulation environment
"""

from generators import exp_agg_generator, exp_generator
from modules import *
from environment import Environment


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
                delay_generator=exp_generator(1)
            ),
            resource=server_resource,
            num_resources=1
        ),
        gen_entity_type='Test',
        arrival_generator=exp_agg_generator(1)
    )

    env = Environment(
        root_modules=[create_mod]
    )
    env.run_simulation(100)
