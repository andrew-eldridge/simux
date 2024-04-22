"""
testcase.py

Full test case for a simple simulation environment
"""

import logging
from generators import exp_agg_generator, exp_generator
from modules import *
from environment import Environment


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

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
        arrival_modules=[create_mod]
    )
    sim_duration = 100
    sys_var, sys_entity_attr_df = env.run_simulation(sim_duration)

    # system metric outputs
    print(sys_entity_attr_df.head())
    print(sys_entity_attr_df.shape)
    print('total entity system time', sys_var['total_entity_system_time'])

    # resource metric outputs
    resource_utilization = server_resource.calc_utilization(sim_duration)
    print('server resource utilization', resource_utilization)
