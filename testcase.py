"""
testcase.py

Full test case for a simple simulation environment
"""

import logging
from generators import exp_agg_generator, exp_generator, tria_generator
from modules import *
from environment import Environment


def testcase_1() -> Tuple[Environment, List[Resource]]:
    server = Resource(
        name='Server',
        capacity=3
    )

    mod_chain = CreateModule(
        name='Test Create',
        next_module=DisposeModule(
            name='Test Dispose'
        ),
        gen_entity_type='Test Entity',
        arrival_generator=exp_agg_generator(1)
    )

    env = Environment(
        module_chains=[mod_chain]
    )
    resources = [server]

    return env, resources


def testcase_2() -> Tuple[Environment, List[Resource]]:
    server = Resource(
        name='Server',
        capacity=3
    )

    mod_chain = CreateModule(
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
                    resource=server,
                    num_resources=1
                ),
                delay_generator=exp_generator(1)
            ),
            resource=server,
            num_resources=1
        ),
        gen_entity_type='Test',
        arrival_generator=exp_agg_generator(1)
    )

    env = Environment(
        module_chains=[mod_chain]
    )
    resources = [server]

    return env, resources


def testcase_3() -> Tuple[Environment, List[Resource]]:
    server1 = Resource(
        name='Server 1',
        capacity=3
    )
    server2 = Resource(
        name='Server 2',
        capacity=2
    )

    def assign_last_entity_handler(variables: dict, entity_attr: dict) -> any:
        return variables['last_entity'] + 1

    def assign_entity_id_handler(variables: dict, entity_attr: dict) -> any:
        return variables['last_entity']

    batch_mod_chain = BatchModule(
        name='Batch 1',
        batch_type=BatchType.ATTRIBUTE,
        batch_size=2,
        batch_attr='entity_id',
        batch_entity_type='Person Group',
        next_module=DelayModule(
            name='Delay 3',
            delay_generator=tria_generator(0, 3),
            cost_allocation=CostType.NON_VALUE_ADDED,
            next_module=DisposeModule(
                name='Dispose 1'
            )
        )
    )

    mod_chain = CreateModule(
        name='Create Person',
        gen_entity_type='Person',
        arrival_generator=exp_agg_generator(1),
        next_module=AssignModule(
            name='Assign entity ID',
            assignments=[
                Assignment(assign_type=AssignType.VARIABLE, assign_name='last_entity', assign_value_handler=assign_last_entity_handler),
                Assignment(assign_type=AssignType.ATTRIBUTE, assign_name='entity_id', assign_value_handler=assign_entity_id_handler)
            ],
            next_module=DuplicateModule(
                name='Duplicate 1',
                next_module_orig=DelayModule(
                    name='Delay 1',
                    delay_generator=exp_generator(0.5),
                    next_module=batch_mod_chain
                ),
                next_module_dup=DelayModule(
                    name='Delay 2',
                    delay_generator=tria_generator(0, 2),
                    next_module=batch_mod_chain
                )
            )
        )
    )

    env = Environment(
        module_chains=[mod_chain],
        variables=[('last_entity', 0)]
    )
    resources = [server1, server2]

    return env, resources


def main():
    logging.basicConfig(level=logging.DEBUG)

    env, resources = testcase_3()
    sim_duration = 100
    sys_var, sys_entity_metrics_df = env.run_simulation(sim_duration)

    # system metric outputs
    print(sys_entity_metrics_df)
    print('Total entity system time:', sys_var['metrics']['Total Entity System Time'])

    # resource metric outputs
    for r in resources:
        resource_utilization = r.calc_utilization(sim_duration)
        print(f'{r.name} resource utilization:', resource_utilization)


if __name__ == '__main__':
    main()
