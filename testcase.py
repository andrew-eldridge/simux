"""
testcase.py

Full test case for a simple simulation environment
"""

import logging
import argparse
from generators import exp_agg_generator, exp_generator, tria_generator, uniform_generator
from modules import *
from environment import Environment


def testcase_1() -> Tuple[Environment, List[Resource]]:
    """
    Minimal test case; simple Create and Dispose simulation
    """

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
    """
    Test case for a single server (M/M/1) queue with Seize/Delay/Release modules
    """

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
    """
    Test case for a Duplicate and Batch process using Delay and variable/attribute Assigns.
    Entities are assigned a unique identifier, duplicated, delayed, and batched by their unique identifier.
    """

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

    return env, []


def testcase_4() -> Tuple[Environment, List[Resource]]:
    """
    Extension of Test Case 3; simulates a Duplicate and Batch process using multiple Servers shared across Seize/Delay/Release groups.
    Entities are assigned a unique identifier, duplicated, seize one of several resources, delay, release the resource,
    are batched by their unique identifier, seize a resource accessed in a previous step, delay, release the resource, and dispose.
    """

    def assign_last_entity_handler(variables: dict, entity_attr: dict) -> any:
        return variables['last_entity'] + 1

    def assign_entity_id_handler(variables: dict, entity_attr: dict) -> any:
        return variables['last_entity']

    server1 = Resource(
        name='Server 1',
        capacity=3
    )
    server2 = Resource(
        name='Server 2',
        capacity=2
    )

    batch_mod_chain = BatchModule(
        name='Batch 1',
        batch_type=BatchType.ATTRIBUTE,
        batch_size=2,
        batch_attr='entity_id',
        batch_entity_type='Person Group',
        next_module=SeizeModule(
            name='Seize Server 1 Again',
            resource=server1,
            num_resources=1,
            next_module=DelayModule(
                name='Delay 3',
                delay_generator=tria_generator(0, 3),
                cost_allocation=CostType.NON_VALUE_ADDED,
                next_module=ReleaseModule(
                    name='Release Server 1 Again',
                    resource=server1,
                    num_resources=1,
                    next_module=DisposeModule(
                        name='Dispose 1'
                    )
                )
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
                next_module_orig=SeizeModule(
                    name='Seize Server 1',
                    resource=server1,
                    num_resources=1,
                    next_module=DelayModule(
                        name='Delay 1',
                        delay_generator=exp_generator(1),
                        next_module=ReleaseModule(
                            name='Release Server 1',
                            resource=server1,
                            num_resources=1,
                            next_module=batch_mod_chain
                        )
                    )
                ),
                next_module_dup=SeizeModule(
                    name='Seize Server 2',
                    resource=server2,
                    num_resources=1,
                    next_module=DelayModule(
                        name='Delay 1',
                        delay_generator=tria_generator(0, 2),
                        next_module=ReleaseModule(
                            name='Release Server 2',
                            resource=server2,
                            num_resources=1,
                            next_module=batch_mod_chain
                        )
                    )
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


def testcase_5() -> Tuple[Environment, List[Resource]]:
    """
    Similar to Test Case 4 with a Batch/Separate instead of a Duplicate. Simulates a scenario of a stream of couples
    running errands together in the same shopping district. Entities are first batched into couples, which are then
    delayed for some driving duration to get to their destination. Once they arrive, the couple batches are separated
    and each individual fulfills their own errands, which requires one of two different servers. The couples then meet
    up and realize that one of them forgot some part of their errand. The batched couple revisits one of the previously
    visited servers and departs.
    """

    def assign_last_entity_ind_handler(variables: dict, entity_attr: dict) -> any:
        return variables['last_entity_ind'] + 1

    def assign_couple_ind_handler(variables: dict, entity_attr: dict) -> any:
        return variables['last_entity_ind'] // 2

    def assign_going_to_store1_handler(variables: dict, entity_attr: dict) -> any:
        return variables['last_entity_ind'] % 2 == 0

    def decide_store_handler(variables: dict, entity_attr: dict) -> bool:
        return entity_attr['going_to_store1']

    clerk_store1 = Resource(
        name='Clerk Store 1',
        capacity=3
    )
    clerk_store2 = Resource(
        name='Clerk Store 2',
        capacity=2
    )

    batch_mod_chain = BatchModule(
        name='Re-Batch Couple',
        batch_type=BatchType.ATTRIBUTE,
        batch_size=2,
        batch_attr='couple_ind',
        batch_entity_type='Couple',
        next_module=SeizeModule(
            name='Seize Store 1 Clerk Again',
            resource=clerk_store1,
            num_resources=1,
            next_module=DelayModule(
                name='Delay Store 1 Checkout Again',
                delay_generator=exp_generator(1),
                cost_allocation=CostType.VALUE_ADDED,
                next_module=ReleaseModule(
                    name='Release Store 1 Clerk Again',
                    resource=clerk_store1,
                    num_resources=1,
                    next_module=DisposeModule(
                        name='Depart Shopping District'
                    )
                )
            )
        )
    )

    mean_drive_time_generator = uniform_generator(1, 3)

    mod_chain = CreateModule(
        name='Create Person',
        gen_entity_type='Person',
        arrival_generator=exp_agg_generator(3),
        next_module=AssignModule(
            name='Assign Errands',
            assignments=[
                Assignment(assign_type=AssignType.VARIABLE, assign_name='last_entity_ind', assign_value_handler=assign_last_entity_ind_handler),
                Assignment(assign_type=AssignType.ATTRIBUTE, assign_name='couple_ind', assign_value_handler=assign_couple_ind_handler),
                Assignment(assign_type=AssignType.ATTRIBUTE, assign_name='going_to_store1', assign_value_handler=assign_going_to_store1_handler)
            ],
            next_module=BatchModule(
                name='Create Couple',
                batch_type=BatchType.ATTRIBUTE,
                batch_attr='couple_ind',
                batch_size=2,
                batch_entity_type='Couple',
                next_module=DelayModule(
                    name='Drive to Shopping District',
                    delay_generator=exp_generator(next(mean_drive_time_generator)),
                    cost_allocation=CostType.VALUE_ADDED,
                    next_module=SeparateModule(
                        name='Split up for errands',
                        next_module=DecideTwoWayByConditionModule(
                            name='Choose store to enter',
                            condition_handler=decide_store_handler,
                            true_next_module=SeizeModule(
                                name='Seize Store 1 Clerk',
                                resource=clerk_store1,
                                num_resources=1,
                                next_module=DelayModule(
                                    name='Delay Store 1 Checkout',
                                    delay_generator=exp_generator(1),
                                    cost_allocation=CostType.NON_VALUE_ADDED,
                                    next_module=ReleaseModule(
                                        name='Release Store 1 Clerk',
                                        resource=clerk_store1,
                                        num_resources=1,
                                        next_module=batch_mod_chain
                                    )
                                )
                            ),
                            false_next_module=SeizeModule(
                                name='Seize Store 2 Clerk',
                                resource=clerk_store2,
                                num_resources=1,
                                next_module=DelayModule(
                                    name='Delay Store 2 Checkout',
                                    delay_generator=tria_generator(1, 3.5),
                                    cost_allocation=CostType.VALUE_ADDED,
                                    next_module=ReleaseModule(
                                        name='Release Store 2 Clerk',
                                        resource=clerk_store2,
                                        num_resources=1,
                                        next_module=batch_mod_chain
                                    )
                                )
                            )
                        )
                    )
                )
            )
        )
    )

    env = Environment(
        module_chains=[mod_chain],
        variables=[('last_entity_ind', -1)]
    )
    resources = [clerk_store1, clerk_store2]

    return env, resources


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--case', type=int, default=5, help='Test case number (1, 2, 3, 4, 5).')
    parser.add_argument('-d', '--debug', action='store_true', help='Flag for debugging outputs.')
    parser.add_argument('-D', '--duration', type=float, default=1000., help='Duration of simulation in default time units.')
    args = parser.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)

    match args.case:
        case 1:
            env, resources = testcase_1()
        case 2:
            env, resources = testcase_2()
        case 3:
            env, resources = testcase_3()
        case 4:
            env, resources = testcase_4()
        case 5:
            env, resources = testcase_5()
        case _:
            raise ValueError('Invalid test case number provided:', args.case)

    sys_var, sys_entity_metrics_df = env.run_simulation(args.duration)
    entity_traces = sys_var['entity']['trace']

    # system metric outputs
    print('Entity metrics:')
    print(sys_entity_metrics_df)
    print('-----------')

    print('Mean values by Entity type:')
    print(sys_entity_metrics_df.groupby('Entity Type').mean().drop(columns=['Created At', 'Disposed At']))
    print('-----------')

    print('Median values by Entity type:')
    print(sys_entity_metrics_df.groupby('Entity Type').median().drop(columns=['Created At', 'Disposed At']))
    print('-----------')

    print('Aggregate values by Entity type:')
    print(sys_entity_metrics_df.groupby('Entity Type').sum().drop(columns=['Created At', 'Disposed At']))
    print('-----------')

    print('Total entity system time:', sys_var['metrics']['Total Entity System Time'])
    print('Average entity system time:', sys_var['metrics']['Average Entity System Time'])
    print('-----------')

    # resource metric outputs
    for r in resources:
        resource_utilization = r.calc_utilization(args.duration)
        print(f'{r.name} resource utilization:', resource_utilization)
    print('-----------')

    # sample entity traces
    print('Sample entity trace (Entity 30):')
    for step in entity_traces[30]:
        print(step)
    print()

    mid_ind = len(sys_var['entity']['metrics'].keys()) // 2
    print(f'Sample entity trace (Entity {mid_ind}):')
    for step in entity_traces[mid_ind]:
        print(step)


if __name__ == '__main__':
    main()
