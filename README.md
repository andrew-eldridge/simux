# Simux: Discrete-Event Simulation in Python
Simux is a discrete-event simulation API built in the Processing Network (PN) 
paradigm and modeled after Arena's module-based design. Simux is meant to be an 
easy-to-use entry point for discrete-event simulation in Python; it abstracts away 
the more complex process-interaction logic needed to develop SimPy simulations and includes 
several pre-built random variate generator functions.

## Project Structure
- **datatypes.py:** contains internal type and enum definitions
- **environment.py:** defines Environment class (entry point to build an environment and run a simulation)
- **generators.py:** contains several pre-built generator and callback functions for common distributions
- **modules.py:** contains Module and subclass definitions
- **testcase.py:** contains multiple test cases for different environments

## Setup and Test Cases
The following commands set up the project and run test cases:
```
git clone https://github.com/andrew-eldridge/simux
cd simux
pip install -r requirements.txt
python testcase.py [--case] [--debug] [--duration]
```

## API Usage
### Creating an environment
Environments in Simux require at minimum a single module chain to be defined. 
A module chain simply refers to an instance of an `ArrivalModule`, which necessarily 
has a chain of modules attached to it and eventually terminates with a Dispose module.

A minimal environment can be set up as follows:

```python
from modules import *
from generators import exp_agg_generator
from environment import Environment

mod_chain = CreateModule(
    name='Test Create',
    gen_entity_type='Test Entity',
    arrival_generator=exp_agg_generator(1),
    next_module=DisposeModule(
        name='Test Dispose'
    )
)

env = Environment(
    module_chains=[mod_chain]
)
```

### Running a simulation
Once an environment has been created, replications of a simulation can be run by invoking the 
"run_simulation" method any number of times on the environment:

```python
sys_var, sys_entity_metrics_df = env.run_simulation(duration=100)
```

### Analyzing outputs
After a replication of the simulation completes, a dictionary of system variables and a DataFrame of entity 
metrics are returned, as shown above. These values can be manipulated to produce the desired output metrics:

```python
mean_values_by_entity_type = sys_entity_metrics_df.groupby('Entity Type').mean().drop(columns=['Created At', 'Disposed At'])
median_values_by_entity_type = sys_entity_metrics_df.groupby('Entity Type').median().drop(columns=['Created At', 'Disposed At'])
agg_values_by_entity_type = sys_entity_metrics_df.groupby('Entity Type').sum().drop(columns=['Created At', 'Disposed At'])
```
