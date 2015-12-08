# big-distributed-simulator
In fact, it is not a big distributed simulator

## Approaches
* Use [discrete event simulation] (https://en.wikipedia.org/wiki/Discrete_event_simulation), and implement three phase simulation when there is complicated interactions
    * <https://github.com/luozhaoyu/big-distributed-simulator/issues/1>
* Base on [SimPy] (https://pypi.python.org/pypi/simpy) and Python 3.5

## Features
* Could support concurrent tasks within one node (resource contention)
* Could run tasks follow their dependencies, e.g., reduce depends on the finish of map

## TODO
* find a way to monitor discrete events
* find a way to communicate between processes (events), e.g., broadcasting

## Run
`make`
