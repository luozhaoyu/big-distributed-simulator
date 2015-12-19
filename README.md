# big-distributed-simulator
In fact, it is not a big distributed simulator

## Approaches
* Use [discrete event simulation] (https://en.wikipedia.org/wiki/Discrete_event_simulation), and implement three phase simulation when there is complicated interactions
    * <https://github.com/luozhaoyu/big-distributed-simulator/issues/1>
* Base on [SimPy] (https://pypi.python.org/pypi/simpy) and Python 3.5

## Features
* A wide of parameters could be customized: replica number, number of datanodes, heart beat interval, heartbeat size, block report interval, data block balance bandwidth, client write packet size, disk speed, NIC bandwidth, disk write buffer.
* HDFS heartbeat, block report and hard disk write cache could be enabled or disabled.
* Pipelined block writes.
* Break one disk and repair it.

## Branches
* There currently 4 branches (include master), which corresponds to one network simulation model:
    * master branch: latest and recommend network simulation
    * network branch: super switch model, the switch is big enough to buffer all the packets, each link has a dedicated FIFO queue
    * worse branch: CSMA model with trivial random backoff, each link has a corresponding mutex lock. Each packet should acquire 2 mutex locks before sending, and it would backoff if it fails to acquire the lock within customized timeout
    * csma branch: similar with worse branch, but with an exponential backoff

## Install
1. install Python 3.3 and pip
- `pip install simpy`

## Run
* run tests: `python -m unittest default_test.py`
* generate report: `make report`
* use the command line tool: `python hdfs.py -h`
