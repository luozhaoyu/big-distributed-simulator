#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Brief Summary
Attributes:

Google Python Style Guide:
http://google-styleguide.googlecode.com/svn/trunk/pyguide.html
"""
__copyright__ = "Zhaoyu Luo"

import simpy
from simpy.events import AnyOf


class SimulatorException(Exception):
    pass


env = simpy.Environment()


def debugprint(msg):
    print("[%6.2f] %s" % (env.now, msg))


class Node(object):
    def __init__(self, env, node_id, ip="127.0.0.1", cpu_cores=4, memory=8*1024*1024*1024, disk=320*1024*1024*1024, disk_speed=100*1024*1024, default_bandwidth=100*1024*1024/8):
        "One node is a resouce entity"
        self.env = env
        self.node_id = node_id

        #: need to contend for the CPU resource
        self.cpu_cores = cpu_cores
        self.memory = memory
        self.disk = disk
        self.disk_speed = simpy.Container(self.env, init=disk_speed, capacity=disk_speed)
        self.disk_events = {}
        self.event_id = 0
        self.ip = ip
        self.bandwidth = simpy.Container(self.env, init=default_bandwidth, capacity=default_bandwidth)

    def random_write_tasks(self):
        current_time = 0
        task_time = [1, 1, 2, 3, 3, 3, 4, 9, 9, 9, 30]
        for i in task_time:
            if i == current_time:
                self.new_disk_write_request(1001*1024*1024)
            elif i > current_time:
                try:
                    self.env.run(until=i)
                except SimulatorException as e:
                    print(e)
                self.new_disk_write_request(1001*1024*1024)
            current_time = i

    def new_disk_write_request(self, total_bytes, start_time=0):
        """This is called by client"""
        self.event_id += 1
        event_id = self.event_id
        new_event = self.env.process(self.create_disk_write_event(total_bytes, event_id, start_time))
        self.disk_events[event_id] = new_event

    def create_disk_write_event(self, total_bytes, event_id, start_time=0):
        if start_time > 0:
            yield self.env.timeout(start_time)
        written_bytes = 0
        current_speed = 0

        while written_bytes < total_bytes:
            try:
                if current_speed > 0 and self.disk_speed.level < self.disk_speed.capacity:
                    yield self.disk_speed.put(min(current_speed, self.disk_speed.capacity - self.disk_speed.level))
                #: it should slow down, since we are adding new disk write event
                ideal_speed = int(float(self.disk_speed.capacity) / len(self.disk_events))
                debugprint("%s\tbytes_written: %s/%s\tspeed_request: %s/%s" % (event_id, written_bytes, total_bytes, ideal_speed, self.disk_speed.level))
                # Here is a compromise: if I could not obtain disk right now, I will wait for 1 second
                # then I interrupt others to gain disk
                request_ideal_disk = self.disk_speed.get(ideal_speed)
                timeout = 0.1
                request_timeout = self.env.timeout(timeout)

                try:
                    yield request_ideal_disk | request_timeout
                except simpy.Interrupt as e:
                    debugprint(e)
                    continue
                except SimulatorException as e:
                    print(e)
                    continue

                if request_ideal_disk.processed: # I have got disk!
                    current_speed = ideal_speed
                    estimated_finish_time = (total_bytes - written_bytes) / current_speed
                    debugprint("%s\tobtained_speed/total_speed: %6.2f/%6.2f\testimated_finish_time: %6.2f"
                               % (event_id, current_speed, self.disk_speed.capacity - self.disk_speed.level, estimated_finish_time + self.env.now))
                    try:
                        yield self.env.timeout(estimated_finish_time)
                    except simpy.Interrupt as e:
                        written_bytes += current_speed * (self.env.now - e.cause['time'])
                        debugprint("%s interrupted\t%s\tbytes_written: %s/%s" % (event_id, e, written_bytes, total_bytes))
                        continue
                    break
                else: # interrupt others!
                   # try:
                   #     request_ideal_disk.fail(SimulatorException("fail myself"))
                   # except SimulatorException as e:
                   #     print(e)
                    if self.disk_speed.capacity > self.disk_speed.level:
                        yield self.disk_speed.put(self.disk_speed.capacity - self.disk_speed.level)
                    debugprint("%s\tis interrupting others!" % event_id)
                    for k, e in self.disk_events.items():
                        if k != event_id:
                            e.interrupt({"info": "make a space for new task", "time": self.env.now - timeout})
            except simpy.Interrupt as e:
                raise e("it should not happen")

        # event finished
        self.disk_events.pop(event_id)
        debugprint("%s\tFINISHED\t%s B written" % (event_id, total_bytes))


class Switch(object):
    def __init__(self, env, default_bandwidth=100*1024*1024/8, latency=0.01):
        self.env = env
        self.bandwidth = default_bandwidth
        self.network = {}
        self.node_id = 0
        self.latency = latency

    def add_node(self, node):
        self.network[node.node_id] = node

    def process_ping(self, from_node_id, to_node_id, packet_size):
        self.env.process(self._ping(from_node_id, to_node_id, packet_size))

    def heartbeat_ping(self, from_node_id, to_node_id, packet_size):
        i = 0
        while i < 120:
            self.env.run(until=self.env.now + 1)
            self.process_ping(from_node_id, to_node_id, packet_size)
            i += 1

    def _ping(self, from_node_id, to_node_id, packet_size=16*1024):
        if self.network[from_node_id].bandwidth.level > 0 and self.network[to_node_id].bandwidth.level > 0:
            require_bandwidth = min(self.network[from_node_id].bandwidth.level, self.network[to_node_id].bandwidth.level)
            yield self.network[from_node_id].bandwidth.get(require_bandwidth) & self.network[to_node_id].bandwidth.get(require_bandwidth)
            debugprint("%s is pinging %s" % (from_node_id, to_node_id))
            need_time = 2 * self.latency + float(packet_size) / require_bandwidth
            # TODO: may be interrupted here
            yield self.env.timeout(need_time)
            debugprint("%s finished pinging %s: %4.2fms" % (from_node_id, to_node_id, need_time * 1000))
            yield self.network[from_node_id].bandwidth.put(require_bandwidth) & self.network[to_node_id].bandwidth.put(require_bandwidth)
        


def main():
    """Main function only in command line"""
    from sys import argv
    node = Node(env, 1)
    node2 = Node(env, 2)

    node.random_write_tasks()

    switch = Switch(env)
    switch.add_node(node)
    switch.add_node(node2)
    switch.heartbeat_ping(node.node_id, node2.node_id, 1024)

    env.run()
    
    
if __name__ == '__main__':
    main()
