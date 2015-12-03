#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Brief Summary
Attributes:

Google Python Style Guide:
http://google-styleguide.googlecode.com/svn/trunk/pyguide.html
"""
__copyright__ = "Zhaoyu Luo"

import random

import simpy
from simpy.events import AnyOf


class SimulatorException(Exception):
    pass


env = simpy.Environment()


def debugprint(msg):
    print("[%8.2f] %s" % (env.now, msg))


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
        return new_event

    def create_disk_write_event(self, total_bytes, event_id, start_time=0):
        if start_time > 0:
            yield self.env.timeout(start_time)
        written_bytes = 0
        current_speed = 0

        while written_bytes < total_bytes:
            if current_speed > 0 and self.disk_speed.level < self.disk_speed.capacity:
                try:
                    yield self.disk_speed.put(min(current_speed, self.disk_speed.capacity - self.disk_speed.level))
                except simpy.Interrupt as e: # try again to put disk control back
                    continue
            #: it should slow down, since we are adding new disk write event
            ideal_speed = int(float(self.disk_speed.capacity) / len(self.disk_events))

            if ideal_speed <= self.disk_speed.level:
                debugprint("%s\tgot speed\t%4.2f/%4.2f MB written\tspeed_request: %4.1f/%4.1f MB/s"
                           % (event_id, written_bytes/1024/1024, total_bytes/1024/1024, ideal_speed/1024/1024, self.disk_speed.level/1024/1024))
                request_ideal_disk = self.disk_speed.get(ideal_speed)
                timeout = 0.01
                request_timeout = self.env.timeout(timeout)
                try:
                    yield request_ideal_disk | request_timeout
                except simpy.Interrupt as e: # restart to recalculate optimal disk speed
                    continue
                # request must success
                assert request_timeout.processed == False and request_ideal_disk.processed == True

                current_speed = ideal_speed
                estimated_finish_time = (total_bytes - written_bytes) / current_speed
                start_time = self.env.now
                try:
                    yield self.env.timeout(estimated_finish_time)
                except simpy.Interrupt as e:
                    written_bytes += current_speed * (e.cause['time'] - start_time)
                    debugprint("%s interrupted\t%s\tprogress: %4.2f MB/%4.2f MB" % (event_id, e, written_bytes/1024/1024, total_bytes/1024/1024))
                    continue
                break
            else:
                debugprint("%s\tinterrupting\t%4.2f/%4.2f MB written\tspeed_request: %4.1f/%4.1f MB/s"
                           % (event_id, written_bytes/1024/1024, total_bytes/1024/1024, ideal_speed/1024/1024, self.disk_speed.level/1024/1024))
                for k, e in self.disk_events.items():
                    if k != event_id:
                        e.interrupt({"info": "Task %s needs disk" % event_id, "time": self.env.now})
                # It MUST wait for a random time to allow interrupted tasks to yield their disk IO
                try:
                    yield self.env.timeout(random.random())
                except simpy.Interrupt: # there is no point to interrupt a poor guy, so just let me ignore that
                    pass

        # event finished
        self.disk_events.pop(event_id)
        if current_speed > 0 and self.disk_speed.level < self.disk_speed.capacity:
            yield self.disk_speed.put(min(current_speed, self.disk_speed.capacity - self.disk_speed.level))
        for k, e in self.disk_events.items():
            e.interrupt({"info": "%s release disk" % event_id, "time": self.env.now})
        debugprint("%s\t%i MB written\t%i MB/s bandwidth released\tNow: %i MB/s"
                   % (event_id, total_bytes/1024/1024, current_speed/1024/1024, self.disk_speed.level/1024/1024))


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
            debugprint("%s: %i bytes from %s: time=%4.2fms" % (from_node_id, packet_size, to_node_id, need_time * 1000))
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
