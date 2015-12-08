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


def _debugprint(env, msg):
    print("[%8.2f] %s" % (env.now, msg))


class Node(object):
    def __init__(self, env, node_id, ip="127.0.0.1", cpu_cores=4, memory=8*1024*1024*1024, disk=320*1024*1024*1024, disk_speed=100*1024*1024, default_bandwidth=100*1024*1024/8, disk_buffer=64*1024*1024):
        "One node is a resouce entity"
        self.env = env
        self.node_id = node_id

        #: need to contend for the CPU resource
        self.cpu_cores = cpu_cores
        self.memory = memory
        # assume we are SMP
        self.disk = disk
        self.ip = ip
        self.memory_speed = 10 * 1024 * 1024 * 1024

        self.memory_controller = simpy.Resource(self.env, capacity=1)
        self.disk_speed = simpy.Container(self.env, init=disk_speed, capacity=disk_speed)
        self.disk_buffer = simpy.Container(self.env, init=disk_buffer, capacity=disk_buffer)
        self.bandwidth = simpy.Container(self.env, init=default_bandwidth, capacity=default_bandwidth)

        self.disk_events = {}
        self.active_disk_events = {}
        self.event_id = 0
        self.is_disk_alive = True
        self.disk_buffer_flush_frequency = 30

        self.disk_alive = self.env.event()
        self.disk_alive.succeed("initial disk is fine")
        self.disk_buffer_full = self.env.event()
        self.init_disk_flush_loop()

    def print(self, msg):
        msg = "node:%s\t%s" % (self.node_id, msg)
        _debugprint(self.env, msg)

    def process_break_disk(self, delay=0):
        self.env.process(self._break_disk(delay))

    def _break_disk(self, delay=0):
        if delay > 0:
            yield self.env.timeout(delay)
        self.disk_alive = self.env.event()
        for k, e in self.active_disk_events.items():
            e.interrupt({"info": "Disk gets broken", "time": self.env.now})

    def process_repair_disk(self, delay=0):
        self.env.process(self._repair_disk(delay))

    def _repair_disk(self, delay=0):
        if delay > 0:
            yield self.env.timeout(delay)
        self.disk_alive.succeed()

    def new_disk_write_request(self, total_bytes, delay=0):
        """This is called by client"""
        self.event_id += 1
        event_id = self.event_id
        new_event = self.env.process(self._write_disk(total_bytes, event_id, delay))
        self.disk_events[event_id] = new_event
        return new_event

    def new_disk_buffer_write_request(self, total_bytes, delay=0):
        self.event_id += 1
        event_id = self.event_id
        new_event = self.env.process(self._write_disk_buffer(total_bytes, event_id, delay))
        return new_event

    def init_disk_flush_loop(self):
        self.env.process(self._flush_disk_when_full())

    def _flush_disk_when_full(self):
        while True:
            flush_frequency = self.env.timeout(self.disk_buffer_flush_frequency)
            
            # when buffer is full or it reaches flush frequency
            yield self.disk_buffer_full | flush_frequency
            buffered_bytes = self.disk_buffer.capacity - self.disk_buffer.level
            self.print("DISK_FLUSH_START\t%4.2f KB" % (buffered_bytes/1024))
            if buffered_bytes > 0:
                # then flush cache TODO: here is assuming we acquire the disk exclusively
                flush_time = float(buffered_bytes) / self.disk_speed.capacity
                yield self.env.timeout(flush_time)
                yield self.disk_buffer.put(buffered_bytes)
                self.print("DISK_FLUSH_COMPLETE\t%4.2f" % (flush_time))
            self.disk_buffer_full = self.env.event()

    def _write_disk_buffer(self, total_bytes, event_id, delay=0):
        if delay > 0:
            yield self.env.timeout(delay)
        written_bytes = 0
        while written_bytes < total_bytes:
            # firstly, acquire memory controller
            with self.memory_controller.request() as req:
                yield req
                if self.disk_buffer.level == 0: # if buffer is full
                    sleep_time = random.random()
                    self.print("%s\tdisk_buffer_full, will sleep %4.2f\t%4.2f/%4.2f MB"
                               % (event_id, sleep_time, written_bytes/1024/1024, total_bytes/1024/1024))
                    yield self.env.timeout(sleep_time)
                else: # if buffer has space to write
                    writable_bytes = min(total_bytes - written_bytes, self.disk_buffer.level)
                    disk_buffer_write_time = float(writable_bytes) / self.memory_speed
                    # acquire the disk buffer space to prevent from others' intrude
                    yield self.disk_buffer.get(writable_bytes)
                    yield self.env.timeout(disk_buffer_write_time)
                    # trigger disk_buffer_full only after I have completed memory write
                    if self.disk_buffer.level == 0:
                        self.disk_buffer_full.succeed()
                    written_bytes += writable_bytes
                    self.print("%s\twrote %4.2f KB\t%4.2f/%4.2f MB"
                               % (event_id, writable_bytes/1024, written_bytes/1024/1024, total_bytes/1024/1024))
        self.print("%s\tFINISHED\t%4.2f MB"
                   % (event_id, written_bytes/1024/1024))

    def _write_disk(self, total_bytes, event_id, delay=0):
        if delay > 0:
            yield self.env.timeout(delay)
        self.active_disk_events[event_id] = self.disk_events[event_id]
        written_bytes = 0
        current_speed = 0

        while written_bytes < total_bytes:
            yield self.disk_alive

            if current_speed > 0 and self.disk_speed.level < self.disk_speed.capacity:
                try:
                    yield self.disk_speed.put(min(current_speed, self.disk_speed.capacity - self.disk_speed.level))
                except simpy.Interrupt as e: # try again to put disk control back
                    continue
            #: it should slow down, since we are adding new disk write event
            ideal_speed = int(float(self.disk_speed.capacity) / len(self.active_disk_events))

            if ideal_speed <= self.disk_speed.level:
                self.print("%s\tgot speed\t%4.2f/%4.2f MB written\tspeed_request: %4.1f/%4.1f MB/s"
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
                    self.print("%s interrupted\t%s\tprogress: %4.2f MB/%4.2f MB" % (event_id, e, written_bytes/1024/1024, total_bytes/1024/1024))
                    continue
                break
            else:
                # It MUST wait for a random time to allow interrupted tasks to yield their disk IO
                try:
                    yield self.env.timeout(random.random())
                except simpy.Interrupt: # there is no point to interrupt a poor guy, so just let me ignore that
                    continue
                self.print("%s\tinterrupting\t%4.2f/%4.2f MB written\tspeed_request: %4.1f/%4.1f MB/s"
                           % (event_id, written_bytes/1024/1024, total_bytes/1024/1024, ideal_speed/1024/1024, self.disk_speed.level/1024/1024))
                for k, e in self.active_disk_events.items():
                    if k != event_id:
                        e.interrupt({"info": "Task %s needs disk" % event_id, "time": self.env.now})

        # event finished
        self.disk_events.pop(event_id)
        self.active_disk_events.pop(event_id)
        if current_speed > 0 and self.disk_speed.level < self.disk_speed.capacity:
            yield self.disk_speed.put(min(current_speed, self.disk_speed.capacity - self.disk_speed.level))
        for k, e in self.active_disk_events.items():
            e.interrupt({"info": "%s release disk" % event_id, "time": self.env.now})
        self.print("%s\t%i MB written\t%i MB/s bandwidth released\tidle disk: %i MB/s"
                   % (event_id, total_bytes/1024/1024, current_speed/1024/1024, self.disk_speed.level/1024/1024))


class Switch(object):
    def __init__(self, env, default_bandwidth=100*1024*1024/8, latency=0.01):
        self.env = env
        self.bandwidth = default_bandwidth
        self.network = {}
        self.node_id = 0
        self.latency = latency

    def print(self, msg):
        _debugprint(self.env, msg)
 
    def add_node(self, node):
        self.network[node.node_id] = node

    def process_ping(self, from_node_id, to_node_id, packet_size, seq=0):
        self.env.process(self._ping(from_node_id, to_node_id, packet_size, seq))

    def heartbeat_ping(self, from_node_id, to_node_id, packet_size):
        i = 0
        while i < 120:
            self.env.run(until=self.env.now + 1)
            self.process_ping(from_node_id, to_node_id, packet_size, i)
            i += 1

    def _ping(self, from_node_id, to_node_id, packet_size=16*1024, seq=0):
        if self.network[from_node_id].bandwidth.level > 0 and self.network[to_node_id].bandwidth.level > 0:
            require_bandwidth = min(self.network[from_node_id].bandwidth.level, self.network[to_node_id].bandwidth.level)
            yield self.network[from_node_id].bandwidth.get(require_bandwidth) & self.network[to_node_id].bandwidth.get(require_bandwidth)
            need_time = 2 * self.latency + float(packet_size) / require_bandwidth
            # TODO: may be interrupted here
            yield self.env.timeout(need_time)
            self.print("%s: %i bytes from %s: seq=%i ttl=63 time=%4.2fms" % (from_node_id, packet_size, to_node_id, seq, need_time * 1000))
            yield self.network[from_node_id].bandwidth.put(require_bandwidth) & self.network[to_node_id].bandwidth.put(require_bandwidth)


def main():
    """Main function only in command line"""
    from sys import argv
    print("Plase run:\npython -m unittest default_test.py -v")
    
    
if __name__ == '__main__':
    main()
