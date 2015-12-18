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
from simpy.events import AllOf


def get_network_latency(latency, bandwidth, queue):
    """Simulate a real world link latency"""
    queue_count = len(queue)
    buffered_size = sum([p['size'] for p in queue])
    max_buffer_size = 9 * 1024 * 1024
    return 0
    return (0.5 + random.random()) * latency * (1 + buffered_size / max_buffer_size)


def get_backoff(backoff_level):
    backoff = float(max(1, random.randint(0, int(min(5*1000, # backoff wait <= 5s
                                                    2**min(30, backoff_level)-1))))) / 1000
    return random.random() / 10
    return backoff


class SimulatorException(Exception):
    pass


def _debugprint(env, msg, do=True):
    if do:
        print("[%8.3f] %s" % (env.now, msg))


class BaseSim(object):
    def __init__(self, do_info=True, do_warning=True, do_debug=False, do_critical=True):
        self.do_info = do_info
        self.do_warning = do_warning
        self.do_debug = do_debug
        self.do_critical = do_critical

    def info(self, msg):
        msg = "INFO\tid:%s\t%s" % (self.id, msg)
        _debugprint(self.env, msg, self.do_info)

    def warning(self, msg):
        msg = "WARN\tid:%s\t%s" % (self.id, msg)
        _debugprint(self.env, msg, self.do_warning)

    def debug(self, msg):
        msg = "DEBUG\tid:%s\t%s" % (self.id, msg)
        _debugprint(self.env, msg, self.do_debug)

    def critical(self, msg):
        msg = "CRITICAL\tid:%s\t%s" % (self.id, msg)
        _debugprint(self.env, msg, self.do_critical)


class Node(BaseSim):
    def __init__(self, env, node_id, ip="127.0.0.1", cpu_cores=4, memory=8*1024*1024*1024, disk=320*1024*1024*1024,
                 disk_speed=80*1024*1024, default_bandwidth=100*1024*1024/8, disk_buffer=512*1024*1024, **kwargs):
        "One node is a resouce entity"
        super(Node, self).__init__(**kwargs)

        self.env = env
        self.id = node_id

        #: need to contend for the CPU resource
        self.cpu_cores = cpu_cores
        self.memory = memory
        # assume we are SMP
        self.disk = disk
        self.ip = ip
        self.memory_speed = 10 * 1024 * 1024 * 1024

        self.memory_controller = simpy.Resource(self.env, capacity=1)
        self.disk_buffer = simpy.Container(self.env, init=disk_buffer, capacity=disk_buffer)
        self.bandwidth = simpy.Container(self.env, init=default_bandwidth, capacity=default_bandwidth)
        #self.bandwidth = default_bandwidth
        #self.link = simpy.Resource(self.env, capacity=1)
        self.link = simpy.PriorityResource(self.env, capacity=1)
        self.set_disk_speed(disk_speed)

        self.disk_events = {}
        self.active_disk_events = {}
        self.event_id = 0
        self.is_disk_alive = True
        self.disk_buffer_flush_frequency = 30

        self.disk_alive = self.env.event()
        self.disk_alive.succeed("initial disk is fine")
        self.disk_buffer_full = self.env.event()
        self.init_disk_flush_loop()

    def set_disk_speed(self, disk_speed):
        self.disk_speed = simpy.Container(self.env, init=disk_speed, capacity=disk_speed)

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
        self.info("start disk flush loop")
        self.env.process(self._flush_disk_when_full())

    def _flush_disk_when_full(self):
        while True:
            flush_frequency = self.env.timeout(self.disk_buffer_flush_frequency)
            
            # when buffer is full or it reaches flush frequency
            yield self.disk_buffer_full | flush_frequency
            buffered_bytes = self.disk_buffer.capacity - self.disk_buffer.level
            self.debug("DISK_FLUSH_START\t%4.2f KB" % (buffered_bytes/1024))
            if buffered_bytes > 0:
                # then flush cache TODO: here is assuming we acquire the disk exclusively
                flush_time = float(buffered_bytes) / self.disk_speed.capacity
                yield self.env.timeout(flush_time)
                yield self.disk_buffer.put(buffered_bytes)
                self.info("DISK_FLUSH_COMPLETE\t%4.2fs" % (flush_time))
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
                    self.debug("BUFFER_FULL:%s sleep %4.2f\t%4.2f/%4.2f MB"
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
                    self.debug("DISK_WROTE_ONCE:%s\t%4.2f KB: %4.2f/%4.2f MB"
                               % (event_id, writable_bytes/1024, written_bytes/1024/1024, total_bytes/1024/1024))
        self.info("DISK_WROTE:%s\t%4.2f MB"
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
                self.debug("%s\tgot speed\t%4.2f/%4.2f MB written\tspeed_request: %4.1f/%4.1f MB/s"
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
                    self.debug("%s interrupted\t%s\tprogress: %4.2f MB/%4.2f MB" % (event_id, e, written_bytes/1024/1024, total_bytes/1024/1024))
                    continue
                break
            else:
                # It MUST wait for a random time to allow interrupted tasks to yield their disk IO
                try:
                    yield self.env.timeout(random.random())
                except simpy.Interrupt: # there is no point to interrupt a poor guy, so just let me ignore that
                    continue
                self.debug("%s\tinterrupting\t%4.2f/%4.2f MB written\tspeed_request: %4.1f/%4.1f MB/s"
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
        self.info("%s\t%i MB written\t%i MB/s bandwidth released\tidle disk: %i MB/s"
                   % (event_id, total_bytes/1024/1024, current_speed/1024/1024, self.disk_speed.level/1024/1024))


class Switch(BaseSim):
    def __init__(self, env, switch_id="switch", default_bandwidth=100*1024*1024/8, latency=0.001, **kwargs):
        super(Switch, self).__init__(**kwargs)

        self.env = env
        self.bandwidth = default_bandwidth
        self.network = {}
        self.id = switch_id
        self.latency = latency

        #: e.g., {("192.168.0.1", "172.16.0.1"): 3}: ping from 192.168.0.1 to 172.16.0.1 every 3s
        self.heartbeats = {}
 
    def add_node(self, node):
        self.network[node.id] = {
            "node": node,
            "backoff_level": 0,
            "queue": [],
            "active": self.env.event(),
        }
        #self.serve_link(node.id)

    def serve_link(self, node_id):
        self.env.process(self._serve_link(node_id))

    def _serve_link(self, node_id):
        the_bandwidth = self.network[node_id]["node"].bandwidth
        self.info("Start serving link between %s and %s: %4.2f MB/s" % (self.id, node_id, float(the_bandwidth)/1024/1024))
        while True:
            # wating event succeed, which indicates there is event coming
            yield self.network[node_id]["active"]
            while self.network[node_id]["queue"]:
                with self.network[node_id]["node"].link.request() as req:
                    yield req

                    packet_event = self.network[node_id]["queue"].pop(0)
                    if packet_event['throttle_bandwidth'] > 0:
                        the_bandwidth = min(the_bandwidth, packet_event['throttle_bandwidth'])
                    the_latency = get_network_latency(self.latency, the_bandwidth, self.network[node_id]["queue"])
                    + float(packet_event['size'])/the_bandwidth
                    yield self.env.timeout(the_latency)
                    self.debug("DOWN:%s->%s: %i KB %4.2f MB/s %ims" %
                               (packet_event['from'], node_id, float(packet_event['size']) / 1024, float(the_bandwidth) / 1024 / 1024, the_latency * 1000))
                    packet_event['event'].succeed()
            # queue is empty, let me reset the event
            self.network[node_id]["active"] = self.env.event()

    def process_ping(self, from_node_id, to_node_id, packet_size, throttle_bandwidth=-1):
        return self.env.process(self._ping(from_node_id, to_node_id, packet_size, delay=0, throttle_bandwidth=throttle_bandwidth))

    def stop_heartbeat(self, from_node_id, to_node_id):
        self.heartbeats.pop(from_node_id, to_node_id)

    def start_heartbeat(self, from_node_id, to_node_id, packet_size, interval=1):
        self.env.process(self._heartbeat_ping(from_node_id, to_node_id, packet_size, interval))

    def _heartbeat_ping(self, from_node_id, to_node_id, packet_size, interval):
        self.info("start heartbeat from %s to %s every %ss" % (from_node_id, to_node_id, interval))
        self.heartbeats[(from_node_id, to_node_id)] = interval
        while self.heartbeats.get((from_node_id, to_node_id)):
            current_interval = self.heartbeats[(from_node_id, to_node_id)]
            yield self.process_ping(from_node_id, to_node_id, packet_size)
            yield self.env.timeout(current_interval)
        self.info("start heartbeat from %s to %s every %ss" % (from_node_id, to_node_id, current_interval))

    def _ping(self, from_node_id, to_node_id, packet_size=16*1024, delay=0, throttle_bandwidth=-1):
        """TODO: need to implement slow start"""
        if delay > 0:
            yield self.env.timeout(delay)

        sent_size = 0
        while sent_size < packet_size:
            require_bandwidth = int(min(self.network[from_node_id]['node'].bandwidth.level, self.network[to_node_id]['node'].bandwidth.level))
            if throttle_bandwidth >= 0:
                require_bandwidth = min(require_bandwidth, throttle_bandwidth)

            if require_bandwidth > 0:
                yield self.network[from_node_id]['node'].bandwidth.get(require_bandwidth) & self.network[to_node_id]['node'].bandwidth.get(require_bandwidth)
                yield self.env.timeout(self.latency)
                need_time = float(packet_size-sent_size) / require_bandwidth
                # TODO: may be interrupted here
                yield self.env.timeout(need_time)
                yield self.env.timeout(self.latency)
                self.debug("%s->%s:%4.0f/%4.0f KB\ttime=%4.2fms" %
                           (from_node_id, to_node_id, float(packet_size-sent_size) / 1024, float(packet_size) / 1024, need_time * 1000))

                yield self.network[from_node_id]['node'].bandwidth.put(require_bandwidth) & self.network[to_node_id]['node'].bandwidth.put(require_bandwidth)

                sent_size += need_time * require_bandwidth
            else:
                sleep_time = random.random()
                self.debug("%s->%s bandwidth <= 0, throttle_bandwidth: %i, sleep %4.2fs" %
                           (from_node_id, to_node_id, throttle_bandwidth, sleep_time))
                yield self.env.timeout(sleep_time)
            

class NameNode(Node):
    def __init__(self, env, node_id, hdfs=None, **kwargs):
        super(NameNode, self).__init__(env, node_id, **kwargs)
        #: store files' placement
        self.metadata = {}
        self.datanodes = {}
        self.hdfs = hdfs

    def __str__(self):
        return "%s:\n%s" % (self.id, self.metadata)
        
    def query_file(self, file_name):
        return self.metadata.get(file_name)

    def find_datanodes_for_new_file(self, file_name, size, replica_number):
        return random.sample(self.datanodes.keys(), min(replica_number, len(self.datanodes)))

    def register_file(self, file_name, datanode_names):
        self.metadata[file_name] = datanode_names

        
class DataNode(Node):
    def __init__(self, env, node_id, hdfs=None, **kwargs):
        super(DataNode, self).__init__(env, node_id, **kwargs)
        self.hdfs = hdfs
        self.doing_block_report = False

    def get_block_report(self):
        #: TODO: I am assuming block report is a static value, not generated according to the stored blocks
        block_report_size = 1234 * 1024
        return block_report_size

    def start_block_report(self, interval):
        self.env.process(self._start_block_report(interval))

    def _start_block_report(self, interval):
        self.doing_block_report = True
        self.info("start block report every %is" % interval)
        while self.doing_block_report:
            report_size = self.get_block_report()
            yield self.hdfs.switch.process_ping(self.id, self.hdfs.namenode.id, report_size)
            yield self.env.timeout(interval)
        self.info("end block report")


def main():
    """Main function only in command line"""
    from sys import argv
    print("Plase run:\npython -m unittest default_test.py -v")
    
    
if __name__ == '__main__':
    main()
