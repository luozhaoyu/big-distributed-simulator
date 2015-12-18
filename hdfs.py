#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Brief Summary
Attributes:

Google Python Style Guide:
    http://google-styleguide.googlecode.com/svn/trunk/pyguide.html
"""
__copyright__ = "Zhaoyu Luo"

import argparse
import random

import simpy
from simpy.events import AllOf

import node


class HDFS(node.BaseSim):
    """By default, HDFS owns one switch and one client machine, it would instantiate that automatically
    The client machine would only be used to submit its task
    """

    def __init__(self, env, namenode, replica_number=3, heartbeat_interval=3, heartbeat_size=1024,
                 enable_datanode_cache=True, enable_heartbeats=True, enable_block_report=True,
                 block_report_interval=30, balance_bandwidth=1024*1024, client_write_packet_size=1024*1024,
                 **kwargs):
        super(HDFS, self).__init__(**kwargs)

        self.env = env
        self.id = "HDFS"

        self.client_write_packet_size = client_write_packet_size
        self.block_size = 64 * 1024 * 1024
        self.replica_number = replica_number
        self.enable_datanode_cache = enable_datanode_cache
        self.enable_heartbeats = enable_heartbeats
        self.enable_block_report = enable_block_report
        self.heartbeat_size = heartbeat_size
        self.heartbeat_interval = heartbeat_interval
        self.block_report_interval = block_report_interval
        #: dfs.datanode.balance.bandwidthPerSec
        self.balance_bandwidth = balance_bandwidth

        self.switch = node.Switch(env, **kwargs)
        self.client = node.Node(env, "client", **kwargs)
        self.switch.add_node(self.client)

        self.datanodes = {}
        if namenode:
            self.set_namenode(namenode)

    def start_services(self):
        if self.enable_heartbeats:
            self.start_hdfs_heartbeat()
        if self.enable_block_report:
            self.start_block_report()

    def run_forever(self):
        self.start_services()
        self.env.run()

    def run_until(self, until):
        self.start_services()
        self.env.run(until)

    def start_block_report(self):
        if len(self.datanodes) < 1 or not self.namenode:
            self.critical("fail to start block report: no datanode exists")
            return

        for node_name in self.datanodes:
            self.datanodes[node_name].start_block_report(self.block_report_interval)
        self.critical("start HDFS block report")

    def start_hdfs_heartbeat(self):
        if len(self.datanodes) < 1 or not self.namenode:
            self.critical("fail to start HDFS heartbeat: no datanode exists")
            return

        for node_name in self.datanodes:
            self.switch.start_heartbeat(node_name, self.namenode.id, self.heartbeat_size, self.heartbeat_interval)
        self.critical("start HDFS heartbeat")

    def set_namenode(self, node):
        self.namenode = node
        self.switch.add_node(node)
        self.datanodes = self.namenode.datanodes

    def create_datanode(self, node_id, **kwargs):
        datanode = node.DataNode(self.env, node_id, hdfs=self,
                                 do_debug=self.do_debug, do_info=self.do_info, do_warning=self.do_warning, do_critical=self.do_critical,
                                 **kwargs)
        self.add_datanode(datanode)

    def add_datanode(self, node):
        self.datanodes[node.id] = node
        self.switch.add_node(node)

    def transfer_data(self, from_node_id, to_node_id, size, throttle_bandwidth=-1):
        return self.env.process(self._transfer_data(from_node_id, to_node_id, size, throttle_bandwidth))

    def _transfer_data(self, from_node_id, to_node_id, size, throttle_bandwidth=-1):
        yield self.switch.process_ping(from_node_id, to_node_id, size, throttle_bandwidth)
        if self.enable_datanode_cache:
            yield self.datanodes[to_node_id].new_disk_buffer_write_request(size)
        else:
            yield self.datanodes[to_node_id].new_disk_write_request(size)

    def replicate_file(self, file_name, size, node_sequence, throttle_bandwidth=-1):
        return self.env.process(self._replicate_file(file_name, size, node_sequence, throttle_bandwidth))

    def _replicate_file(self, file_name, size, node_sequence, throttle_bandwidth=-1):
        self.info("REPLICATING\t%s\tin %s" % (file_name, node_sequence))
        i = 0
        while i < len(node_sequence) - 1:
            yield self.transfer_data(node_sequence[i], node_sequence[i+1], size, throttle_bandwidth)
            i += 1
        self.info("REPLICATED\t%s\tin %s" % (file_name, node_sequence))

    def create_file(self, file_name, size, node_sequence, throttle_bandwidth=-1):
        return self.env.process(self._create_file(file_name, size, node_sequence, throttle_bandwidth))

    def _create_file(self, file_name, size, node_sequence, throttle_bandwidth=-1):
        """big file would be splitted into packtes <= 64KB"""
        # pipeline writing (divide into packets) to all datanodes
        sent_file_size = 0
        pipeline_events = []
        i = 1
        while sent_file_size < size:
            sending_size = min(self.client_write_packet_size, size - sent_file_size)
            p = self.replicate_file("%s.%i" % (file_name, i), sending_size, node_sequence, throttle_bandwidth)
            pipeline_events.append(p)
            sent_file_size += sending_size
            i += 1

        # wait for all ACKs
        yield AllOf(self.env, pipeline_events)
        if self.client.id in node_sequence:
            node_sequence.remove(self.client.id)
        self.namenode.register_file(file_name, node_sequence)
        self.critical("ALL ACKs collected, put_file %s finished" % (file_name))

    def put_files(self, num, size, throttle_bandwidth=-1):
        """This API is used by client"""
        events = []
        for i in range(num):
            file_name = "hello.%i.txt" % i
            datanode_names = self.namenode.find_datanodes_for_new_file(file_name, size, self.replica_number)
            datanode_names.insert(0, self.client.id)
            e = self.create_file(file_name, size, datanode_names, throttle_bandwidth)
            events.append(e)
        run_all = AllOf(self.env, events)
        self.run_until(run_all)
        self.critical("%i files stored in NameNode" % len(self.namenode.metadata))
        return self.env.now

    def regenerate_blocks(self, num):
        """TODO: it is justly randomly regenerate blocks, not according to block placement and its replica number"""
        regenerate_events = []
        i = 1
        for i in range(num):
            from_node_id, to_node_id = random.sample(self.namenode.datanodes.keys(), 2)
            self.info("regenerating block %s->%s" % (from_node_id, to_node_id))
            r = self.create_file("block.%s.dat" % i, self.block_size, [from_node_id, to_node_id], self.balance_bandwidth)
            regenerate_events.append(r)
        run_all = AllOf(self.env, regenerate_events)
        self.run_until(run_all)
        return self.env.now

    def limplock_create_30_files(self):
        """create 30 64-MB files"""
        self.put_files(30, self.block_size)
        return self.env.now

    def limplock_regenerate_90_blocks(self):
        """regenerate 90 blocks"""
        self.info("regenerating 90 blocks: throttle_bandwidth: %s" % self.balance_bandwidth)
        self.regenerate_blocks(90)
        return self.env.now


def create_hdfs(env=None, number_of_datanodes=3, replica_number=3,
                enable_block_report=True, enable_heartbeats=True, enable_datanode_cache=True,
                default_bandwidth=100*1024*1024/8, default_disk_speed=80*1024*1024, heartbeat_interval=3,
                heartbeat_size=16*1024, block_report_interval=30, client_write_packet_size=1024*1024, **kwargs):
    if not env:
        env = simpy.Environment()
    hdfs = HDFS(env, namenode=None, replica_number=replica_number,
                          enable_block_report=enable_block_report, enable_heartbeats=enable_heartbeats,
                          heartbeat_interval=heartbeat_interval, heartbeat_size=heartbeat_size,
                          block_report_interval=block_report_interval, client_write_packet_size=client_write_packet_size, **kwargs)
    namenode = node.NameNode(env, "namenode", hdfs, **kwargs)
    hdfs.set_namenode(namenode)

    for i in range(number_of_datanodes):
        hdfs.create_datanode("datanode%i" % i, disk_speed=default_disk_speed, default_bandwidth=default_bandwidth)

    return hdfs


def create_silent_hdfs(**kwargs):
    return create_hdfs(do_debug=False, do_info=False, do_warning=False, do_critical=False, **kwargs)


def main():
    """Main function only in command line"""
    from sys import argv
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--disk-speed', type=int, default=80*1024*1024, help='disk speed')
    parser.add_argument('--nodes', type=int, default=20, help='number of datanodes')
    parser.add_argument('--files', type=int, default=30, help='number of generate files')
    args = parser.parse_args()
    print(args)

    hdfs = create_hdfs(number_of_datanodes=args.nodes, default_disk_speed=args.disk_speed,
                       do_debug=True,
                       )
    if True:
        hdfs.put_files(args.files, 64*1024*1024)
    else:
        hdfs.regenerate_blocks(args.files)


if __name__ == '__main__':
    main()
