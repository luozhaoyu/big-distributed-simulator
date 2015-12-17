#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Brief Summary
Attributes:

Google Python Style Guide:
    http://google-styleguide.googlecode.com/svn/trunk/pyguide.html
"""
__copyright__ = "Zhaoyu Luo"

import argparse

import simpy
from simpy.events import AllOf

import node


class HDFS(node.BaseSim):
    """By default, HDFS owns one switch and one client machine, it would instantiate that automatically
    The client machine would only be used to submit its task
    """

    def __init__(self, env, namenode, replica_number=3, heartbeat_interval=3, heartbeat_size=1024,
                 enable_datanode_cache=True, enable_heartbeats=True, enable_block_report=True,
                 block_report_interval=30, balance_bandwidth=1024*1024, **kwargs):
        super(HDFS, self).__init__(**kwargs)

        self.env = env
        self.id = "HDFS"

        self.pipeline_packet_size = 2560 * 1024
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
        i = 0
        while i < len(node_sequence) - 1:
            yield self.transfer_data(node_sequence[i], node_sequence[i+1], size, throttle_bandwidth)
            i += 1
        self.info("REPLICATED\t%s\tin %s" % (file_name, node_sequence))

    def put_file(self, file_name, size, submitter=None, throttle_bandwidth=-1):
        return self.env.process(self._put_file(file_name, size, submitter, throttle_bandwidth))

    def _put_file(self, file_name, size, submitter=None, throttle_bandwidth=-1):
        """big file would be splitted into packtes <= 64KB"""
        # ask namenode for available datanodes
        yield self.env.timeout(self.switch.latency)

        if submitter:
            datanode_names = self.namenode.find_datanodes_for_new_file(file_name, size, self.replica_number)
            datanode_names.insert(0, submitter)
        else:
            datanode_names = self.namenode.find_datanodes_for_new_file(file_name, size, self.replica_number + 1)

        # pipeline writing (divide into packets) to all datanodes
        sent_file_size = 0
        pipeline_events = []
        i = 1
        while sent_file_size < size:
            sending_size = min(self.pipeline_packet_size, size - sent_file_size)
            p = self.replicate_file("%s.%i" % (file_name, i), sending_size, datanode_names, throttle_bandwidth)
            pipeline_events.append(p)
            sent_file_size += sending_size
            i += 1

        # wait for all ACKs
        yield AllOf(self.env, pipeline_events)
        if self.client.id in datanode_names:
            datanode_names.remove(self.client.id)
        self.namenode.register_file(file_name, datanode_names)
        self.critical("ALL ACKs collected, put_file %s finished" % (file_name))

    def create_files(self, num, size, submitter=None, throttle_bandwidth=-1):
        events = []
        for i in range(num):
            e = self.put_file("hello.%i.txt" % i, size, submitter, throttle_bandwidth)
            events.append(e)
        run_all = AllOf(self.env, events)
        self.run_until(run_all)
        self.critical("%i files stored in NameNode" % len(self.namenode.metadata))

    def limplock_create_30_files(self):
        """create 30 64-MB files"""
        self.create_files(30, 64*1024*1024, submitter=self.client.id)

    def limplock_regenerate_90_blocks(self):
        """regenerate 90 blocks"""
        self.info("regenerating 90 blocks: throttle_bandwidth: %s" % self.balance_bandwidth)
        self.create_files(90, 64*1024*1024, throttle_bandwidth=self.balance_bandwidth)


def create_hdfs(env=None, number_of_datanodes=3, replica_number=3, enable_block_report=True, enable_heartbeats=True,
                default_bandwidth=100*1024*1024/8, default_disk_speed=80*1024*1024, heartbeat_interval=3,
                heartbeat_size=16*1024, block_report_interval=30, **kwargs):
    if not env:
        env = simpy.Environment()
    hdfs = HDFS(env, namenode=None, replica_number=replica_number,
                          enable_block_report=enable_block_report, enable_heartbeats=enable_heartbeats,
                          heartbeat_interval=heartbeat_interval, heartbeat_size=heartbeat_size,
                          block_report_interval=block_report_interval, **kwargs)
    namenode = node.NameNode(env, "namenode", hdfs, **kwargs)
    hdfs.set_namenode(namenode)

    for i in range(number_of_datanodes):
        hdfs.create_datanode("datanode%i" % i, disk_speed=default_disk_speed, default_bandwidth=default_bandwidth)

    return hdfs


def main():
    """Main function only in command line"""
    from sys import argv
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--disk-speed', type=int, default=80*1024*1024, help='disk speed')
    args = parser.parse_args()
    print(args)

    hdfs = create_hdfs(number_of_datanodes=3, default_disk_speed=args.disk_speed, do_debug=True)
    #hdfs.limplock_regenerate_90_blocks()
    hdfs.limplock_create_30_files()


if __name__ == '__main__':
    main()
