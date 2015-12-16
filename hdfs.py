#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Brief Summary
Attributes:

Google Python Style Guide:
    http://google-styleguide.googlecode.com/svn/trunk/pyguide.html
"""
__copyright__ = "Zhaoyu Luo"

import simpy
from simpy.events import AllOf

import node


class HDFS(node.BaseSim):
    """By default, HDFS owns one switch and one client machine, it would instantiate that automatically
    The client machine would only be used to submit its task
    """

    def __init__(self, env, namenode, replica_number=3, heartbeat_interval=3, heartbeat_size=16*1024,
                 enable_datanode_cache=True, enable_heartbeats=True, enable_block_report=True,
                 block_report_interval=30):
        super(node.BaseSim, self).__init__()

        self.env = env
        self.id = "HDFS"

        self.pipeline_packet_size = 1024 * 1024
        self.replica_number = replica_number
        self.enable_datanode_cache = enable_datanode_cache
        self.enable_heartbeats = enable_heartbeats
        self.enable_block_report = enable_block_report
        self.heartbeat_size = heartbeat_size
        self.heartbeat_interval = heartbeat_interval
        self.block_report_interval = block_report_interval

        self.switch = node.Switch(env)
        self.client = node.Node(env, "client")
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

    def add_datanode(self, node):
        self.datanodes[node.id] = node
        self.switch.add_node(node)

    def transfer_data(self, from_node_id, to_node_id, size):
        return self.env.process(self._transfer_data(from_node_id, to_node_id, size))

    def _transfer_data(self, from_node_id, to_node_id, size):
        yield self.switch.process_ping(from_node_id, to_node_id, size)
        if self.enable_datanode_cache:
            yield self.datanodes[to_node_id].new_disk_buffer_write_request(size)
        else:
            yield self.datanodes[to_node_id].new_disk_write_request(size)

    def replicate_file(self, file_name, size, node_sequence):
        return self.env.process(self._replicate_file(file_name, size, node_sequence))

    def _replicate_file(self, file_name, size, node_sequence):
        i = 0
        while i < len(node_sequence) - 1:
            yield self.transfer_data(node_sequence[i], node_sequence[i+1], size)
            i += 1
        self.info("%s is replicated in %s" % (file_name, node_sequence))

    def process_put_file(self, file_name, size):
        return self.env.process(self._put_file(file_name, size))

    def _put_file(self, file_name, size):
        # ask namenode for 3 datanode
        yield self.env.timeout(self.switch.latency)
        datanode_names = self.namenode.find_datanodes_for_new_file(file_name, size, self.replica_number)
        datanode_names.insert(0, self.client.id)
        self.info(datanode_names)

        # pipeline writing (divide into packets) to 3 datanodes
        sent_file_size = 0
        pipeline_events = []
        i = 1
        while sent_file_size < size:
            sending_size = min(self.pipeline_packet_size, size - sent_file_size)
            p = self.replicate_file("%s.%i" % (file_name, i), sending_size, datanode_names)
            pipeline_events.append(p)
            sent_file_size += sending_size
            i += 1

        # wait for all ACKs
        yield AllOf(self.env, pipeline_events)
        datanode_names.remove(self.client.id)
        self.namenode.register_file(file_name, datanode_names)
        self.critical("ALL ACKs collected, put_file %s finished" % (file_name))

    def limplock_create_30_files(self):
        """create 30 64-MB files"""
        events = []
        for i in range(30):
            e = self.process_put_file("hello.txt.%i" % i, 64*1024*1024)
            events.append(e)
        run_all = AllOf(self.env, events)
        self.run_until(run_all)
        self.critical(str(self.namenode))

    def limplock_regenerate_90_blocks(self):
        """regenerate 90 blocks"""
        pass


def create_hdfs(number_of_datanodes=3, replica_number=3, enable_block_report=True, enable_heartbeats=True,
                default_bandwidth=100*1024*1024/8, default_disk_speed=80*1024*1024, heartbeat_interval=3,
                heartbeat_size=16*1024, block_report_interval=30):
    env = simpy.Environment()
    hdfs = HDFS(env, namenode=None, replica_number=replica_number,
                          enable_block_report=enable_block_report, enable_heartbeats=enable_heartbeats,
                          heartbeat_interval=heartbeat_interval, heartbeat_size=heartbeat_size,
                          block_report_interval=block_report_interval)
    namenode = node.NameNode(env, "namenode", hdfs)
    hdfs.set_namenode(namenode)

    for i in range(number_of_datanodes):
        datanode = node.DataNode(env, "datanode%i" % i, hdfs=hdfs, disk_speed=default_disk_speed)
        hdfs.add_datanode(datanode)

    return hdfs


def main():
    """Main function only in command line"""
    from sys import argv
    hdfs = create_hdfs(number_of_datanodes=3)
    hdfs.run_until(100)
    #hdfs.limplock_create_30_files()


if __name__ == '__main__':
    main()
